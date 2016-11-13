/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";
const assert = require('assert');

const debug = require('../debug')('ha-cache:client-handler');

const now = Date.now;
const min = Math.min;

const {
        CLIENT_REQ_REFRESH_INTERVAL
      , CANDIDATE_RETRY_QUESTION_TTL
      // client commands
      , CMD_GET_MATCH
      , CMD_DISCOVER_MATCH
      // client response types
      , RES_VALUE
      , RES_SEEOTHER
      , RES_SERVERS
      // custom targets
      , EVERYONE
      // states
      , STATE_CANDIDATE
      // utils
      , randomExpire
      , uint2bin
      , bin2uint
      } = require('../common');

const startSourcing = require('./sourcing');

module.exports = exports = function clientHandler(ident, reqid, command, expire, key, after) {
  if (arguments.length < 4 || command.length !== 1) {
    /* ignore trash */
    debug('got some trash from client');
    return;
  }

  expire = bin2uint(expire);

  var moment = now();
  /* ignore expired request */
  if (moment > expire) {
    debug('<<< CLIENT DROP expired');
    return;
  }

  switch(command[0]) {

    case CMD_DISCOVER_MATCH:
      // debug('>>> CLIENT DISCOVER');
      this.server.send([ident, reqid, RES_SERVERS, this.apiUrlsPacked]);
      return;

    case CMD_GET_MATCH:
      break;

    default:
      debug('<<< CLIENT DROP unknown command: %s', command);
      return;
  }

  if (key === undefined) {
    debug('<<< CLIENT DROP no key');
    return;
  }
  else if (key.length === 0) {
    debug('<<< CLIENT DROP empty key');
    return;
  }

  if (this.isPartitioned) {
    debug('<<< CLIENT DROP partitioned state');
    return;
  }

  var keystr = key.toString('binary');
  after = (after === undefined) ? 0 : min(bin2uint(after), moment);
  debug('<<< CLIENT %s after: %d', keystr, after);

  this._storageGetOrCreateMeta(key).then(entry => {
    if (entry.expire > after) {
      /* 2.2 we have entry, so respond to the client and forget about him */
      this.dataStorage.get(key).then(value => {
        debug('>>> CLIENT %s VALUE: %s EXP: %s NOW: %s', keystr, value.length, entry.expire, moment);
        this.server.send([ident, reqid, RES_VALUE, value, uint2bin(entry.expire)]);
      })
      .catch(err => {
        /* this is fatal, so better die */
        this.emit('error', err);
      });
    }
    else {
      /* signal client that we are alive, it will reset client timeout to longer period */
      debug('>>> CLIENT %s REFRESH', keystr);
      this.server.send([ident, reqid]);
      /* save client request for later */
      queueClientRequest.call(this, keystr, after, [ident, reqid]);
    }

    if (entry.expire < moment && !this._states.has(keystr)) {
      /* 2.3 we have expired entry or none so ask the QUESTION */
      if (this.majority === 1) {
      /* single peer */
        startSourcing.call(this, keystr, entry)
      }
      else
        askPeers.call(this, keystr, entry);
    } /* 2.4 do nothing */

  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
};

function queueClientRequest(keystr, after, request) {
  this._clientRequests.add(keystr, [after, request]);

  if (this._keepAliveInterval == null) {
    /* refresh client timeouts */
    this._keepAliveInterval = setInterval(() => {
      debug('>>> CLIENT * REFRESH');
      var server = this.server;
      for(var [keystr, queue] of this._clientRequests) {
        if (!this._states.has(keystr)) {
          /* drop clients if idle state */
          dropClientRequestQueue.call(this, keystr, queue)
        }
        /* send keep alive to clients */
        else for(var [after, request] of queue) server.send(request);
      }
      this._checkClientRefreshInterval();
    }, CLIENT_REQ_REFRESH_INTERVAL);
  }
}

function dropClientRequestQueue(keystr, queue) {
  debug('>>> CLIENT %s DROP', keystr);
  this._clientRequests.delete(keystr);
  var server = this.server;
  for(var [after, request] of queue) {
    request.push(RES_SEEOTHER);
    server.send(request);
  }
}

function askPeers(keystr, entry) {
  /* start election */
  this._sendQuestion(EVERYONE, entry);

  const votes = new Set();

  const state = {
    type: STATE_CANDIDATE,
    entry: entry,
    keystr: keystr,
    votes: votes,
    agreed: 1,
    aborted: false, // do not process event if true
    reqPeer: null, // peer to fetch expired event to update ours
    reqExpire: entry.expire, // expiration of old entry to fetch, default to our entry
    expire: randomExpire(),
  };

  const timeoutcb = state.timeoutcb = () => {
    /* 1.1 handle candidate timeout */
    var ttl = state.expire - now();
    if (ttl > 0) {
      if (votes.size < this.peerUrls.size) {
        debug('replying questions: %s on %s', votes.size, keystr);
        /* repeat RPC request */
        if (votes.size === 0) {
          this._sendQuestion(EVERYONE, entry);
        }
        else {
          for(let [peerUrl, peerIdent] of this.peerIdentities) {
            if (!votes.has(peerUrl)) this._sendQuestion(peerIdent, entry);
          }
        }
        ttl = min(CANDIDATE_RETRY_QUESTION_TTL, ttl);
      }

      state.timeout = setTimeout(timeoutcb, ttl);
    }
    else {
      if (votes.size + 1 < this.majority) {
        /* didn't get enough answers, are we partitioned?
           let's block new client requests, drop to idle and start pinging */
        this._enterPartitionedState();
        /* sanity check, this would never happen, we always clear timeouts before changing state */
        assert.strictEqual(state, this._states.get(keystr));
        this._states.delete(keystr);
      }
      else {
        debug('timeout candidate, candidating again: %s', keystr);
        /* a split vote, go again */
        ++entry.term;
        this.metaStorage.set(entry.key, entry).then(() => {
          if (state === this._states.get(keystr)) {
            askPeers.call(this, keystr, entry);
          }
        })
        .catch(err => {
          /* this is fatal, so better die */
          this.emit('error', err);
        });
      }
    }
  };

  state.timeout = setTimeout(timeoutcb, CANDIDATE_RETRY_QUESTION_TTL);
  this._states.set(keystr, state);

  debug('candidate expire in: %s for %s', state.expire - now(), keystr);
}
