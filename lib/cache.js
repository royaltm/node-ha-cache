/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const assert = require('assert');
const EventEmitter = require('events');
const zmq = require('zmq');
const debug = require('debug')('ha-cache');

const pid = process.pid;

const isBuffer = Buffer.isBuffer;
const now    = Date.now;
const min    = Math.min;
const random = Math.random;
const round  = Math.round;

const CANDIDATE_STATE_MAX_TTL = 300;
const CANDIDATE_STATE_MIN_TTL = 150;
const CANDIDATE_STATE_DELTA_TTL = CANDIDATE_STATE_MAX_TTL - CANDIDATE_STATE_MIN_TTL;
const randomExpire = () => round(random() * CANDIDATE_STATE_DELTA_TTL + CANDIDATE_STATE_MIN_TTL + now());

const CACHE_TTL = 3600*1000;
const CACHE_MIN_TTL = 60000;

const CLIENT_REQ_REFRESH_INTERVAL  = 1000;
const FOLLOWER_STATE_CANDIDATE_TTL =  300;
const CANDIDATE_RETRY_QUESTION_TTL =  100;
const FOLLOWER_STATE_SOURCE_TTL    = 2000;
const SOURCE_ANNOUNCEMENT_INTEVAL  = 1000;
const PING_INTERVAL                = 3000;

assert(CANDIDATE_RETRY_QUESTION_TTL < CANDIDATE_STATE_MAX_TTL);

/* custom targets */
const EVERYONE = Buffer.from('*');

// client commands
const CMD_GET_MATCH      = '?'.charCodeAt(0);
const CMD_DISCOVER_MATCH = '*'.charCodeAt(0);

// client response types
const RES_VALUE    = Buffer.from('=');
const RES_SEEOTHER = Buffer.from('#');
const RES_SERVERS  = Buffer.from('*');

// peer message types
const QUESTION       = Buffer.from('Q');
const QUESTION_MATCH = QUESTION[0];
const ANSWER         = Buffer.from('A');
const ANSWER_MATCH   = ANSWER[0];
const ENTRYREQ       = Buffer.from('E');
const ENTRYREQ_MATCH = ENTRYREQ[0];
const ANNOUNCE       = Buffer.from('!');
const ANNOUNCE_MATCH = ANNOUNCE[0];
const UPDATE         = Buffer.from('=');
const UPDATE_MATCH   = UPDATE[0];
const PING           = Buffer.from('.');
const PING_MATCH     = PING[0];
const PONG           = Buffer.from('-');
const PONG_MATCH     = PONG[0];

const STATE_FOLLOWER  = 1;
const STATE_CANDIDATE = 2;
const STATE_SOURCING  = 3;

const { allocVarUIntLE: uint2bin, readVarUIntLE: bin2uint } = require('./varint');

const msgpack = require('msgpack-lite');

/*

  storage api:
  storage.get(key) -> promise(entry)
  storage.set(key, entry) -> promise(entry) ; entry must be original entry from arguments

  source(key) -> promise

entry:
{
  key
  value
  expire
  term
}
*/


class RequestCache extends Map {
  constructor() {
    super();
  }

  add(key, entry) {
    var queue = this.get(key);
    if (queue === undefined) {
      this.set(key, queue = []);
    }
    queue.push(entry);
  }

  fetch(key) {
    var queue = this.get(key);
    if (queue !== undefined) {
      this.delete(key);
      return queue;
    }
  }
}

class StatesMap extends Map {
  constructor(cache) {
    super();
  }
}

class Cache extends EventEmitter {
  /**
   * Create Cache server instance
   *
   * REQUIRED `options`:
   *
   * - `url` {string}: an zmq address of this server serving as its id and
   *         this is the address that other peers connect to and
   *         the default bind address
   * - `api` {string}: an api zmq address of this server on which it servers its clients
   *         this is the address that clients connect to and
   *         the default bind address
   * - `peers` {Array}: an array describing sibling peers which are obejcts with two
   *         properties: `url` and `api` describing each server in the cluster
   *         it's safe to include this server's descriptor but not necessary
   * - `source` {Function}: a function({String}) -> {Promise} which is being called
   *         when sourcing cache entry. Should resolve to entry value as {Buffer}.
   * - `storage` {object}: a pair of local storages for cache entries following storage api.
   *         it should consist of `meta` and `data` storages
   *
   * Additional `options` may be one of:
   *
   * - `bindUrl` {string}: a zmq address to bind socket that other servers connect to
   * - `bindApi` {string}: a zmq address to bind socket that clients connect to
   * - `ttl` {number}: entry cache time-to-live in milliseconds, default is 1 hour (3600000).
   *
   * @param {Object} options
   * @return {Cache}
  **/
  constructor(options) {
    super();
    if (!options || 'object' !== typeof options)
      throw new TypeError("Cache: options must be an object");
    if ('string' !== typeof options.url || options.url.length === 0)
      throw new TypeError("Cache: options.url must be specified as a string");
    /* address for peers to connect to with their SUBs */
    this.myUrl = options.url;
    /* our identity message frame */
    this.myIdentity = Buffer.from(this.myUrl);
    if (options.bindUrl !== undefined && ('string' !== typeof options.bindUrl || options.bindUrl.length === 0))
      throw new TypeError("Cache: options.bindUrl must be specified as a string");
    /* address to bind our PUB to */
    this.bindUrl = options.bindUrl || this.myUrl;
    if ('string' !== typeof options.api || options.api.length === 0)
      throw new TypeError("Cache: options.api must be specified as a string");
    if (options.bindApi !== undefined && ('string' !== typeof options.bindApi || options.bindApi.length === 0))
      throw new TypeError("Cache: options.bindApi must be specified as a string");
    /* address to bind our ROUTER to */
    this.apiUrl = options.api;
    this.bindApiUrl = options.bindApi || this.apiUrl;
    if (!Array.isArray(options.peers) || !options.peers.every(peer => (peer && 'object' === typeof peer
                                                        && 'string' === typeof peer.url && peer.url.length !== 0
                                                        && 'string' === typeof peer.api && peer.api.length !== 0))) {
      throw new TypeError("Cache: options.peers must be an array of peer descriptors");
    }
    /* a set of cluster server urls excluding our */
    this.peerUrls = new Set(options.peers.map(peer => peer.url));
    this.peerUrls.delete(this.myUrl);
    /* peer identity message frames */
    this.peerIdentities = Array.from(this.peerUrls).map(url => [url, Buffer.from(url)]);
    /* list of all api urls in the cluster */
    var apiUrls = new Set(options.peers.map(peer => peer.api));
    apiUrls.add(this.apiUrl);
    assert.strictEqual(apiUrls.size, this.peerUrls.size + 1, "number of unique api urls does not match number of peer urls");
    /* list of api urls message frame for clients */
    this.apiUrlsPacked = msgpack.encode(Array.from(apiUrls));
    /* a majority required for decisions */
    this.majority = (apiUrls.size >>> 1) + 1;
    if (!options.storage || 'object' !== typeof options.storage
        || !options.storage.meta || 'object' !== typeof options.storage.meta
        || !options.storage.data || 'object' !== typeof options.storage.data) {
      throw new TypeError("Cache: options.storage must be an object containing meta and data storages");
    }
    if ('function' !== typeof options.storage.meta.get
        || 'function' !== typeof options.storage.meta.set
        || 'function' !== typeof options.storage.meta.delete) {
      throw new TypeError("Cache: options.storage.meta must be a storage");
    }
    if ('function' !== typeof options.storage.data.get
        || 'function' !== typeof options.storage.data.set
        || 'function' !== typeof options.storage.data.delete) {
      throw new TypeError("Cache: options.storage.data must be a storage");
    }
    this.dataStorage = options.storage.data; // only values
    this.metaStorage = options.storage.meta; // {key, term, expire, size}
    if ('function' !== typeof options.source)
      throw new TypeError("Cache: options.source must be a function");
    this.source = options.source;

    if (options.ttl !== undefined
        && ('number' !== typeof options.ttl
            || !isFinite(options.ttl)
            || options.ttl < CACHE_MIN_TTL)) {
      throw new TypeError("Cache: options.ttl must be a number >= " + CACHE_MIN_TTL);
    }
    this.ttl = options.ttl || CACHE_TTL; // cache entry expiration time in milliseconds

    /* queues for client requests */
    this._clientRequests = new RequestCache();
    /* key -> state map */
    this._states = new Cache.StatesMap(this);
    /* partitioned state */
    this.isPartitioned = false;
    this.pongResponses = new Set();
    this.pingInterval = null;

    var incoming = this.incoming = zmq.socket('sub');
    incoming.setsockopt(zmq.ZMQ_LINGER, 0); // do not linger at all on close()
    incoming.on('message', peerHandler.bind(this));
    incoming.subscribe(EVERYONE); // subscribe to ventilator messages
    incoming.subscribe(this.myIdentity); // subscribe to targeted messages

    var outgoing = this.outgoing = zmq.socket('pub');
    outgoing.setsockopt(zmq.ZMQ_LINGER, 0); // do not linger at all on close()
    outgoing.bind(this.bindUrl, err => {
      if (err) return this.emit('error', err);
      debug('%s listening on: %s cluster peers: %s majority: %s', pid, this.bindUrl, apiUrls.size, this.majority);
      /* connect to all peers */
      for(let url of this.peerUrls) incoming.connect(url);
    });

    var server = this.server = zmq.socket('router');
    server.setsockopt(zmq.ZMQ_LINGER, 1000); // do not linger forever on close()
    server.on('message', clientHandler.bind(this));
    server.bind(this.bindApiUrl, err => {
      if (err) return this.emit('error', err);
    });

  }

  /**
   * Disconnect and close Cache server connection.
   *
   * Returns false when server is already closed.
   * When returns true it will emit 'closed' event after all sockets are closed.
   *
   * @return {boolean}
  **/
  close() {
    if (!this.incoming) return false;
    var incoming = this.incoming;
    for(let url of this.peerUrls) incoming.disconnect(url);
    incoming.close();
    this.incoming = null;
    var pending = 2;
    this.outgoing.unbind(this.bindUrl, err => {
      if (err) return this.emit('error', err);
      this.outgoing.close();
      this.outgoing = null;
      if (!--pending) this.emit('closed');
    });
    this.server.unbind(this.bindApiUrl, err => {
      if (err) return this.emit('error', err);
      this.server.close();
      this.server = null;
      if (!--pending) this.emit('closed');
    });
    return true;
  }

  /* PRIVATE: for internal use and debugging purposes */

  _clearPartitionedState() {
    clearInterval(this.pingInterval);
    this.isPartitioned = false;
  }

  _enterPartitionedState() {
    debug("%s entering PARTITIONED state", pid);
    clearInterval(this.pingInterval);
    this.isPartitioned = true;
    var pongResponses = this.pongResponses;
    this.pingInterval = setInterval(() => {
      pongResponses.clear();
      this.outgoing.send([EVERYONE, this.myIdentity, PING]);
    }, PING_INTERVAL);
  }

  _sendQuestion(target, entry) {
    debug('%s >>> %s QUESTION %s /%s', pid, target, entry.key, entry.term);
    this.outgoing.send([target, this.myIdentity, QUESTION, entry.key, uint2bin(entry.term)]);
  }

  _sendEntryRequest(target, key) {
    debug('%s >>> %s ENTRYREQ %s', pid, target, key);
    this.outgoing.send([target, this.myIdentity, ENTRYREQ, key]);
  }

  _sendAnnouncement(target, entry) {
    debug('%s >>> %s ANNOUNCE %s /%s exp: %s', pid, target, entry.key, entry.term, entry.expire);
    this.outgoing.send([target, this.myIdentity, ANNOUNCE, entry.key, uint2bin(entry.term), uint2bin(entry.expire)]);
  }

  _sendUpdate(target, entry, value) {
    debug('%s >>> %s UPDATE %s /%s exp: %s', pid, target, entry.key, entry.term, entry.expire);
    this.outgoing.send([target, this.myIdentity, UPDATE, entry.key, uint2bin(entry.term), uint2bin(entry.expire), value]);
  }

  _setFollowerState(entry, keystr, peer, isSource) {
    var state = {type: STATE_FOLLOWER, entry: entry, keystr: keystr, peer: peer, isSource: isSource,
      timeoutcb: () => {
        /* 1.2 follower timeout */
        /* sanity check, this would never happen, we always clear timeouts before changing state */
        assert.strictEqual(state, this._states.get(keystr));
        this._states.delete(keystr);
      }
    };
    state.timeout = setTimeout(state.timeoutcb, isSource
                                                ? FOLLOWER_STATE_SOURCE_TTL
                                                : FOLLOWER_STATE_CANDIDATE_TTL);
    this._states.set(keystr, state);
    return state;
  }
}

function peerHandler(target, peer, type, key, term, expire, extra) {
  if (target === undefined || peer === undefined || type === undefined || type.length !== 1) {
    /* ignore trash */
    debug('got some trash');
    return;
  }
  var peerKey = peer.toString();
  if (!this.peerUrls.has(peerKey)) {
    /* check if this peer is in our setup */
    debug('%s unknown peer: %s [%s]: %s %s', pid, peerKey, type, target, key);
    return;
  }
  try {
    switch(type[0]) {
      case QUESTION_MATCH:
        term = bin2uint(term);
        debug('%s got QUESTION: %s key: %s /%s', pid, peer, key, term);
        return processQuestion.call(this, peer, key, term);
      case ANSWER_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        extra = bin2bool(extra);
        debug('%s got ANSWER: %s key: %s /%s exp: %s, vote: %s', pid, peer, key, term, expire, extra);
        return processAnswer.call(this, peer, peerKey, key, term, expire, extra);
      case ENTRYREQ_MATCH:
        debug('%s got ENTRYREQ: %s key: %s', pid, peer, key);
        return processEntryRequest.call(this, peer, key);
      case UPDATE_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        debug('%s got UPDATE: %s key: %s /%s exp: %s data: %d', pid, peer, key, term, expire, extra.length);
        return processUpdate.call(this, peer, key, term, expire, extra);
      case ANNOUNCE_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        debug('%s got ANNOUNCE: %s key: %s /%s exp: %s', pid, peer, key, term, expire);
        return processAnnouncement.call(this, peer, key, term, expire);
      case PING_MATCH:
        debug('%s got PING: %s', pid, peer);
        return processPing.call(this, peer);
      case PONG_MATCH:
        debug('%s got PONG: %s', pid, peer);
        return processPong.call(this, peer);
      default:
        debug('%s got [%s] from %s', pid, type, peer);
    }
  } catch (err) {
    this.emit('error', err);
  }
}

function askPeers(keystr, entry) {
  /* start election */
  this._sendQuestion(EVERYONE, entry);
  var votes = new Set();
  var state = {
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
  var timeoutcb = state.timeoutcb = () => {
    /* 1.1 handle candidate timeout */
    var ttl = state.expire - now();
    if (ttl > 0) {
      if (votes.size < this.peerUrls.size) {
        debug('%s: replying questions: %s', pid, votes.size);
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
        /* didn't get enough ansers, are we partitioned?
           let's block new client requests, drop to idle and start pinging */
        this._enterPartitionedState();
        /* sanity check, this would never happen, we always clear timeouts before changing state */
        assert.strictEqual(state, this._states.get(keystr));
        this._states.delete(keystr);
      }
      else {
        debug('%s: timeout candidate, candidating again', pid);
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
  debug('%s: candidate expire in: %s', pid, state.expire - now())
}

function processAnswer(peer, peerKey, key, term, expire, vote) {
  const keystr = key.toString('binary');
  var entry;
  var state = this._states.get(keystr);

  var updateTermOrDie = () => {
    entry.term = term;
    this.metaStorage.set(key, entry)
    .catch(err => {
      /* this is fatal, so better die */
      this.emit('error', err);
    });
  };

  /* 7.1 candidate */
  if (state !== undefined && state.type === STATE_CANDIDATE) {
    /* we already have peer's answer */
    if (state.votes.has(peerKey)) return;
    /* mark peer's voice at least for net partition check */
    state.votes.add(peerKey);
    entry = state.entry
    /* voting aborted, let it just timeout or hope for announcement */
    if (state.aborted) {
      debug('aborted!');
      if (term > entry.term) updateTermOrDie();
      return;
    }
    debug('new vote: %s exp: %s ... %s terms: %s/%s', vote, expire, state.type, entry.term, term);
    /* find best source of expired entry to fetch */
    if (expire > state.reqExpire) {
      state.reqPeer = peer;
      state.reqExpire = expire;
    }
    if (expire > now()) {
      /* wow, someone has a fresh one, fetch immediately, abort voting and refresh timeout */
      clearTimeout(state.timeout);
      state.expire = randomExpire();
      state.timeout = setTimeout(state.timeoutcb, 100);
      state.aborted = true;
      this._sendEntryRequest(peer, key);
      /* update term if necessary */
      if (term > entry.term) updateTermOrDie();
    }
    else if (term === entry.term) {
      debug('%s: ON OUR TERM: %s  %s ? %s', pid, vote, state.agreed + vote, this.majority);
      if (vote && ++state.agreed >= this.majority) {
        /* start sourcing */
        debug('%s: will start sourcing', pid)
        clearTimeout(state.timeout);
        /* fetch expired entry */
        if (state.reqExpire > entry.expire) {
          this._sendEntryRequest(state.reqPeer, key);
        }
        startSourcing.call(this, keystr, entry);
      }
    }
    /* update term if necessary */
    else if (term > entry.term) {
      debug('%s: TOO BAD TERM', pid);
      /* abort voting and let it timeout */
      state.aborted = true;
      updateTermOrDie();
    }
  } /* 7.2 finish processing */
}

function startSourcing(keystr, entry) {
  const state = {type: STATE_SOURCING, entry: entry, keystr: keystr}
      , key = entry.key;
  this._states.set(keystr, state);
  state.interval = setInterval(() => this._sendAnnouncement(EVERYONE, entry), SOURCE_ANNOUNCEMENT_INTEVAL);
  this._sendAnnouncement(EVERYONE, entry);
  Promise.resolve(this.source(key))
  .then(value => {
    assert(isBuffer(value), "value resolved from source is not a buffer");
    entry.expire = now() + this.ttl;
    entry.size = value.length;
    return this.dataStorage.set(key, value)
    .then(() => this.metaStorage.set(key, entry))
    .then(entry => {
      sendEntryToClients.call(this, keystr, entry, value);
      /* send update */
      this._sendUpdate(EVERYONE, entry, value);
      /* drop to idle */
      clearInterval(state.interval);
      this._states.delete(keystr);
    });
  })
  .catch(err => {
    /* drop to idle */
    clearInterval(state.interval);
    this._states.delete(keystr);
    // TODO: what about clients?
  });
}

function processEntryRequest(peer, key) {
  Promise.all([this.metaStorage.get(key),
               this.dataStorage.get(key)])
  .then(([entry, value]) => {
    if (entry != null && isBuffer(value)) {
      this._sendUpdate(peer, entry, value);
    }
  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
}

function processUpdate(peer, key, term, expire, value) {
  var keystr = key.toString('binary');
  var state = this._states.get(keystr);
  debug('updated expire: %s size: %s', expire, value.length);
  (state !== undefined
           ? Promise.resolve(state.entry)
           : storageGetOrCreateMeta.call(this, key))
  .then(entry => {
    var shouldUpdateMeta = false, shouldUpdateAll = false;
    if (term > entry.term) {
      shouldUpdateMeta = true;
      entry.term = term;
    }
    if (expire > entry.expire) {
      shouldUpdateAll = true;
      entry.expire = expire;
      entry.size = value.length;
      sendEntryToClients.call(this, keystr, entry, value);
    }
    debug('shouldUpdateMeta: %s shouldUpdateAll: %s', shouldUpdateMeta, shouldUpdateAll);
    if (shouldUpdateAll) {
      return this.dataStorage.set(key, value)
      .then(() => this.metaStorage.set(key, entry));
    }
    else if (shouldUpdateMeta) {
      return this.metaStorage.set(key, entry);
    }
    else return entry;
  })
  .then(entry => {
    /* check after entry was saved */
    var state = this._states.get(keystr);
    if (expire > now() && state !== undefined && state.type !== STATE_SOURCING) {
      /* drop to idle */
      debug('%s: dropping to idle on update!', pid);
      clearTimeout(state.timeout);
      this._states.delete(keystr);
    }
  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
}

function processAnnouncement(peer, key, term, expire) {
  const keystr = key.toString('binary')
      , state = this._states.get(keystr);
  (state !== undefined
           ? Promise.resolve(state.entry)
           : storageGetOrCreateMeta.call(this, key))
  .then(entry => {
    /* 5.1 */
    if (expire > entry.expire) {
      this._sendEntryRequest(peer, key);
    }
    /* 5.2 get state again */
    var state = this._states.get(keystr);
    if (state !== undefined) {
      /* sourcing */
      if (state.type === STATE_SOURCING) {
        return;
      }
      /* follower of the same peer */
      else if (state.type === STATE_FOLLOWER && state.isSource && state.peer.equals(peer)) {
        /* just refresh timeout */
        refreshStateTimout(state, FOLLOWER_STATE_SOURCE_TTL);
      }
      else if (term >= entry.term) {
        /* otherwise drop it */
        clearTimeout(state.timeout);
        state = undefined;
      }
    }
    /* follow sourcing peer */
    if (state === undefined) {
      this._setFollowerState(entry, keystr, peer, true);
      if (term > entry.term) {
        entry.term = term;
        return this.metaStorage.set(key, entry);
      }
    }
  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
}

function processQuestion(peer, key, term) {
  const keystr = key.toString('binary');
  var vote
    , state = this._states.get(keystr);
  const sendAnswer = (entry) => {
    debug('%s >>> %s ANSWER %s /%s exp: %s vote: %s', pid, peer, keystr, entry.term, entry.expire, vote);
    this.outgoing.send([peer, this.myIdentity, ANSWER, key,
                              uint2bin(entry.term), uint2bin(entry.expire), bool2bin(vote)]);
  };
  if (state === undefined) {
    /* 3.1. idle */
    storageGetOrCreateMeta.call(this, key).then(entry => {
      // be safe, it's async
      if (this._states.has(keystr)) {
        /* state has been set in the meantime, so try again */
        return processQuestion.call(this, peer, key, term);
      }
      else {
        if (entry.expire < now() && term >= entry.term) {
          this._setFollowerState(entry, keystr, peer, false);
          vote = true;
        }
        else
          vote = false;

        /* update term if necessary */
        if (term > entry.term) {
          entry.term = term;
          return this.metaStorage.set(key, entry).then(sendAnswer);
        }
        else
          sendAnswer(entry);
      }
    })
    .catch(err => {
      /* this is fatal, so better die */
      this.emit('error', err);
    });
    /* end of 3.1. idle */
  }
  else {
    var entry = state.entry;
    /* 3.4. sourcing */
    if (state.type === STATE_SOURCING) {
      this._sendAnnouncement(peer, entry);
      return;
    }
    /* 3.2. follower */
    else if (state.type === STATE_FOLLOWER) {
      entry = state.entry;
      vote = (state.isSource || term < entry.term)
               ? false
               : (term > entry.term || state.peer.equals(peer));
      if (vote) {
        /* refresh timeout */
        refreshStateTimout(state, FOLLOWER_STATE_CANDIDATE_TTL);
      }
    /* 3.3. candidate */
    }
    else if (state.type === STATE_CANDIDATE) {
      if (term > entry.term) {
        /* convert to follower */
        clearTimeout(state.timeout);
        this._setFollowerState(entry, keystr, peer, false);
        vote = true;
      }
      else
        vote = false;
    }
    else
      throw new Error(`Bad juju - unknown state type: ${state.type}`);

    /* update term if necessary */
    if (term > entry.term) {
      entry.term = term;
      this.metaStorage.set(key, entry).then(sendAnswer)
      .catch(err => {
        /* this is fatal, so better die */
        this.emit('error', err);
      });
    }
    else
      sendAnswer(entry);
  }
}

function processPing(peer) {
  debug('%s <<< PING %s', pid, peer);
  this.outgoing.send([peer, this.myIdentity, PONG]);
}

function processPong(peer) {
  if (this.isPartitioned) {
    debug('%s <<< PONG %s', pid, peer);
    var pongResponses = this.pongResponses;
    pongResponses.add(peer.toString());
    if (pongResponses.size + 1 >= this.majority) {
      debug('%s step down from PARTITIONED state', pid);
      this._clearPartitionedState();
    }
  }
}

function storageGetOrCreateMeta(key) {
  return this.metaStorage.get(key).then(entry => (entry || {key: key, term: 0, expire: 0, size: 0}));
}

function clientHandler(ident, reqid, command, expire, key, after) {
  if (arguments.length < 4 || command.length !== 1) {
    /* ignore trash */
    debug('%s got some trash from client', pid);
    return;
  }
  expire = bin2uint(expire);
  var moment = now();
  /* ignore expired request */
  if (moment > expire) {
    debug('%s <<< CLIENT %s DROP expired', pid, key);
    return;
  }
  switch(command[0]) {
    case CMD_DISCOVER_MATCH:
      debug('%s >>> CLIENT DISCOVER', pid);
      this.server.send([ident, reqid, RES_SERVERS, this.apiUrlsPacked]);
      return;
    case CMD_GET_MATCH:
      break;
    default:
      debug('%s <<< CLIENT %s DROP unknown command: %s', pid, key, command);
      return;
  }
  if (key === undefined) {
    debug('%s <<< CLIENT %s DROP no key', pid);
    return;
  }
  else if (key.length === 0) {
    debug('%s <<< CLIENT %s DROP empty key', pid);
    return;
  }
  if (this.isPartitioned) {
    debug('%s <<< CLIENT DROP partitioned state', pid);
    return;
  }
  var keystr = key.toString('binary');
  after = (after === undefined) ? 0 : max(bin2uint(after), moment);
  debug('%s <<< CLIENT %s after: %d', pid, keystr, after);
  storageGetOrCreateMeta.call(this, key).then(entry => {
    if (entry.expire > after) {
      /* 2.2 we have entry, so respond to the client and forget about him */
      return this.dataStorage.get(key).then(value => {
        debug('%s >>> CLIENT %s VALUE: %s', pid, keystr, value.length);
        this.server.send([ident, reqid, RES_VALUE, value, uint2bin(entry.expire)]);
      });
    }
    else {
      /* signal client that we are alive, it will reset client timeout to longer period */
      debug('%s >>> CLIENT %s REFRESH', pid, keystr);
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
}

function queueClientRequest(keystr, after, request) {
  this._clientRequests.add(keystr, [after, request]);
  if (this._keepAliveHandler === undefined) {
    /* refresh client timeouts */
    this._keepAliveHandler = setInterval(() => {
      debug('%s >>> CLIENT * REFRESH', pid);
      var server = this.server;
      for(var [keystr, queue] of this._clientRequests) {
        if (!this._states.has(keystr)) {
          /* drop clients if idle state */
          dropClientRequestQueue.call(this, keystr, queue)
        }
        /* send keep alive to clients */
        else for(var [after, request] of queue) server.send(request);
      }
      checkClientRefreshInterval.call(this);
    }, CLIENT_REQ_REFRESH_INTERVAL);
  }
}

function dropClientRequestQueue(keystr, queue) {
  debug('%s >>> CLIENT %s DROP', pid, keystr);
  this._clientRequests.delete(keystr);
  var server = this.server;
  for(var [after, request] of queue) {
    request.push(RES_SEEOTHER);
    server.send(request);
  }
}

function sendEntryToClients(keystr, entry, value) {
  const clientRequests = this._clientRequests
      , queue = clientRequests.fetch(keystr);
  if (queue !== undefined) {
    const server = this.server
        , expire = entry.expire
        , expbin = uint2bin(expire);
    for(var item of queue) {
      var [after, request] = item;
      if (expire > after) {
        request.push(RES_VALUE, value, expbin);
        debug('%s >>> CLIENT %s VALUE', pid, keystr);
        server.send(request);
      }
      else clientRequests.add(keystr, item);
    }
  }
  checkClientRefreshInterval.call(this);
}

function checkClientRefreshInterval() {
  if (this._clientRequests.size === 0) {
    clearInterval(this._keepAliveHandler);
    this._keepAliveHandler = undefined;
  }
}

function refreshStateTimout(state, ttl) {
  clearTimeout(state.timeout);
  state.timeout = setTimeout(state.timeoutcb, ttl);
}


const BINARY_FALSE = Buffer.alloc(0);
const BINARY_TRUE = Buffer.from([1]);
function bin2bool(buffer) {
  return buffer.length !== 0 && !!buffer[0];
}

function bool2bin(bool) {
  return bool ? BINARY_TRUE : BINARY_FALSE;
}

Cache.StatesMap = Map;
module.exports = Cache;
