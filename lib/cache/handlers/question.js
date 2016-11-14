/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:question-handler');

const now = Date.now;

const {
        FOLLOWER_STATE_CANDIDATE_TTL
      // peer message types
      , ANSWER
      // states
      , STATE_FOLLOWER
      , STATE_CANDIDATE
      , STATE_SOURCING
      // utils
      , refreshStateTimout
      , bool2bin
      , uint2bin
      } = require('../common');

const processQuestion = module.exports = exports = function processQuestion(peer, key, term) {
  const keystr = key.toString('binary')
      , state = this._states.get(keystr);

  var vote;

  const sendAnswer = (entry) => {
    debug('>>> ANSWER %s %j /%s exp: %s vote: %s', peer, keystr, entry.term, entry.expire, vote);
    this.outgoing.send([peer, this.identity, ANSWER, key,
                              uint2bin(entry.term), uint2bin(entry.expire), bool2bin(vote)]);
  };

  if (state === undefined) {
    /* 3.1. idle */
    this._storageGetOrCreateMeta(key).then(entry => {
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
};
