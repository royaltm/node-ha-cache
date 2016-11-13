/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:answer-handler');

const now = Date.now;

const {
        CANDIDATE_RETRY_QUESTION_TTL
      // states
      , STATE_CANDIDATE
      // utils
      , randomExpire
      , refreshStateTimout
      } = require('../common');

const startSourcing = require('./sourcing');

module.exports = exports = function processAnswer(peer, peerKey, key, term, expire, vote) {
  const keystr = key.toString('binary')
      , state = this._states.get(keystr);

  var entry;

  const updateTermOrDie = () => {
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
      debug('voting aborted: %s', keystr);
      if (term > entry.term) updateTermOrDie();
      return;
    }
    /* find best source of expired entry to fetch */
    if (expire > state.reqExpire) {
      state.reqPeer = peer;
      state.reqExpire = expire;
    }

    debug("vote %s from: %s exp: %s term: %s peer's term: %s best: %s", vote, peerKey, expire, entry.term, term, state.reqPeer);

    if (expire > now()) {
      debug('peer %s has fresh entry: %s', peerKey, keystr);
      /* wow, someone has a fresh one, fetch immediately, abort voting and refresh timeout */
      state.expire = randomExpire();
      state.aborted = true;
      refreshStateTimout(state, CANDIDATE_RETRY_QUESTION_TTL);
      this._sendEntryRequest(peer, key);
      /* update term if necessary */
      if (term > entry.term) updateTermOrDie();
    }
    else if (term === entry.term) {
      debug('vote on our term: %s agreed: %s/%s', vote, state.agreed + vote, this.majority);
      if (vote && ++state.agreed >= this.majority) {
        /* start sourcing */
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
      debug('our term too low: %s < %s aborting %s', entry.term, term, keystr);
      /* abort voting and let it timeout */
      state.aborted = true;
      updateTermOrDie();
    }
  } /* 7.2 finish processing */
};
