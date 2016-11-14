/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:announcement-handler');

const {
        FOLLOWER_STATE_SOURCE_TTL
      // states
      , STATE_FOLLOWER
      , STATE_SOURCING
      // utils
      , refreshStateTimout
      } = require('../common');

module.exports = exports = function processAnnouncement(peer, key, term, expire) {
  const keystr = key.toString('binary')
      , state = this._states.get(keystr);

  (state !== undefined
           ? Promise.resolve(state.entry)
           : this._storageGetOrCreateMeta(key))
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
        debug('received announcement, but sourcing already: %j', keystr);
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
      debug('following announced peer for: %j', keystr);
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

};
