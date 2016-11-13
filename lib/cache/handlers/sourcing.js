/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const assert = require('assert');

const debug = require('../debug')('ha-cache:sourcing');

const isBuffer = Buffer.isBuffer;
const now = Date.now;

const {
        SOURCE_ANNOUNCEMENT_INTERVAL
      // custom targets
      , EVERYONE
      // states
      , STATE_SOURCING
      } = require('../common');

module.exports = exports = function startSourcing(keystr, entry) {
  const state = {type: STATE_SOURCING, entry: entry, keystr: keystr}
      , key = entry.key;

  this._states.set(keystr, state);

  debug('sourcing start: %s', keystr);

  const announce = () => {
    this._sendAnnouncement(EVERYONE, entry);
  };

  state.interval = setInterval(announce, SOURCE_ANNOUNCEMENT_INTERVAL);
  announce();

  Promise.resolve().then(() => this.source(key))
  .then(value => {
    assert(isBuffer(value), "value resolved from source is not a buffer");

    debug('sourcing done: %s', keystr, value.length);

    entry.expire = now() + this.ttl;
    entry.size = value.length;

    return this.dataStorage.set(key, value)
    .then(() => this.metaStorage.set(key, entry))
    .then(entry => {
      this._sendEntryToClients(keystr, entry, value);
      /* send update */
      this._sendUpdate(EVERYONE, entry, value);
      /* drop to idle */
      clearInterval(state.interval);
      this._states.delete(keystr);
    });

  })
  .catch(err => {
    /* drop to idle */
    debug('ERROR while sourcing %s: %s', keystr, err);
    console.warn(err.stack);
    clearInterval(state.interval);
    this._states.delete(keystr);
    // TODO: what about clients? they will timeout eventually and try again
  });
};
