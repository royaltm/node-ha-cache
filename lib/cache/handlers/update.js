/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:update-handler');

const now = Date.now;

const { STATE_SOURCING } = require('../common');

module.exports = exports = function processUpdate(peer, key, term, expire, value) {
  const keystr = key.toString('binary')
      , state = this._states.get(keystr);

  (state !== undefined
           ? Promise.resolve(state.entry)
           : this._storageGetOrCreateMeta(key))
  .then(entry => {
    var shouldUpdateMeta = false
      , shouldUpdateAll = false;

    if (term > entry.term) {
      shouldUpdateMeta = true;
      entry.term = term;
    }

    if (expire > entry.expire) {
      shouldUpdateAll = true;
      entry.expire = expire;
      entry.size = value.length;
      this._sendEntryToClients(keystr, entry, value);
    }

    debug('should update all: %s or meta only: %s', shouldUpdateAll, shouldUpdateMeta);

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
    const state = this._states.get(keystr);
    if (expire > now() && state !== undefined && state.type !== STATE_SOURCING) {
      /* drop to idle */
      debug('dropping to idle on update: %j', keystr);
      clearTimeout(state.timeout);
      this._states.delete(keystr);
    }
  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
};
