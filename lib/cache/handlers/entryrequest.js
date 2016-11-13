/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:entry-request-handler');

const isBuffer = Buffer.isBuffer;

module.exports = exports = function processEntryRequest(peer, key) {
  Promise.all([this.metaStorage.get(key),
               this.dataStorage.get(key)])
  .then(([entry, value]) => {

    if (entry != null && isBuffer(value)) {
      this._sendUpdate(peer, entry, value);
    }
    else debug('entry or value not found for: %s', key.toString('binary'));

  })
  .catch(err => {
    /* this is fatal, so better die */
    this.emit('error', err);
  });
};
