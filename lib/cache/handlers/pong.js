/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:pong-handler');

module.exports = exports = function processPong(peer, peerKey) {
  if (this.isPartitioned) {
    const pongResponses = this.pongResponses;

    pongResponses.add(peerKey);

    if (pongResponses.size + 1 >= this.majority) {
      debug('step down from PARTITIONED state');
      this._clearPartitionedState();
    }
  }
};
