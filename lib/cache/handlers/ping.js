/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:ping-handler');

const { PONG } = require('../common');

module.exports = exports = function processPing(peer) {
  this.outgoing.send([peer, this.identity, PONG]);
};
