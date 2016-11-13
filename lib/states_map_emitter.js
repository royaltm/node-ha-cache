/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const STATE_IDLE = Symbol('idle');

class StatesMapEmitter extends Map {
  constructor(cache) {
    super();
    this.cache = cache;
  }

  set(key, state) {
    var oldstate = this.get(key);
    super.set(key, state);
    this.cache.emit('statechange', key, (oldstate ? oldstate.type : STATE_IDLE), state.type);
  }

  delete(key) {
    var oldstate = this.get(key);
    super.delete(key);
    this.cache.emit('statechange', key, (oldstate ? oldstate.type : STATE_IDLE), STATE_IDLE);
  }
}

module.exports = StatesMapEmitter;
