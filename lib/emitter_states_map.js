/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

class StatesMap extends Map {
  constructor(cache) {
    super();
    this.cache = cache;
  }

  set(key, state) {
    var oldstate = this.get(key);
    super.set(key, state);
    this.cache.emit('statechange', key, (oldstate ? oldstate.type : 0), state.type);
  }

  delete(key) {
    var oldstate = this.get(key);
    super.delete(key);
    this.cache.emit('statechange', key, (oldstate ? oldstate.type : 0), 0);
  }
}

module.exports = StatesMap;
