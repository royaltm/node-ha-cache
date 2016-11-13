/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

class RequestCache extends Map {
  constructor() {
    super();
  }

  add(key, entry) {
    var queue = this.get(key);
    if (queue === undefined) {
      this.set(key, queue = []);
    }
    queue.push(entry);
  }

  fetch(key) {
    var queue = this.get(key);
    if (queue !== undefined) {
      this.delete(key);
      return queue;
    }
  }
}

module.exports = exports = RequestCache;
