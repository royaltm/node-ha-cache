/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

class TestStorage {
  constructor() {
    this.data = new Map();
  }

  get(key) {
    return new Promise((resolve, reject) => {
      setTimeout(() => resolve(this.data.get(key)), (Math.random()*10)|0);
    });
  }

  set(key, value) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        this.data.set(key, value);
        resolve(value);
      }, (Math.random()*10)|0);
    });
  }

  delete(key) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        this.data.delete(key);
        resolve();
      }, (Math.random()*10)|0);
    });
  }
}

module.exports = TestStorage;
