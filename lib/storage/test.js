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
      setTimeout(() => resolve(this.data.get(key.toString())), (Math.random()*10)|0);
    });
  }

  set(key, entry) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        this.data.set(key.toString(), entry);
        resolve(entry);
      }, (Math.random()*10)|0);
    });
  }

  delete(key) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        this.data.delete(key.toString());
        resolve();
      }, (Math.random()*10)|0);
    });
  }
}

module.exports = TestStorage;
