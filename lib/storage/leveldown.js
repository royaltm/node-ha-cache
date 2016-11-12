/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 */
"use strict";

const isBuffer = Buffer.isBuffer;
const assert = require('assert');
const leveldown = require('leveldown');
const msgpack = require('msgpack-lite');
const PUT_OPTIONS = {sync: true};
const DEL_OPTIONS = {sync: true};
const NOT_FOUND_ERR_MSG = "NotFound: ";
const debug = require('debug')('ha-cache:storage:leveldown');

const passThroughAssertBuffer = (value) => {
  assert(isBuffer(value), "value is not a buffer, needs encoding");
  return value;
};

const passThrough = (value) => value;

const DEFAULT_LEVELDOWN_OPTIONS = {
  'createIfMissing': true,
  'errorIfExists': false,
  'compression': true
}

class LevelDownStorage {
  constructor(location, options) {
    options = Object.assign({}, options);
    if (options.encode === false) options.encode = passThroughAssertBuffer;
    if (options.decode === false) options.decode = passThrough;
    this.encode = options.encode || msgpack.encode;
    this.decode = options.decode || msgpack.decode;
    delete options.encode;
    delete options.decode;
    this.db = leveldown(location);
    debug('database: %s', location);
    this.dboptions = Object.assign(DEFAULT_LEVELDOWN_OPTIONS, options);
    this.opened = false;
    this.opening = null;
  }

  open() {
    if (this.opening) return this.opening;
    return this.opening = new Promise((resolve, reject) => {
      this.db.open(this.dboptions, err => (err ? reject(err) : resolve(this)));
    })
    .then(() => this.opened = true);
  }

  close() {
    if (this.closing) return this.closing;
    this.opening = Promise.reject(new Error("database closed"));
    this.opened = false;
    return this.closing = new Promise((resolve, reject) => {
      this.db.close(err => (err ? reject(err) : resolve(this)));
    });
  }

  get(key) {
    assert(isBuffer(key), 'key is not a buffer!');
    if (!this.opened) return this.open().then(() => this.get(key));
    return new Promise((resolve, reject) => {
      this.db.get(key, (err, buf) => {
        if (err) {
          if (err.message === NOT_FOUND_ERR_MSG)
            resolve(null);
          else
            reject(err);
        }
        else {
          try {
            resolve(buf && this.decode(buf));
          } catch(err) {
            reject(err);
          }
        }
      });
    });
  }

  set(key, value) {
    assert(isBuffer(key), 'key is not a buffer!');
    if (!this.opened) return this.open().then(() => this.set(key, value));
    return new Promise((resolve, reject) => {
      var buf = this.encode(value);
      this.db.put(key, buf, PUT_OPTIONS, (err) => {
        if (err) return reject(err);
        resolve(value);
      });
    });
  }

  delete(key) {
    assert(isBuffer(key), 'key is not a buffer!');
    if (!this.opened) return this.open().then(() => this.delete(key));
    return new Promise((resolve, reject) => {
      this.db.del(key, DEL_OPTIONS, (err) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

}

module.exports = LevelDownStorage;
