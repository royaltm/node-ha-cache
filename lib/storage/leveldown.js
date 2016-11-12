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
    const encode = this.encode = options.encode || msgpack.encode
        , decode = this.decode = options.decode || msgpack.decode;
    delete options.encode;
    delete options.decode;
    this.db = leveldown(location);
    debug('database: %s encode: %s, decode: %s', location
      , encode === passThroughAssertBuffer ? 'passthrough' : encode === msgpack.encode ? 'msgpack' : 'custom'
      , decode === passThrough ? 'passthrough' : decode === msgpack.decode ? 'msgpack' : 'custom');
    this.dboptions = Object.assign(DEFAULT_LEVELDOWN_OPTIONS, options);
    this.opened = false;
    this.opening = null;
  }

  open() {
    const db = this.db;
    if (!db) return Promise.reject(new Error("LevelDownStorage is closed"));
    if (this.opening) return this.opening;
    return this.opening = new Promise((resolve, reject) => {
      db.open(this.dboptions, err => (err ? reject(err) : resolve(this)));
    })
    .then(() => this.opened = true);
  }

  close() {
    const db = this.db;
    this.db = null;
    if (db && this.opening) {
      return this.opening.then(() => new Promise((resolve, reject) => {
        this.opened = false;
        db.close(err => (err ? reject(err) : resolve()));
      }));
    }
    else return Promise.resolve();
  }

  get(key) {
    assert(isBuffer(key), 'key is not a buffer!');
    if (!this.opened) return this.open().then(() => this.get(key));
    return new Promise((resolve, reject) => {
      const db = this.db;
      if (!db) return reject(new Error("LevelDownStorage is closed"));
      db.get(key, (err, buf) => {
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
      const db = this.db;
      if (!db) return reject(new Error("LevelDownStorage is closed"));
      const buf = this.encode(value);
      db.put(key, buf, PUT_OPTIONS, (err) => {
        if (err) return reject(err);
        resolve(value);
      });
    });
  }

  delete(key) {
    assert(isBuffer(key), 'key is not a buffer!');
    if (!this.opened) return this.open().then(() => this.delete(key));
    return new Promise((resolve, reject) => {
      const db = this.db;
      if (!db) return reject(new Error("LevelDownStorage is closed"));
      db.del(key, DEL_OPTIONS, (err) => {
        if (err) return reject(err);
        resolve();
      });
    });
  }

  iterator(options) {
    return new LevelDBPromiseIterator(this, options);
  }

}

const emptyResult = Object.freeze({});

class LevelDBPromiseIterator {
  constructor(cache, options) {
    const decode = cache.decode;
    this.iterator = null;
    const { keys, values } = Object.assign({keys: true, values: true}, options);
    if (keys && values) {
      this.decode = (key, value) => ({key, value: decode(value)});
    }
    else if (values) {
      this.decode = (key, value) => ({value: decode(value)});
    }
    else {
      this.decode = (key, value) => ({key: key});
    }
    this.pending = cache.open().then(() => {
      const db = cache.db;
      if (db) this.iterator = db.iterator(options);
    });
  }

  next() {
    const next = () => {
      return new Promise((resolve, reject) => {
        const iterator = this.iterator;
        if (!iterator) return resolve(emptyResult);
        iterator.next((err, key, value) => {
          if (err) return reject(err);
          if (key === undefined) {
            this.iterator = null;
            endIterator(iterator, resolve, reject);
          }
          else {
            resolve(this.decode(key, value));
          }
        });
      });
    };

    return this.pending = this.pending.then(next, next);
  }

  seek(key) {
    this.iterator.seek(key);
    return this;
  }

  end() {
    const end = () => {
      const iterator = this.iterator;
      this.iterator = null;
      return new Promise((resolve, reject) => endIterator(iterator, resolve, reject));
    };

    return this.pending = this.pending.then(end, end);
  }

}

LevelDBPromiseIterator.emptyResult = emptyResult;

function endIterator(iterator, resolve, reject) {
  if (!iterator) return resolve(emptyResult);
  iterator.end(err => {
    if (err) return reject(err);
    resolve(emptyResult);
  });
}

module.exports = LevelDownStorage;
