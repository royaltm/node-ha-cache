/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const LevelDownStorage = require('../lib/storage/leveldown');

test('should be a function', t => {
  t.type(LevelDownStorage, 'function');
  t.end();
});

test('LevelDownStorage', suite => {
  var storage = new LevelDownStorage('./tmp/db');

  suite.test('should get non-existent entry', t => {
    return storage.get('foo')
    .then(value => {
      t.strictEqual(value, null);
      t.end();
    })
  })
  .catch(suite.threw);

  suite.test('should set entry', t => {
    var entry = {bar: 'baz'}
    return storage.set('foo', entry)
    .then(value => {
      t.strictEqual(value, entry);
      return storage.get('foo')
      .then(value => {
        t.notStrictEqual(value, entry);
        t.deepEqual(value, entry);
        t.end();
      })
    })
  })
  .catch(suite.threw);

  suite.test('should delete entry', t => {
    return storage.delete('foo')
    .then(value => {
      t.strictEqual(value, undefined);
      return storage.get('foo')
      .then(value => {
        t.strictEqual(value, null);
        t.end();
      })
    })
  })
  .catch(suite.threw);

  suite.end();
});
