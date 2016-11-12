/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 */
"use strict";

const co = require('co');
const Storage = require('../lib/storage/leveldown');


var meta = process.argv[2]
  , data = process.argv[3];

if (!meta || !data) throw new Error("Please specify: meta and data paths");

meta = new Storage(meta);
data = new Storage(data, {encode: false, decode: false});

Buffer.prototype.toJSON = function() {
  return this.toString('hex');
};

co(function *() {
  var it = meta.iterator();
  for(;;) {
    let {key, value: entry} = yield it.next();
    if (key === undefined) break;
    if (!key.equals(entry.key)) throw new Error(`key: "${key.toJSON()}" has not equal entry.key`);
    let value = yield data.get(key);
    if (!value) throw new Error(`key: "${key.toString()}" has no value`);
    if (value.length !== entry.size) throw new Error(`key: "${key.toJSON()}" has wrong value`);
    process.stdout.write(JSON.stringify(Object.assign(entry, {
      keystr: key.toString()
    , value: value.length
    })));
  }
  yield Promise.all([meta.close(), data.close()]);
})
.catch(err => console.warn(err));
