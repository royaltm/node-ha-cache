/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const varint = require('../lib/varint');

test('should have functions', t => {
  t.type(varint.allocVarUIntLE, 'function');
  t.strictEquals(varint.allocVarUIntLE.length, 1);
  t.type(varint.writeVarUIntLE, 'function');
  t.strictEquals(varint.writeVarUIntLE.length, 3);
  t.type(varint.readVarUIntLE, 'function');
  t.strictEquals(varint.readVarUIntLE.length, 2);
  t.end();
});

test('allocVarUIntLE', t => {
  var buf;
  t.type(buf = varint.allocVarUIntLE(0), Buffer);
  t.deepEquals(Array.from(buf), [0]);
  t.type(buf = varint.allocVarUIntLE(2), Buffer);
  t.deepEquals(Array.from(buf), [2]);
  t.type(buf = varint.allocVarUIntLE(255), Buffer);
  t.deepEquals(Array.from(buf), [255]);
  t.type(buf = varint.allocVarUIntLE(256), Buffer);
  t.deepEquals(Array.from(buf), [0, 1]);
  t.type(buf = varint.allocVarUIntLE(65535), Buffer);
  t.deepEquals(Array.from(buf), [255, 255]);
  t.type(buf = varint.allocVarUIntLE(65536), Buffer);
  t.deepEquals(Array.from(buf), [0, 0, 1]);
  t.type(buf = varint.allocVarUIntLE(Number.MAX_SAFE_INTEGER), Buffer);
  t.deepEquals(Array.from(buf), [255,255,255,255,255,255,31]);
  t.throws(() => varint.allocVarUIntLE(Number.MAX_SAFE_INTEGER + 1), new Error("value is above MAX_SAFE_INTEGER"));
  t.end();
});

test('writeVarUIntLE', t => {
  var buf = Buffer.alloc(10);
  t.strictEquals(varint.writeVarUIntLE(0, buf), 1);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(1, buf), 1);
  t.deepEquals(Array.from(buf), [1,0,0,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(255, buf), 1);
  t.deepEquals(Array.from(buf), [255,0,0,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(256, buf), 2);
  t.deepEquals(Array.from(buf), [0,1,0,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(65535, buf), 2);
  t.deepEquals(Array.from(buf), [255,255,0,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(65536, buf), 3);
  t.deepEquals(Array.from(buf), [0,0,1,0,0,0,0,0,0,0]);
  t.strictEquals(varint.writeVarUIntLE(Number.MAX_SAFE_INTEGER, buf), 7);
  t.deepEquals(Array.from(buf), [255,255,255,255,255,255,31,0,0,0]);
  t.throws(() => varint.writeVarUIntLE(Number.MAX_SAFE_INTEGER + 1, buf), new Error("value is above MAX_SAFE_INTEGER"));
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(1, buf, 9), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,0,0,1]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(1, buf, 10), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,0,0,0]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(65535, buf, 8), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,0,255,255]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(16777215, buf, 7), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,255,255,255]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(16777215, buf, 9), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,0,0,0,0,0,255]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(Number.MAX_SAFE_INTEGER, buf, 3), 10);
  t.deepEquals(Array.from(buf), [0,0,0,255,255,255,255,255,255,31]);
  buf.fill(0);
  t.strictEquals(varint.writeVarUIntLE(Number.MAX_SAFE_INTEGER, buf, 4), 10);
  t.deepEquals(Array.from(buf), [0,0,0,0,255,255,255,255,255,255]);
  t.end();
});

test('readVarUIntLE', t => {
  var buf = Buffer.alloc(10);
  t.strictEquals(varint.readVarUIntLE(buf), 0);
  t.strictEquals(varint.readVarUIntLE(buf, 1), 0);
  t.strictEquals(varint.readVarUIntLE(buf, 9), 0);
  t.strictEquals(varint.readVarUIntLE(buf, 10), null);
  t.strictEquals(varint.readVarUIntLE(buf, -1), null);
  t.strictEquals(varint.readVarUIntLE(buf, -2), null);
  t.strictEquals(varint.readVarUIntLE(Buffer.alloc(0)), null);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([42])), 42);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([42]), 1), null);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([0, 0, 0, 0, 0, 0, 42]), 6), 42);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([0, 0, 0, 0, 0, 41, 42]), 5), 41 + 42*256);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([0, 0, 0, 0, 40, 41, 42]), 4), 40 + 41*256 + 42*65536);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([255,255,255,255,255,255,31,0,0,0])), Number.MAX_SAFE_INTEGER);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([255,255,255,255,255,255,31])), Number.MAX_SAFE_INTEGER);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([254,255,255,255,255,255,31,0,0,0])), Number.MAX_SAFE_INTEGER - 1);
  t.strictEquals(varint.readVarUIntLE(Buffer.from([255,1,2,3,4,5,6]), 1), 1 + 2*256 + 3*65536 + 4*16777216 + 5*4294967296 + 6*1099511627776);
  t.end();
});


test('random', t => {
  for(var i = 1000; i-- > 0; ) {
    var value = Math.round(Math.random()*Number.MAX_SAFE_INTEGER);
    var buf = varint.allocVarUIntLE(value);
    t.strictEquals(buf.length,  Math.ceil(Math.log2(value + 1)/8));
    t.strictEquals(varint.readVarUIntLE(varint.allocVarUIntLE(value)), value);
  }
  t.end();
});
