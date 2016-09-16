/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 *
 *  Fast and GC friendly converter of any unsigned integer to variable binary
 *
 */
"use strict";

const min = Math.min;
const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const POOL_SIZE        = 8192;

const offsetLimit = POOL_SIZE - 8;
var pool          = Buffer.allocUnsafe(POOL_SIZE);
var poolOffset    = 0;

exports.allocVarUIntLE = allocVarUIntLE;
exports.writeVarUIntLE = writeVarUIntLE;
exports.readVarUIntLE  = readVarUIntLE;

/**
 * Write unsigned integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns a new buffer object pointing to the data allocated from internal pool.
 *
 * `value` must be less or equal to MAX_SAFE_INTEGER and greater or equal to 0.
 *
 * @param {number} value
 * @return {Buffer}
**/
function allocVarUIntLE(value) {
  var offset = poolOffset;
  if (offset > offsetLimit) {
    pool = Buffer.allocUnsafe(POOL_SIZE);
    offset = 0;
  }
  return pool.slice(offset, poolOffset = writeVarUIntLE(value, pool, offset));
}

/**
 * Write unsigned integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns offset position in the buffer after the last written byte.
 *
 * `value` must be less or equal to MAX_SAFE_INTEGER and greater or equal to 0.
 *
 * @param {number} value
 * @param {Buffer} buffer
 * @param {number} [offset] at which to begin writing
 * @return {number}
**/
function writeVarUIntLE(value, buffer, offset) {
  value = +value;
  offset = offset >>> 0;
  if (value > MAX_SAFE_INTEGER) throw new Error("value is above MAX_SAFE_INTEGER");

  const limit = buffer.length - 1;
  if (offset > limit) return offset;

  buffer[offset] = value;
  for (var byte, mul = 0x100;
      (byte = (value / mul) >>> 0) !== 0 && offset < limit;
      mul *= 0x100) buffer[++offset] = byte;

  return offset + 1;
}

/**
 * Read variable unsigned integer from the buffer in LSB-first order
 *
 * The variable is being read from up to 7 bytes or to the end of the buffer.
 * Returns decoded number or null if buffer after given offset was empty.
 *
 * @param {Buffer} buffer
 * @param {number} [offset] to start reading from
 * @return {number|null}
**/
function readVarUIntLE(buffer, offset) {
  offset = offset >>> 0;
  const limit = min(buffer.length - 1, offset + 7);
  if (offset > limit) return null;

  var val = buffer[offset];
  for (var mul = 0x100; offset < limit; mul *= 0x100)
    val += buffer[++offset] * mul;

  return val;
}

/*
const ben=require('ben');
var b=Buffer.alloc(10)
b.fill(0);
const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
var allocVarUIntLE = require('./lib/varint').allocVarUIntLE;
var writeVarUIntLE = require('./lib/varint').writeVarUIntLE;
var readVarUIntLE  = require('./lib/varint').readVarUIntLE;
ben(10000000,()=>{allocVarUIntLE(MAX_SAFE_INTEGER)})
ben(10000000,()=>{writeVarUIntLE(MAX_SAFE_INTEGER,b,0)})
ben(10000000,()=>{b.writeUIntLE(MAX_SAFE_INTEGER,0,8)})
ben(10000000,()=>{readVarUIntLE(b,0)})
ben(10000000,()=>{b.readUIntLE(0,8)})
ben(10000000,()=>{writeVarUIntLE(1,b,0)})
ben(10000000,()=>{b.writeUIntLE(1,0,8)})
ben(10000000,()=>{readVarUIntLE(b,0)})
ben(10000000,()=>{b.readUIntLE(0,1)})
*/
