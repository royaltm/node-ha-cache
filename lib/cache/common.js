/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const assert = require('assert');

const now    = Date.now;
const random = Math.random;
const round  = Math.round;

const CANDIDATE_STATE_MAX_TTL   = 300;
const CANDIDATE_STATE_MIN_TTL   = 150;
const CANDIDATE_STATE_DELTA_TTL = CANDIDATE_STATE_MAX_TTL - CANDIDATE_STATE_MIN_TTL;
const randomExpire = () => round(random() * CANDIDATE_STATE_DELTA_TTL + CANDIDATE_STATE_MIN_TTL + now());

const CACHE_TTL     = exports.CACHE_TTL = 3600*1000;
const CACHE_MIN_TTL = exports.CACHE_MIN_TTL = 60000;

const CLIENT_REQ_REFRESH_INTERVAL  = exports.CLIENT_REQ_REFRESH_INTERVAL  = 1000;
const FOLLOWER_STATE_CANDIDATE_TTL = exports.FOLLOWER_STATE_CANDIDATE_TTL =  300;
const CANDIDATE_RETRY_QUESTION_TTL = exports.CANDIDATE_RETRY_QUESTION_TTL =  100;
const FOLLOWER_STATE_SOURCE_TTL    = exports.FOLLOWER_STATE_SOURCE_TTL    = 2000;
const SOURCE_ANNOUNCEMENT_INTERVAL = exports.SOURCE_ANNOUNCEMENT_INTERVAL = 1000;
const PING_INTERVAL                = exports.PING_INTERVAL                = 3000;

assert(CANDIDATE_RETRY_QUESTION_TTL < CANDIDATE_STATE_MAX_TTL);

/* custom targets */
const EVERYONE = exports.EVERYONE = Buffer.from('*');

// client commands
const CMD_GET_MATCH      = exports.CMD_GET_MATCH      = '?'.charCodeAt(0);
const CMD_DISCOVER_MATCH = exports.CMD_DISCOVER_MATCH = '*'.charCodeAt(0);

// client response types
const RES_VALUE    = exports.RES_VALUE    = Buffer.from('=');
const RES_SEEOTHER = exports.RES_SEEOTHER = Buffer.from('#');
const RES_SERVERS  = exports.RES_SERVERS  = Buffer.from('*');

// peer message types
const QUESTION       = exports.QUESTION       = Buffer.from('Q');
const QUESTION_MATCH = exports.QUESTION_MATCH = QUESTION[0];
const ANSWER         = exports.ANSWER         = Buffer.from('A');
const ANSWER_MATCH   = exports.ANSWER_MATCH   = ANSWER[0];
const ENTRYREQ       = exports.ENTRYREQ       = Buffer.from('E');
const ENTRYREQ_MATCH = exports.ENTRYREQ_MATCH = ENTRYREQ[0];
const ANNOUNCE       = exports.ANNOUNCE       = Buffer.from('!');
const ANNOUNCE_MATCH = exports.ANNOUNCE_MATCH = ANNOUNCE[0];
const UPDATE         = exports.UPDATE         = Buffer.from('=');
const UPDATE_MATCH   = exports.UPDATE_MATCH   = UPDATE[0];
const PING           = exports.PING           = Buffer.from('.');
const PING_MATCH     = exports.PING_MATCH     = PING[0];
const PONG           = exports.PONG           = Buffer.from('-');
const PONG_MATCH     = exports.PONG_MATCH     = PONG[0];

const STATE_FOLLOWER  = exports.STATE_FOLLOWER   = Symbol('follower');
const STATE_CANDIDATE = exports.STATE_CANDIDATE  = Symbol('candidate');
const STATE_SOURCING  = exports.STATE_SOURCING   = Symbol('sourcing');

const { allocVarUIntLE, readVarUIntLE } = require('../varint');

/* utils */
exports.randomExpire = randomExpire;
exports.uint2bin = allocVarUIntLE;
exports.bin2uint = readVarUIntLE;
exports.bin2bool = bin2bool;
exports.bool2bin = bool2bin;
exports.refreshStateTimout = refreshStateTimout;

const BINARY_FALSE = Buffer.alloc(0);
const BINARY_TRUE = Buffer.from([1]);

function bin2bool(buffer) {
  return buffer.length !== 0 && !!buffer[0];
}

function bool2bin(bool) {
  return bool ? BINARY_TRUE : BINARY_FALSE;
}

function refreshStateTimout(state, ttl) {
  clearTimeout(state.timeout);
  state.timeout = setTimeout(state.timeoutcb, ttl);
}
