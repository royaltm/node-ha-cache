/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const debug = require('../debug')('ha-cache:peer-handler');

const {
      // peer message types
        QUESTION_MATCH
      , ANSWER_MATCH
      , ENTRYREQ_MATCH
      , ANNOUNCE_MATCH
      , UPDATE_MATCH
      , PING_MATCH
      , PONG_MATCH
      // utils
      , bin2uint
      , bin2bool
      } = require('../common');

const processQuestion     = require('./question')
    , processAnswer       = require('./answer')
    , processEntryRequest = require('./entryrequest')
    , processUpdate       = require('./update')
    , processAnnouncement = require('./announcement')
    , processPing         = require('./ping')
    , processPong         = require('./pong');

module.exports = exports = function peerHandler(target, peer, type, key, term, expire, extra) {
  if (target === undefined || peer === undefined || type === undefined || type.length !== 1) {
    /* ignore trash */
    debug('received some trash');
    return;
  }
  var peerKey = peer.toString();
  if (!this.peerUrls.has(peerKey)) {
    /* check if this peer is in our setup */
    debug('unknown peer: %s [%s]: %s %s', peerKey, type, target, key);
    return;
  }
  try {
    switch(type[0]) {

      case QUESTION_MATCH:
        term = bin2uint(term);
        debug('recv.QUESTION: %s key: %s /%s', peer, key, term);
        return processQuestion.call(this, peer, key, term);

      case ANSWER_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        extra = bin2bool(extra);
        debug('recv.ANSWER: %s key: %s /%s exp: %s, vote: %s', peer, key, term, expire, extra);
        return processAnswer.call(this, peer, peerKey, key, term, expire, extra);

      case ENTRYREQ_MATCH:
        debug('recv.ENTRYREQ: %s key: %s', peer, key);
        return processEntryRequest.call(this, peer, key);

      case UPDATE_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        debug('recv.UPDATE: %s key: %s /%s exp: %s data: %d', peer, key, term, expire, extra.length);
        return processUpdate.call(this, peer, key, term, expire, extra);

      case ANNOUNCE_MATCH:
        term = bin2uint(term);
        expire = bin2uint(expire);
        debug('recv.ANNOUNCE: %s key: %s /%s exp: %s', peer, key, term, expire);
        return processAnnouncement.call(this, peer, key, term, expire);

      case PING_MATCH:
        debug('recv.PING: %s', peer);
        return processPing.call(this, peer);

      case PONG_MATCH:
        debug('recv.PONG: %s', peer);
        return processPong.call(this, peer);

      default:
        debug('recv.[%s] from %s', type, peer);
    }
  } catch (err) {
    this.emit('error', err);
  }
};
