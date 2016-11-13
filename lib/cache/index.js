/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const assert = require('assert');
const EventEmitter = require('events');
const zmq = require('zmq');

const debug = require('./debug')('ha-cache:cache');

const msgpack = require('msgpack-lite');

const {
        CACHE_TTL
      , CACHE_MIN_TTL
      , FOLLOWER_STATE_CANDIDATE_TTL
      , FOLLOWER_STATE_SOURCE_TTL
      , PING_INTERVAL
      // custom targets
      , EVERYONE
      // client response types
      , RES_VALUE
      // peer message types
      , QUESTION
      , ENTRYREQ
      , ANNOUNCE
      , UPDATE
      , PING
      // states
      , STATE_FOLLOWER
      // utils
      , uint2bin
      } = require('./common');


const RequestCache  = require('./request_cache');

const peerHandler   = require('./handlers/peer')
    , clientHandler = require('./handlers/client');

/*

entry:
{
  key
  term
  expire
  size
}

*/

class StatesMap extends Map {
  constructor(cache) {
    super();
  }
}

class Cache extends EventEmitter {
  /**
   * Create Cache server instance
   *
   * REQUIRED `options`:
   *
   * - `url` {string}: an zmq address of this server serving as its id and
   *         this is the address that other peers connect to and
   *         the default bind address
   * - `api` {string}: an api zmq address of this server on which it servers its clients
   *         this is the address that clients connect to and
   *         the default bind address
   * - `peers` {Array}: an array describing sibling peers which are obejcts with two
   *         properties: `url` and `api` describing each server in the cluster
   *         it's safe to include this server's descriptor but not necessary
   * - `source` {Function}: a function({String}) -> {Promise} which is being called
   *         when sourcing cache entry. Should resolve to entry value as {Buffer}.
   * - `storage` {object}: a pair of local storages for cache entries following storage api.
   *         it should consist of `meta` and `data` storages
   * - `statesMapClass` {Function}: a replacement class for `Cache.StatesMap`;
   *         currently `StatesMapEmitter` is an available replacement
   *
   * Additional `options` may be one of:
   *
   * - `bindUrl` {string}: a zmq address to bind socket that other servers connect to
   * - `bindApi` {string}: a zmq address to bind socket that clients connect to
   * - `ttl` {number}: entry cache time-to-live in milliseconds, default is 1 hour (3600000).
   *
   * @param {Object} options
   * @return {Cache}
  **/
  constructor(options) {
    super();
    if (!options || 'object' !== typeof options)
      throw new TypeError("Cache: options must be an object");
    if ('string' !== typeof options.url || options.url.length === 0)
      throw new TypeError("Cache: options.url must be specified as a string");
    /* address for peers to connect to with their SUBs */
    this.url = options.url;
    /* our identity message frame */
    this.identity = Buffer.from(this.url);
    if (options.bindUrl !== undefined && ('string' !== typeof options.bindUrl || options.bindUrl.length === 0))
      throw new TypeError("Cache: options.bindUrl must be specified as a string");
    /* address to bind our PUB to */
    this.bindUrl = options.bindUrl || this.url;
    if ('string' !== typeof options.api || options.api.length === 0)
      throw new TypeError("Cache: options.api must be specified as a string");
    if (options.bindApi !== undefined && ('string' !== typeof options.bindApi || options.bindApi.length === 0))
      throw new TypeError("Cache: options.bindApi must be specified as a string");
    /* address to bind our ROUTER to */
    this.apiUrl = options.api;
    this.bindApiUrl = options.bindApi || this.apiUrl;
    if (!Array.isArray(options.peers) || !options.peers.every(peer => (peer && 'object' === typeof peer
                                                        && 'string' === typeof peer.url && peer.url.length !== 0
                                                        && 'string' === typeof peer.api && peer.api.length !== 0))) {
      throw new TypeError("Cache: options.peers must be an array of peer descriptors");
    }
    /* a set of cluster server urls excluding our */
    this.peerUrls = new Set(options.peers.map(peer => peer.url));
    this.peerUrls.delete(this.url);
    /* peer identity message frames */
    this.peerIdentities = Array.from(this.peerUrls).map(url => [url, Buffer.from(url)]);
    /* list of all api urls in the cluster */
    var apiUrls = new Set(options.peers.map(peer => peer.api));
    apiUrls.add(this.apiUrl);
    assert.strictEqual(apiUrls.size, this.peerUrls.size + 1, "number of unique api urls does not match number of peer urls");
    /* list of api urls message frame for clients */
    this.apiUrlsPacked = msgpack.encode(Array.from(apiUrls));
    /* a majority required for decisions */
    this.majority = (apiUrls.size >>> 1) + 1;
    if (!options.storage || 'object' !== typeof options.storage
        || !options.storage.meta || 'object' !== typeof options.storage.meta
        || !options.storage.data || 'object' !== typeof options.storage.data) {
      throw new TypeError("Cache: options.storage must be an object containing meta and data storages");
    }
    if ('function' !== typeof options.storage.meta.get
        || 'function' !== typeof options.storage.meta.set
        || 'function' !== typeof options.storage.meta.delete) {
      throw new TypeError("Cache: options.storage.meta must be a storage");
    }
    if ('function' !== typeof options.storage.data.get
        || 'function' !== typeof options.storage.data.set
        || 'function' !== typeof options.storage.data.delete) {
      throw new TypeError("Cache: options.storage.data must be a storage");
    }
    this.dataStorage = options.storage.data; // only values
    this.metaStorage = options.storage.meta; // {key, term, expire, size}
    if ('function' !== typeof options.source)
      throw new TypeError("Cache: options.source must be a function");
    this.source = options.source;

    if (options.ttl !== undefined
        && ('number' !== typeof options.ttl
            || !isFinite(options.ttl)
            || options.ttl < CACHE_MIN_TTL)) {
      throw new TypeError("Cache: options.ttl must be a number >= " + CACHE_MIN_TTL);
    }
    this.ttl = options.ttl || CACHE_TTL; // cache entry expiration time in milliseconds

    /* queues for client requests */
    this._clientRequests = new RequestCache();
    /* key -> state map */
    const StatesMap = options.statesMapClass || Cache.StatesMap;
    if ('function' !== typeof StatesMap)
      throw new TypeError("Cache: StatesMap must be a function");
    this._states = new StatesMap(this);
    /* partitioned state */
    this.isPartitioned = false;
    this.pongResponses = new Set();
    this.pingInterval = null;
    /* client keep-alive handler */
    this._keepAliveInterval = null;
    /* pending close promise */
    this._closing = null;

    var incoming = this.incoming = zmq.socket('sub');
    incoming.setsockopt(zmq.ZMQ_LINGER, 0); // do not linger at all on close()
    incoming.on('message', peerHandler.bind(this));
    incoming.subscribe(EVERYONE); // subscribe to ventilator messages
    incoming.subscribe(this.identity); // subscribe to targeted messages

    var outgoing = this.outgoing = zmq.socket('pub');
    outgoing.setsockopt(zmq.ZMQ_LINGER, 0); // do not linger at all on close()
    outgoing.bind(this.bindUrl, err => {
      if (err) return this.emit('error', err);
      debug('broadcasting on: %s cluster peers: %s majority: %s', this.bindUrl, apiUrls.size, this.majority);
      /* connect to all peers */
      for(let url of this.peerUrls) incoming.connect(url);
    });

    var server = this.server = zmq.socket('router');
    server.setsockopt(zmq.ZMQ_LINGER, 1000); // do not linger forever on close()
    server.on('message', clientHandler.bind(this));
    server.bind(this.bindApiUrl, err => {
      if (err) return this.emit('error', err);
      debug('serving on: %s cache ttl: %s', this.bindApiUrl, this.ttl);
    });

  }

  /**
   * Clear states, disconnect and close Cache server connection.
   *
   * Emits 'close' event after all sockets are closed.
   *
   * @return {Promise}
  **/
  close() {
    if (this._closing) return this._closing;

    for(let state of this._states.values()) {
      if (state.timeout !== undefined) clearTimeout(state.timeout);
      if (state.interval !== undefined) clearInterval(state.interval);
    }
    this._states.clear();

    this._clientRequests.clear();
    this._checkClientRefreshInterval();

    this._clearPartitionedState();

    const incoming = this.incoming
        , outgoing = this.outgoing
        , server   = this.server;

    this.incoming = null;
    this.outgoing = null;
    this.server = null;

    incoming.removeAllListeners('message');
    incoming.unsubscribe(EVERYONE);
    incoming.unsubscribe(this.identity);

    for(let url of this.peerUrls) incoming.disconnect(url);
    incoming.close();

    return this._closing = Promise.all([
    , closeSocket(outgoing, 'outgoing')
    , closeSocket(server,   'server')
    ]).then(() => this.emit('close'));
  }

  /* PRIVATE: for internal use and debugging purposes */

  _storageGetOrCreateMeta(key) {
    return this.metaStorage.get(key)
    .then(entry => (entry || {key: key, term: 0, expire: 0, size: 0}));
  }

  _sendEntryToClients(keystr, entry, value) {
    const clientRequests = this._clientRequests
        , queue = clientRequests.fetch(keystr);
    if (queue !== undefined) {
      const server = this.server
          , expire = entry.expire
          , expbin = uint2bin(expire);
      for(var item of queue) {
        var [after, request] = item;
        if (expire > after) {
          request.push(RES_VALUE, value, expbin);
          debug('>>> CLIENT VALUE %s', keystr);
          server.send(request);
        }
        else clientRequests.add(keystr, item);
      }
    }
    this._checkClientRefreshInterval();
  }

  _checkClientRefreshInterval() {
    if (this._clientRequests.size === 0) {
      clearInterval(this._keepAliveInterval);
      this._keepAliveInterval = null;
    }
  }

  _clearPartitionedState() {
    clearInterval(this.pingInterval);
    this.pingInterval = null;
    this.isPartitioned = false;
  }

  _enterPartitionedState() {
    const pongResponses = this.pongResponses;
    debug("entering PARTITIONED state");
    clearInterval(this.pingInterval);
    this.isPartitioned = true;
    /* start pinging */
    this.pingInterval = setInterval(() => {
      pongResponses.clear();
      this.outgoing.send([EVERYONE, this.identity, PING]);
    }, PING_INTERVAL);
  }

  _sendQuestion(target, entry) {
    debug('>>> %s QUESTION %s /%s', target, entry.key, entry.term);
    this.outgoing.send([target, this.identity, QUESTION, entry.key, uint2bin(entry.term)]);
  }

  _sendEntryRequest(target, key) {
    debug('>>> %s ENTRYREQ %s', target, key);
    this.outgoing.send([target, this.identity, ENTRYREQ, key]);
  }

  _sendAnnouncement(target, entry) {
    debug('>>> %s ANNOUNCE %s /%s exp: %s', target, entry.key, entry.term, entry.expire);
    this.outgoing.send([target, this.identity, ANNOUNCE, entry.key, uint2bin(entry.term), uint2bin(entry.expire)]);
  }

  _sendUpdate(target, entry, value) {
    debug('>>> %s UPDATE %s /%s exp: %s', target, entry.key, entry.term, entry.expire);
    this.outgoing.send([target, this.identity, UPDATE, entry.key, uint2bin(entry.term), uint2bin(entry.expire), value]);
  }

  _setFollowerState(entry, keystr, peer, isSource) {
    var state = {type: STATE_FOLLOWER, entry: entry, keystr: keystr, peer: peer, isSource: isSource,
      timeoutcb: () => {
        /* 1.2 follower timeout */
        /* sanity check, this would never happen, we always clear timeouts before changing state */
        assert.strictEqual(state, this._states.get(keystr));
        this._states.delete(keystr);
      }
    };
    state.timeout = setTimeout(state.timeoutcb, isSource
                                                ? FOLLOWER_STATE_SOURCE_TTL
                                                : FOLLOWER_STATE_CANDIDATE_TTL);
    this._states.set(keystr, state);
    return state;
  }
}

Cache.StatesMap = StatesMap;
module.exports = Cache;

/* PRIVATE */
function closeSocket(socket, name) {
  return new Promise((resolve, reject) => {
    if (!socket) return resolve();
    const closed = () => {
      socket.removeListener('close', closed);
      socket.removeAllListeners('error');
      socket.unmonitor();
      debug('%s closed', name);
      resolve();
    };
    socket.removeAllListeners('message');
    socket.on('error', err => {
      debug('Error closing %s: %s', name, err);
      closed();
    });
    socket.on('close', closed);
    socket.monitor(10, 1);
    socket.close();
  });
}
