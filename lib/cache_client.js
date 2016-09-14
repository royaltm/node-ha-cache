/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 */
"use strict";
const zmq = require('zmq');
const debug = require('debug')('cache-client');

const msgpack = require('msgpack-lite');

const ZmqDealerSocket = require('./zmq_dealer_socket');

const { allocVarUIntLE: uint2bin, readVarUIntLE: bin2uint } = require('./varint');

const now = Date.now;
const max = Math.max;

const RES_VALUE_MATCH     = '='.charCodeAt(0);
const RES_SEEOTHER_MATCH  = '#'.charCodeAt(0);
const RES_SERVERS_MATCH   = '*'.charCodeAt(0);
const CMD_GET      = Buffer.from('?');
const CMD_DISCOVER = Buffer.from('*');

const CLIENT_INITIAL_TIMEOUT = 500;
const CLIENT_KEEP_ALIVE_TIMEOUT = 2100;
const CLIENT_MIN_TIMEOUT = 1500;
const RETRIEVE_SERVER_URLS_TIMEOUT = 2000;
const MIN_DISCOVERY_INTERVAL_TIMEOUT = 1000;

/*
  client = new CacheClient(['tcp://127.0.0.1:1234', 'tcp://127.0.0.1:1235', 'tcp://127.0.0.1:1236']);
  client.get('foo')
  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));
*/

class CacheClient extends ZmqDealerSocket {

  /**
   * Create ZmqDealerSocket
   *
   * `options` may be one of:
   *
   * - `urls` {string}: urls of the servers to connect to
   * - `timeout` {number}: default timeout in milliseconds
   * - `lazy` {boolean}: specify `true` to connect lazily on first request
   * - `sockopts` {Object}: specify zmq socket options as object e.g.: {ZMQ_IPV4ONLY: true}
   * - `highwatermark` {number}: shortcut to specify ZMQ_SNDHWM for a zmq DEALER socket
   *   this affects how many messages are queued per server so if one of the peers
   *   goes down this many messages are possibly lost
   *   (unless the server goes up and responds within the request timeout)
   *   default is 5
   * - `discoveryinterval` {number}: how many milliseconds to wait between retrieving
   *    the list of current server urls; set to 0 for never retrieve and -1 to retrieve once
   *                 
   *
   * @param {string|Array} [urls] - this overrides urls set in options
   * @param {number|Object} options or default timeout
   * @return {ZmqDealerSocket}
  **/
  constructor(urls, options) {
    super(urls, options);
    this.timeoutMs = CLIENT_INITIAL_TIMEOUT;
    this.defaultReqTimeoutMs = max(this.options.timeout || 0, CLIENT_MIN_TIMEOUT);
    var discoveryinterval = +this.options.discoveryinterval;
    if (isFinite(discoveryinterval)) {
      if (discoveryinterval < 0) {
        this.refreshServerUrls();
      }
      else if (discoveryinterval > 0) {
        discoveryinterval = max(discoveryinterval, MIN_DISCOVERY_INTERVAL_TIMEOUT);
        var setupDiscovery = () => {
          this.discoveryInterval = setTimeout(() => {
            this.refreshServerUrls().then(setupDiscovery, setupDiscovery);
          }, discoveryinterval)
          .unref();
        };
      }
    }
  }

  close() {
    if (this.discoveryInterval !== undefined) {
      clearTimeout(this.discoveryInterval);
    }
    return super.close();
  }

  refreshBy(args, lastTimeoutMs) {
    return args.length === 0 ? CLIENT_KEEP_ALIVE_TIMEOUT : -1;
  }

  refreshServerUrls() {
    return this.request([CMD_DISCOVER, uint2bin(now() + RETRIEVE_SERVER_URLS_TIMEOUT)]
                                                   , RETRIEVE_SERVER_URLS_TIMEOUT)
    .then(([type, value]) => {
      if (type[0] === RES_SERVERS_MATCH) {
        var peerUrls = msgpack.decode(value);
        this.setUrls(peerUrls);
        return peerUrls;
      }
      else throw new Error(`Unknown response type for peer list request: ${type.toString()}`);
    })
    .catch(err => {
      /* this should repeat forever */
      if (err.message === 'timeout') {
        debug('TIMEOUT, retrying');
        return this.refreshServerUrls();
      }
      else throw err
    });
  }

  get(key, timeoutMs) {
    var expire = now() + max(timeoutMs || this.defaultReqTimeoutMs, CLIENT_MIN_TIMEOUT);
    var request = () => this.request([CMD_GET, uint2bin(now() + CLIENT_INITIAL_TIMEOUT), key])
    .then(([type, value]) => {
      switch(type[0]) {
        case RES_VALUE_MATCH:
          return value;
        case RES_SEEOTHER_MATCH:
          debug('SEEOTHER, retrying');
          return request();
        default:
          throw new Error(`Unknown type: ${type.toString()}`);
      }
    })
    .catch(err => {
      if (err.message === 'timeout' && now() < expire) {
        debug('TIMEOUT, retrying');
        return request();
      }
      else throw err
    });

    return request();
  }
}

module.exports = CacheClient;
