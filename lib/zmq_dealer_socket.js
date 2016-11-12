/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";
const zmq = require('zmq');
const debug = require('debug')('ha-cache:zmq-socket');

const $handlers       = Symbol.for('handlers');
const $connected      = Symbol.for('connected');
const $sockopts       = Symbol.for('sockopts');
const $lastReqId      = Symbol.for('lastReqId');
const $nextRequestId  = Symbol.for('nextRequestId');
const $connectUrls    = Symbol.for('connectUrls');
const $setTimeout     = Symbol.for('setTimeout');

const ZmqBaseSocket = require('./zmq_base_socket');

/*
  ZmqDealerSocket is a handy wrapper for zmq DEALER socket that implements RPC pattern.

  ZmqDealerSocket allows to set timeout for every request, so they will be rejected after timeout occures.

  ZmqDealerSocket guarantees that responses will be correlated with requests.
  ZmqDealerSocket DOES NOT guarantee that the order of responses will be the same as the order of requests.

  The response is handled using promises.

  example:

  sock = new ZmqDealerSocket('tcp://127.0.0.1:1234', 2000);
  sock.request('foo')
  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));
*/

class ZmqDealerSocket extends ZmqBaseSocket {

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
   *                 
   *
   * @param {string|Array} [urls] - this overrides urls set in options
   * @param {number|Object} options or default timeout
   * @return {ZmqDealerSocket}
  **/
  constructor(urls, options) {
    super(urls, options);

    options = this.options;

    this.timeoutMs = options.timeout|0;
    if (!this[$sockopts].has(zmq.ZMQ_SNDHWM))
      this[$sockopts].set(zmq.ZMQ_SNDHWM, (options.highwatermark|0) || 5);
    this[$lastReqId] = 0;
    this[$handlers] = new Map();
    if (!options.lazy) this.connect();
  }

  /**
   * Send request
   *
   * @param {string|Array} req
   * @param {number} [timeoutMs]
   * @return {Promise}
  **/
  request(req, timeoutMs) {
    return new Promise((resolve, reject) => {
      if (!this[$connected]) this.connect();

      var requestId = this[$nextRequestId]();
      var handler = {resolve: resolve, reject: reject};

      if (!timeoutMs) timeoutMs = this.timeoutMs;
      if (timeoutMs > 0) {
        handler.timeoutMs = timeoutMs;
        handler.timeout = this[$setTimeout](requestId, handler, timeoutMs);
      }

      this[$handlers].set(requestId, handler);
      this.socket.send([requestId].concat(req));
    });
  }

  /**
   * Add new urls to server pool and optionally connect to them
   *
   * @param {string|Array} ...urls
   * @return {ZmqReqSocket}
   *
   * addUrls(...urls)
  **/

  /**
   * Remove urls from current server pool and disconnect from them
   *
   * @param {string|Array} ...urls
   * @return {ZmqReqSocket}
   *
   * removeUrls(...urls)
  **/

  /**
   * Connect to the new set of urls
   *
   * Unchanged urls will keep the current server connection.
   *
   * @param {string|Array} ...urls
   * @return {ZmqReqSocket}
   *
   * setUrls(...urls)
  **/

  /**
   * Disconnect, close socket, reject all pending requests and prevent further ones
   *
   * destroy()
  **/

  /**
   * Disconnect, close socket and reject all pending requests
   *
   * @return {ZmqDealerSocket}
  **/
  close() {
    super.close();
    // reject all handlers and clear timeouts
    for (var handler of this[$handlers].values()) {
      clearTimeout(handler.timeout);
      handler.reject(new Error("closed"));
    }
    // clear handler
    this[$handlers].clear();
    return this;
  }

  /**
   * Override this method to handle refresh timeout messages
   *
   * @return {number}
  **/
  refreshBy(args, lastTimeoutMs) {
    return -1;
  }

  /**
   * Connect socket
   *
   * @return {ZmqDealerSocket}
  **/
  connect() {
    if (this[$connected]) return;
    var socket = this.socket || (this.socket = zmq.socket('dealer'));
    socket.setsockopt(zmq.ZMQ_LINGER, 0); // makes sure socket is really closed when close() is called
    for(let [opt, val] of this[$sockopts]) socket.setsockopt(opt, val);
    this[$connectUrls](this.urls);
    this[$connected] = true;
    socket.on('message', (requestId, ...args) => {
      requestId = requestId.toString();
      // now get the handler assiciated with requestId
      var handler = this[$handlers].get(requestId);
      if (!handler) {
        debug("socket.recv: received requestId: %s doesn't have a correlated response handler", requestId);
        return;
      }
      // since we received the reply, clear the timeout
      clearTimeout(handler.timeout);
      // check for timeout refresh message
      var timeoutMs = this.refreshBy(args, handler.timeoutMs);
      if (timeoutMs > 0) {
        handler.timeout = this[$setTimeout](requestId, handler, timeoutMs);
      }
      else {
        // remove handler
        this[$handlers].delete(requestId);
        // resolve handler promise
        handler.resolve(args);
      }
    });
    return this;
  }

  /**
   * Get zmq socket option from the underlaying socket
   *
   * @param {string} opt
   * @return {*}
   *
   * getsockopt(opt)
  **/

  /**
   * Set zmq socket option on the underlaying socket
   *
   * @param {string} opt
   * @param {*} value
   * @return {ZmqDealerSocket}
   *
   * setsockopt(opt, value)
  **/

  [$nextRequestId]() {
    var id = (this[$lastReqId] + 1)>>>0;
    this[$lastReqId] = id;
    return id.toString(36);
  }

  [$setTimeout](requestId, handler, timeoutMs) {
    return setTimeout(() => {
      // remove handler
      this[$handlers].delete(requestId);
      // reject handler
      handler.reject(new Error("timeout")); // TODO: make timeout error class
    }, timeoutMs);
  }

}

module.exports = ZmqDealerSocket;
