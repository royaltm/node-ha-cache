/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 */
"use strict";
const zmq = require('zmq');
const debug = require('debug')('zmq-socket');

const now = Date.now;
const $timeout        = Symbol.for('timeout');
const $handlers       = Symbol.for('handlers');
const $connected      = Symbol.for('connected');
const $sockopts       = Symbol.for('sockopts');
const $setupTimeout   = Symbol.for('setupTimeout');
const $rejectAll      = Symbol.for('rejectAll');
const $connectUrls    = Symbol.for('connectUrls');

const ZmqBaseSocket = require('./zmq_base_socket');

/*
  ZmqReqSocket is a handy wrapper for zmq REQ socket that implements RPC pattern.

  ZmqReqSocket allows to set timeout for all requests, so they will be rejected after timeout occures.

  Due to the nature of REQ socket (the fact that REQ will not send another message until the response is received)
  the timeout will also reject all currently queued requests.

  ZmqReqSocket guarantees that the order of responses will be the same as the order of requests.

  The response is handled using promises.

  example:

  sock = new ZmqReqSocket('tcp://127.0.0.1:1234', 2000);
  sock.request('foo')
  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));
*/

class ZmqReqSocket extends ZmqBaseSocket {

  /**
   * Create ZmqReqSocket
   *
   * `options` may be one of:
   *
   * - `urls` {string}: urls of the servers to connect to
   * - `timeout` {number}: timeout in milliseconds
   * - `lazy` {boolean}: specify `true` to connect lazily on first request
   * - `sockopts` {Object}: specify zmq socket options
   *
   * @param {string|Array} [urls] - this overrides urls set in options
   * @param {number|Object} options or default timeout
   * @return {ZmqDealerSocket}
  **/
  constructor(urls, options) {
    super(urls, options);

    options = this.options;

    Object.defineProperty(this, 'timeoutMs', {
      value: options.timeout|0,
      enumerable: true
    });
    this[$timeout] = null;
    this[$handlers] = [];
    if (!options.lazy) this.connect();
  }

  /**
   * @property connected {boolean}
  **/

  /**
   * Send request
   *
   * @param {string|Array} req
   * @return {Promise}
  **/
  request(req) {
    return new Promise((resolve, reject) => {
      if (!this[$connected]) this.connect();

      var handler = {resolve: resolve, reject: reject};

      if (this.timeoutMs > 0) {
        if (this[$timeout] === null) {
          this[$setupTimeout](this.timeoutMs);
        }
        else {
          handler.expire = now() + this.timeoutMs;
        }
      }

      this[$handlers].push(handler);
      this.socket.send(req);
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
   * @return {ZmqReqSocket}
  **/
  close() {
    super.close();
    clearTimeout(this[$timeout]);
    this[$timeout] = null;
    this[$rejectAll](new Error("closed"));
    return this;
  }

  /**
   * Connect socket
   *
   * @return {ZmqReqSocket}
  **/
  connect() {
    if (this[$connected]) return;
    var socket = this.socket || (this.socket = zmq.socket('req'));
    socket.setsockopt(zmq.ZMQ_LINGER, 0); // makes sure socket is really closed, when .close() is called
    for(let [opt, val] of this[$sockopts]) socket.setsockopt(opt, val);
    this[$connectUrls](this.urls);
    this[$connected] = true;
    socket.on('message', (...msgs) => {
        // since we received the reply, clear the timeout and remove the timeout handler
        clearTimeout(this[$timeout]);
        // now get the first handler in queue
        var handler = this[$handlers].shift();
        // set next timeout if applies or clear the last one
        var expire = handler.expire;
        if (expire !== undefined) {
          this[$setupTimeout](expire - now());
        }
        else this[$timeout] = null;
        // resolve handler promise
        handler.resolve(msgs);
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

  [$setupTimeout](timeoutMs) {
    this[$timeout] = setTimeout(() => {
      this[$timeout] = null;
      super.close();
      this.connect();
      // reject all queued handlers since REQ socket became unusable now with all queued messages to send next
      this[$rejectAll](new Error("timeout")); // TODO: make timeout error class
    }, timeoutMs);
  }

  [$rejectAll](error) {
    for (var handler of this[$handlers]) handler.reject(error);
    this[$handlers].length = 0;
  }

}

module.exports = ZmqReqSocket;
