/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const ZmqDealerSocket = require('../lib/zmq_dealer_socket');
const zmq = require('zmq');

test('should be a function', t => {
  t.type(ZmqDealerSocket, 'function');
  t.end();
});

test('router', suite => {
  var [router, url] = createZmqSocket('router');

  suite.test('test requests', t => {
    t.plan(13);
    var socket = new ZmqDealerSocket(url);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('message', (src, id, msg) => {
          try {
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.type(msg, Buffer);
            t.notStrictEquals(src.length, 0);
            t.notStrictEquals(id.length, 0);
            t.strictEquals(msg.toString(), "foo");
            router.send([src, id, "foo", "bar"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").then(res => {
        t.type(res, Array);
        t.type(res.length, 2);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.strictEquals(res[0].toString(), "foo");
        t.strictEquals(res[1].toString(), "bar");
        return new Promise((resolve, reject) => {
          router.unbind(url, err => {
            if (err) return reject(err);
            resolve();
          });          
        });
      })
      .then(() => {
        router.close();
        socket.close();
        t.ok(true);
      })
    ]).catch(t.threw);
  });

  suite.test('test request timeout', t => {
    t.plan(4);
    var socket = new ZmqDealerSocket(url, 100);
    var start = Date.now();
    socket.request("foo").catch(err => {
      t.type(err, Error);
      t.strictEquals(err.message, "timeout");
      t.ok(Date.now() - start > 100);
    })
    .then(() => {
      socket.close();
      t.ok(true);
    })
    .catch(t.threw);
  });

  suite.end();
});

function createZmqSocket(type) {
  var url, sock = zmq.socket(type);
  sock.setsockopt(zmq.ZMQ_LINGER, 0);
  do {
    url = 'tcp://127.0.0.1:' + ((Math.random()*20000 + 10000) >>> 0);
    try {
      sock.bindSync(url);
    } catch(err) {
      url = undefined
    }
  } while(url === undefined);
  return [sock, url];
}
