"use strict";
const assert = require('assert');
const os = require('os');
const dns = require('dns');

const colors = require('colors/safe');

const port = +process.env.PORT || 11000;
const numpeers = process.argv[2]>>>0 || 1;

const CacheClient = require('../lib/cache_client');

dns.lookup(os.hostname(), (err, address, family) => {
  assert.ifError(err);
  const host = family == 4 ? address : `[${address}]`;
  var peers = Array(numpeers);
  for(var i = peers.length; i-- > 0; ) peers[i] = `tcp://${host}:${port + 1000 + i}`;
  var client = new CacheClient(peers, 50000);

  const readline = require('readline');

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true
  });

  setImmediate(ask);

  function ask() {
    rl.question(colors.grey('> '), (cmd) => {
      var [key, times] = cmd.trim().split(/\s+/, 2);

      if (!key) return process.exit(0);
      else {
        console.log('key: %s', key);
        times = (times >>> 0) || 1;
        while(times-- > 0) {
          client.get(key).then(value => {
            console.log('got: %s: %d', key, value.length);
          })
          .catch(err => {
            console.log(colors.red('error: %s: %s'), key, err);
          });
        }
      }
      setImmediate(ask);
    });
  }

});

