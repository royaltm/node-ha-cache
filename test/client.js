"use strict";
const assert = require('assert');
const os = require('os');
const dns = require('dns');

const colors = require('colors/safe');

const port = +process.env.PORT || 11000;
const numpeers = process.argv[2]>>>0 || 1;

const hosts = [];
for(let arg of process.argv.slice(3)) hosts.push(arg);

const CacheClient = require('../lib/cache_client');

dns.lookup(os.hostname(), (err, address, family) => {
  assert.ifError(err);
  if (hosts.length === 0) hosts.unshift(family == 4 ? address : `[${address}]`);
  var peers = [];
  for(let host of hosts) {
    let hostpeers = new Array(numpeers);
    for(let i = hostpeers.length; i-- > 0; ) hostpeers[i] = `tcp://${host}:${port + 1000 + i}`;
    peers.push(...hostpeers);
  }
  var client = new CacheClient(peers, {discoveryinterval: -1, timeout: 50000});
  process.stdout.write(peers.map(url => colors.magenta(url)).join("\n") + "\n");

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
          client.get(key).then(entry => {
            console.log('got: "%s" data: %d exp: %d', key, entry.value.length, entry.expire);
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

