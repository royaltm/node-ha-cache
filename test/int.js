"use strict";
const assert = require('assert');
const os = require('os');
const path = require('path');
const dns = require('dns');
const cluster = require('cluster');
const util = require('util');
const crypto = require('crypto');
const colors = require('colors/safe');
const Cache = require('../lib/cache');
Cache.StatesMap = require('../lib/emitter_states_map');
// const Storage = require('../lib/storage/test');
const Storage = require('../lib/storage/leveldown');

const pid = process.pid;

const port = +process.env.PORT || 11000;
const numpeers = process.argv[2]>>>0 || 1;
const initialDelay = process.argv[3]>>>0;
const hosts = [];
for(let arg of process.argv.slice(3)) hosts.push(arg);

function source(keystr) {
  process.send({text: `${pid}: source: ${keystr}`});
  return new Promise((resolve, reject) => {
    crypto.randomBytes(Math.random()*10000|0, (e, bytes) => {
      assert.ifError(e);
      setTimeout(() => resolve(bytes), Math.random()*10000+5000|0);
    });
  });
}

dns.lookup(os.hostname(), (err, address, family) => {
  assert.ifError(err);
  var addr = family == 4 ? address : `[${address}]`;
  if (hosts.length === 0) hosts.unshift(addr);
  var peers = [];
  for(let host of hosts) {
    let hostpeers = new Array(numpeers)
    for(let i = hostpeers.length; i-- > 0; ) hostpeers[i] = {
      id: String(100 + i).substr(1),
      url: `tcp://${host}:${port + i}`,
      api: `tcp://${host}:${port + 1000 + i}`,
      bindUrl: `tcp://${addr}:${port + i}`,
      bindApi: `tcp://${addr}:${port + 1000 + i}`,
    }
    peers.push(...hostpeers);
  }

  if (cluster.isMaster) {
    const readline = require('readline');

    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: true
    });
    const currentPeers = new Map();
    var currentActive;
    // Fork workers.
    const peersmap = new Map();
    const peerurltoid = new Map();
    const freepeers = [];
    for (let [i, peer] of shuffle(peers.slice(0, numpeers)).entries()) {
      setTimeout(() => {
        peersmap.set(peer.id, peer);
        peerurltoid.set(peer.url, peer.id);
        console.log('forking: %s: %s', peer.id, peer.url);
        peer.worker = cluster.fork({URL: peer.url, API: peer.api, BIND_URL: peer.bindUrl, BIND_API: peer.bindApi
                      , STORAGE: path.resolve(__dirname, '..', 'tmp', peer.id)});
      }, i*initialDelay);
      // freepeers.push(...peers.slice(1));
      // break;
    }

    function ask() {
      rl.question(colors.grey('> '), (cmd) => {
        cmd = cmd.trim();
        processCmd();

        function processCmd() {
          if (!cmd) return setImmediate(ask);
          if (cmd === 'q') {
            cmd = '';
            rl.close();
            Object.keys(cluster.workers).map(w => cluster.workers[w]).forEach(worker => {
              worker.send('shutdown');
            });
            setTimeout(() => {
              cluster.disconnect(() => {
                process.exit(0);
              });
            }, 500);
          }
          else if (cmd === 'l') {
            cmd = '';
            for(let peer of peersmap.values()) peer.worker.send('status');
          }
          else if (cmd.substr(0, 1) === 'w') {
            let micros = cmd.substr(1).match(/\d+/);
            if (micros) {
              cmd = cmd.substr(1 + micros[0].length);
              setTimeout(processCmd, +micros[0]);
              return;
            } else
              cmd = cmd.substr(1);
          }
          else if (cmd.substr(0, 1) === '=') {
            let count = cmd.substr(1).match(/\d+/);
            if (count) {
              cmd = cmd.substr(1 + count[0].length);
              count = +count[0];
              if (count > peersmap.size && count <= numpeers) {
                do {
                  let peer = sample(freepeers);
                  console.log('forking: %s: %s', peer.id, peer.url);
                  peer.worker = cluster.fork({URL: peer.url, API: peer.api, BIND_URL: peer.bindUrl, BIND_API: peer.bindApi
                                , STORAGE: path.resolve(__dirname, '..', 'tmp', peer.id)});
                  peersmap.set(peer.id, peer);
                } while(count > peersmap.size);
              }
              else if (count < peersmap.size) {
                for(count = peersmap.size - count; count-- > 0; ) {
                  let peer = sample(Array.from(peersmap.values()));
                  freepeers.push(peer);
                  peersmap.delete(peer.id);
                  let worker = peer.worker;
                  peer.worker = null;
                  worker.kill('SIGTERM');
                }
              }
            } else
              cmd = cmd.substr(1);
          }
          else if (cmd === '--') {
            cmd = '';
            for(var count = random(1, peersmap.size); count-- > 0; ) {
              let peer = sample(Array.from(peersmap.values()));
              freepeers.push(peer);
              peersmap.delete(peer.id);
              let worker = peer.worker;
              peer.worker = null;
              worker.kill('SIGTERM');
            }
          }
          else if (cmd.substr(0, 1) === '-') {
            let id = cmd.substr(1, 2);
            cmd = cmd.substr(3);
            let peer = peersmap.get(id);
            if (peer) {
              freepeers.push(peer);
              peersmap.delete(id);
              let worker = peer.worker;
              peer.worker = null;
              worker.kill('SIGTERM');
            }
          }
          else if (cmd.substr(0, 1) === '+') {
            cmd = cmd.substr(1);
            let peer = freepeers.shift();
            if (peer) {
              console.log('forking: %s: %s', peer.id, peer.url);
              peer.worker = cluster.fork({URL: peer.url, API: peer.api, BIND_URL: peer.bindUrl, BIND_API: peer.bindApi});
              peersmap.set(peer.id, peer);
            }
          }
          // else if (cmd.substr(0, 1) === 'u') {
          //   let id = cmd.substr(1, 2);
          //   let idfrom = cmd.substr(3, 2);
          //   cmd = cmd.substr(5);
          //   let peer = peersmap.get(id);
          //   peer && peer.worker.send('unsubscribe:' + idfrom);
          //   if ((peer = peersmap.get(idfrom)))
          //     peer.worker.send('unsubscribe:' + peer.url);
          // }
          // else if (cmd.substr(0, 1) === 's') {
          //   let id = cmd.substr(1, 2);
          //   let idto = cmd.substr(3, 2);
          //   cmd = cmd.substr(5);
          //   let peer = peersmap.get(id);
          //   peer && peer.worker.send('subscribe:' + idto);
          //   if ((peer = peersmap.get(idto)))
          //     peer.worker.send('subscribe:' + id);
          // }
          // else if (cmd.substr(0, 1) === 'c') {
          //   let unpartition = [];
          //   cmd = cmd.substr(1);
          //   while (/^\d\d$/.test(cmd.substr(0,2))) {
          //     let id = cmd.substr(0, 2);
          //     cmd = cmd.substr(2);
          //     let peer = peersmap.get(id);
          //     if (peer) unpartition.push(peer);
          //   }
          //   for(let peer of unpartition) {
          //     for(let peerto of peersmap.values()) {
          //       peer.worker.send('subscribe:' + peerto.id);
          //       peerto.worker.send('subscribe:' + peer.id);
          //     }
          //   }
          // }
          // else if (cmd.substr(0, 1) === 'p') {
          //   let partition = [], rest = new Map(Array.from(peersmap));
          //   cmd = cmd.substr(1);
          //   while (/^\d\d$/.test(cmd.substr(0,2))) {
          //     let id = cmd.substr(0, 2);
          //     cmd = cmd.substr(2);
          //     let peer = peersmap.get(id);
          //     if (peer) {
          //       partition.push(peer);
          //       rest.delete(peer.id);
          //     }
          //   }
          //   for(let peer of partition) {
          //     for(let peerfrom of rest.values()) {
          //       peer.worker.send('unsubscribe:' + peerfrom.id);
          //       peerfrom.worker.send('unsubscribe:' + peer.id);
          //     }
          //   }
          // }
          else cmd = '';
          setImmediate(processCmd);
        }
          
      });
    }

    cluster.on('fork', worker => worker.on('message', msg => messageHandler(msg, worker)));
    cluster.on('exit', (worker, code, signal) => {
      let peer = currentPeers.get(worker.id);
      if (peer) {
        currentPeers.delete(worker.id);
      }
      console.log(`worker ${worker.process.pid} died ${signal} peer: ${peer && peer.url}`);
    });

    function messageHandler(msg, worker) {
      let ident = msg.cache && (msg.cache + '#' + msg.worker + '.' + worker.process.pid);
      if (msg.error) {
        console.log('%s ERROR %s', colors.bgRed(ident), colors.red(msg.error));
        process.exit(1);
      } else if (msg.text) {
        if (ident) {
          process.stdout.write(colors.bgMagenta(ident) + ' ' + msg.text + '\n');
        } else {
          process.stdout.write(msg.text + '\n');
        }
      } else {
        let peer = peersmap.get(peerurltoid.get(msg.cache));
        currentPeers.set(msg.worker, peer);
      }
    }

    setImmediate(ask);

  } else {
    // WORKER
    var ident = `${process.env.URL}#${cluster.worker.id}`;
    var cache = new Cache({
      ttl: 60000,
      peers: peers,
      url: process.env.URL,
      api: process.env.API,
      bindUrl: process.env.BIND_URL,
      bindApi: process.env.BIND_API,
      storage: { meta: new Storage(path.join(process.env.STORAGE, 'meta'))
               , data: new Storage(path.join(process.env.STORAGE, 'data'),
                                   {encoding:false, decoding: false})},
      source: source
    });
    process.send({cache: process.env.URL, worker: cluster.worker.id});
    cache.on('error', err => {
      process.send({worker: cluster.worker.id, cache: cache.myUrl, error: err.stack});
      cache.close();
    });
    cache.on('statechange', (key, oldstate, newstate) => {
      process.send({cache: cache.myUrl, text: `statechange: ${key}: ${oldstate} -> ${newstate}`, worker: cluster.worker.id});
    });
    cache.on('closed', () => {
      process.send({cache: process.env.URL, text: 'closed', worker: cluster.worker.id});
      setTimeout(() => process.exit(), 50);
    });
    process.on('message', (msg) => {
      if (msg === 'shutdown') {
        cache.close();
        cache = null;
      } else if (msg === 'status') {
        let text = Array.from(cache._states.values()).map(s => `(${s.keystr} :- ${s.type})`).join(',');
        process.send({cache: cache.myUrl, worker: cluster.worker.id, text: text});
      } else if (msg.substr(0, 12) === 'unsubscribe:') {
        let url = msg.substr(12);
        for(let peer of cache.peerUrls) {
          if (peer === url) {
            process.send({cache: cache.myUrl, worker: cluster.worker.id, text: ident + ' unsubscribing: ' + url});
            cache.incoming.unsubscribe(url);
            break;
          }
        }
      } else if (msg.substr(0, 10) === 'subscribe:') {
        let url = msg.substr(10);
        for(let peer of cache.peerUrls) {
          if (peer === url) {
            process.send({cache: cache.myUrl, worker: cluster.worker.id, text: ident + ' subscribing: ' + url});
            cache.incoming.subscribe(url);
            break;
          }
        }
      }
    });
  }
});

function random(min, max) {
  return max < min ? max : (Math.random()*(max + 1 - min)|0) + min;
}

function sample(array) {
  var index = random(0, array.length - 1);
  if (index > -1)
    return array.splice(index, 1)[0];
}

function shuffle(iterable) {
  var result = [];
  for(let item of iterable) {
    if (Math.random() < 0.5) result.unshift(item); else result.push(item);
  }
  return result;
}
