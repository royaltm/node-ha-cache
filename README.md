HA Cache Cluster
================

Assumptions
-----------

- real HA: servers (peers) work in a cluster, peers may go down, maight be partitioned, as long as majority of the peers is seeing each other, the cluster should provide uninterrupted service to the clients
- clients can query any server, on occasion might ask another server if rebuted or didn't get answer within predefined time
- values of cache entries are determined only by key and time
- we want to avoid parallel retrieval of the same entry from the source
- cache expiration time is much longer than average source retrieval time
- source retrieval time is significant (many seconds)
- stale but quick responses to clients are better than the slow ones
- number of cache entries are handfull (<~ 1000)

Solution
--------

- implementation of networking based on 0MQ
- servers retrieve entries from source on demand
- entries are lazily distributed
- every new/fresh entry retrieval will be negotiated (elected)
- peers respond with a local copy of an entry or retrieve a copy from peers or/and trigger source retrieval election
- eventually every peer will have copy of all cache entries at some point in time
- cache expire time is set up front (before retrieval)

Implementation
--------------

### Cache

Each cache entry consists of:

- key
- data
- expire {number}
- term {number}

### Messages

Peer-peer messages:

Every message consists of the following frames:

- target: "*" or target url
- sender: sender's url
- type: message type + authorization

and additional tail frames, depending on `type` of the message.

State types:

- QUESTION: tail frames: `key` and `term`
- ANSWER: tail frames: `key`, `term`, `expiration` and `vote`
- ENTRYREQ: expects UPDATE, attributes: `key`
- UPDATE: attributes: `key`, `term`, `expiration`, `value`
- ANNOUNCE: attributes: `key`, `term`, `expiration`
- PING: expects PONG
- PONG

data types:

- `key`: binary
- `term`: variable unsigned integer LSByte first
- `expiration`: variable unsigned integer LSByte first
- `vote`: empty frame: false, on non-zero byte: true
- `value`: binary

### Finite state machine

- state is key scoped
- state is volatile except term

List of possible states for each key:

- "idle": fresh key in storage or default for non-existent entries
- "follower": has voted for other candidate or follows current sourcing peer
  attributes: `term`, `entry`, `peer`, `isSourcing`
- "candidate": peer candidates for sourcing an entry
  attributes: `term`, `entry`, `votes`, `aborted`
- "sourcing": peer is currently retrieving entry from source
  attributes: `term`, `entry`


#### 1. Timeouts

1.1 state "candidate"

- when number of ANSWERs received is less than majority, enter global partitioned state, drop to "idle" state for a key
- majority answers received but disagreed, increase term and go ask another QUESTION

1.2 state "follower"

- drop back to "idle"

1.3 send keep-alive to clients in CLIENT_REQ_REFRESH_INTERVAL intervals

- if state is "idle" send SEEOTHER to clients and drop their requests
- otherwise send keep-alive to clients awaiting responses

#### 2. on CLIENT REQUEST

2.1 If in partitioned state, reply to new clients with SEEOTHER and finish processing.

2.2 check local copy of entry at key
- send immediate response if value for a requested key exists and do not save request
- otherwise save request for later and send first keepalive message to client
- if entry is fresh finish processing.

2.3 state "idle"

- set state to "candidate" (random timeout 150 - 300 ms) RPC timeout (100 ms)
- send QUESTION to EVERYONE (repeat question after RPC timeout to peers that we didn't receive ANSWER from)

2.4 state "follower" or "sourcing"

- do nothing

#### 3. on QUESTION

3.1 state "idle"

- vote yes if our entry's expiration is not fresh and term in QUESTION >= our entry's term; set follower state with QUESTION sender as peer candidate (300 ms timeout)
- else vote no
- update term of our entry if needed
- always respond with an ANSWER

3.2 state "follower" (we know already we don't have a fresh entry)

- vote no if we follow sourcing peer
- vote yes if the term in QUESTION == our entry's term and our candidate is the same as in the QUESTION
- vote yes if the term in QUESTION > our entry's term and change candidate to the QUESTION sender
- refresh state timeout if voted yes (300 ms)
- update term if needed
- always respond with an ANSWER

3.3 state "candidate"

- if the term in QUESTION <= our entry's term vote no
- if the term in QUESTION > our entry's term vote yes; convert to "follower" (300 ms timeout) set candidate to the QUESTION sender
- update term if needed
- always respond with ANSWER

3.4 state "sourcing"

- respond with ANNOUNCEMENT

#### 4. on UPDATE

- if UPDATE expiration > current entry's expiration, save value and expiration from UPDATE
- if expiration > now (in the future) and state is not "sourcing", drop to idle
- ipdate entry's term if needed
- respond to clients.

#### 5. on ANNOUNCEMENT

5.1 if ANNOUNCEMENT's entry expiration > our entry's send ENTRYREQ

5.2 depending on current state

- on state "sourcing", finish processing.
- on state "follower" with the same peer as sourcing, refresh timeout (2000 ms)
- otherwise if ANNOUNCEMENT's term >= our entry's, or state is "idle", set state to "follower", set peer, set isSourcing to true, timeout (2000 ms)
- update term if needed


#### 6. on ENTRYREQ

- if our entry has value send UPDATE with value, term and expiration to sender of ENTRYREQ

#### 7. on ANSWER

7.1 state "candidate"

- if ANSWER's expiration > now send ENTRYREQ to the ANSWER's sender, update term if needed, abort current voting and set new timeout (150 - 300 ms)
- if ANSWER's term < ours, do not check vote (but count vote toward network partitioning check), finish processing
- otherwise if ANSWER's term > ours, update our term, abort current voting and let it timeout to go with another election, finish processing
- otherwise check vote if majority agreed convert to "sourcing", start sourcing, start sending ANNOUNCEMENTs (on 1000ms interval)

7.2 state "follower" or "idle" or "sourcing" finish processing.

#### 8. on PING

- reply with PONG

#### 9. on PONG

- if we are in partitioned state count toward majority
- if majority PONGed step down from partitioned state

#### 10. sourcing

- on success update expiration, save entry, send UPDATE to EVERYONE
- regardless of success or failure drop to idle state

### Client

Clients query servers in round-robin fashion using DEALER socket.

- after sending request to the next server, client waits intially 500ms for any response from it
- when server responds with a keep-alive message, client waits additional 2000ms for a next response from the server
- when server responds with a see-other message or when no response was received withtin the time interval client drops current request and repeats this procedure from the beginning requesting from the next server.

### Example

#### Server

```
var peers = [
  {url: "tcp://peer1.cluster.com:6000", api: "tcp://peer1.cluster.com:6001"},
  {url: "tcp://peer2.cluster.com:6000", api: "tcp://peer2.cluster.com:6001"},
  {url: "tcp://peer3.cluster.com:6000", api: "tcp://peer3.cluster.com:6001"},
]

var retrieveEntry = (key) => new Promise((resolve, reject) => {
  // ... somehow fetch entry here
  resolve(value);
});

var server = new Cache({
  url: peers[CURRENT_PEER_INDEX].url,
  api: peers[CURRENT_PEER_INDEX].api,
  peers: peers,
  storage: {
    meta: new LevelDownStorage("/path/to/persistent/storage/meta"),
    data: new LevelDownStorage("/path/to/persistent/storage/data", {encode: false, decode: false})
  },
  source: retrieveEntry,
  bindUrl: "tcp://*:6000", // optional
  bindApi: "tcp://*:6001"  // optional
});
```

#### Client

```
var apiUrls = [
  "tcp://peer1.cluster.com:6001",
  "tcp://peer2.cluster.com:6001",
  "tcp://peer3.cluster.com:6001"
];
var client = new CacheClient(apiUrls, {
  timeout: 60000, // wait at least 1 minute before giving up
  discoveryinterval: 10000 // discover new servers once in 10 seconds
});

client.get("key").then(entry => {
  console.log("value size: %d expiration: %d", entry.value.length, entry.expire);
})
.catch(err => console.log(err));
```

### TODO

- some basic security
