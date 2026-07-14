# procmesh

Shared in-memory cache, pub/sub, RPC, and locks across **independent Node.js
processes on the same machine** — no Redis, no external server, no native addons.

A tiny **broker process** holds the authoritative in-memory state and routes all
messaging over a local socket (Unix domain socket on Linux/macOS, named pipe on
Windows). The first client to connect auto-spawns the broker, so there's nothing to
run or manage. Every operation serializes through the one broker process, so the
cache, atomic ops, and locks are strongly consistent and correct by construction.

**What you get:**

- 🗃️ **Cache** — shared key/value store with TTL and LRU eviction
- ➕ **Atomic ops** — `incr` / `decr` / `cas`, TTL-preserving, race-free
- 📣 **Pub/Sub** — channels with prefix wildcards and slow-consumer protection
- 🔁 **RPC** — request/response across processes that doubles as a load-balanced worker pool
- 🔒 **Locks** — TTL'd mutexes with fencing tokens, auto-released on crash
- 📦 **Zero infra** — no external server; only `lru-cache` required, no native deps

```bash
npm install procmesh-js
```

## Contents

- [Quick start](#quick-start)
- [Core API](#core-api)
  - [Cache](#cache) · [Atomic ops](#atomic-ops) · [Pub/Sub](#pubsub) · [RPC](#rpc-requestresponse-across-processes) · [Locks](#locks)
- [Advanced](#advanced)
  - [Fencing tokens](#fencing-tokens-safe-under-ttl-overrun) · [Sharding](#sharding-scale-past-one-core) · [Persistence & crash survival](#persistence--crash-survival-ha)
- [Operations](#operations)
  - [Running the broker explicitly](#running-the-broker-explicitly) · [Observability](#observability) · [Events](#events)
- [Configuration reference](#configuration-reference)
- [How it works](#how-it-works)
- [Limitations](#limitations-v1)
- [License](#license)

## Quick start

```js
const { createClient } = require('procmesh-js');

// In process A
const mesh = await createClient();           // auto-spawns a broker if needed
await mesh.set('user:1', { name: 'Ada' }, { ttl: 60_000 });

// In process B (a totally separate `node` invocation)
const mesh = await createClient();
console.log(await mesh.get('user:1'));        // -> { name: 'Ada' }
```

All processes that use the same `name` (default `'default'`) share one broker.

## Core API

### Cache
```js
await mesh.set(key, value, { ttl })   // ttl in ms (optional)
await mesh.get(key)                    // -> value | undefined
await mesh.has(key)                    // -> boolean
await mesh.del(key)                    // -> boolean
await mesh.keys()                      // -> string[]
await mesh.clear()
await mesh.mget(['a', 'b'])            // -> [valA, valB]
await mesh.mset({ a: 1, b: 2 })        // or [['a',1], ['b',2]]
```

### Atomic ops
```js
await mesh.incr('count')               // -> new value (starts at 0)
await mesh.decr('count', 2)
await mesh.cas('cfg', expected, next)  // compare-and-set -> boolean
```
`incr`/`decr`/`cas` are read-modify-write and **preserve any per-item TTL** on the key — a counter
created with an expiry keeps counting down rather than becoming immortal on the next update.

### Pub/Sub
```js
const off = await mesh.subscribe('orders', (payload, channel) => { ... });
await mesh.publish('orders', { id: 7 });   // -> number of subscribers reached
await off();                                // unsubscribe

// Wildcard (prefix) subscriptions — a trailing * matches by prefix:
await mesh.subscribe('orders.*', (p, channel) => { /* orders.created, orders.shipped, ... */ });
```
A slow subscriber can't make the broker run out of memory: by default pub/sub frames to a
backed-up consumer are dropped past the high-water mark (see `sendHighWaterMark`),
while replies/RPC stay reliable.

#### Producer `acks` — choose your delivery guarantee

procmesh is a **single broker per channel** (sharding partitions, it doesn't replicate), so the
Kafka `acks` ladder maps to what one broker can promise:

| `acks`   | Waits for                                                                     | Reliability | Speed   |
| -------- | ---------------------------------------------------------------------------- | ----------- | ------- |
| `0`      | nothing — writes the frame and returns immediately (`undefined`)             | Low         | Fastest |
| `1` *(default)* | the broker to receive + fan out best-effort (slow subs may be **dropped**) | Medium | Fast    |
| `'all'`  | **reliable, no-drop** fan-out — slow subs are backpressured, `delivered === subscriberCount` | Highest | Slowest |

```js
await mesh.publish('orders', msg);                 // acks:1 (default)
await mesh.publish('metrics', msg, { acks: 0 });   // fire-and-forget, fastest
await mesh.publish('orders', msg, { acks: 'all' }); // no message dropped to a slow consumer
```

`publish` resolves with the **delivered count** — the number of subscribers the message was
*queued to* (written to the socket send buffer; not dropped/overflowed). That is a fan-out count,
**not** an end-to-end consumer acknowledgement: `acks:'all'` guarantees the frame was enqueued to
every live subscriber (slow ones are disconnected, not silently dropped), not that each consumer
processed it. A deduped retry resolves `null` (distinct from `0` = delivered to zero subscribers);
`acks:0` resolves `undefined`.

#### Idempotent producer — safe retries, no duplicates

Turn on `idempotent` and each publish carries a producer id + per-channel sequence. The broker
dedupes retries, so a publish that times out is retried automatically (for `acks>=1`) with the
*same* sequence and is delivered **at most once**:

```js
await mesh.publish('orders', msg, { acks: 'all', idempotent: true });

// Or bind defaults with a Kafka-style producer handle:
const producer = mesh.producer({ acks: 'all', idempotent: true });
await producer.publish('orders', msg);
```

Dedup is a **sliding window** (`dedup.window`, default 1024): a retried or out-of-order sequence is
dropped only if that exact seq was already accepted, so a genuine gap — a lower seq that never
landed while a higher one did — is still delivered rather than mistaken for a duplicate. Retries on a
dropped link **wait for reconnection** (bounded by the publish `timeout`) instead of burning the
retry budget while offline.

The broker-minted producer id survives client reconnects, so dedup holds across a dropped link. To
keep dedup working across a full **process restart**, pass a stable `pubsub.producerId` and seed the
per-channel sequence you last used (persist it yourself), so the restarted producer resumes above the
broker's high-water:

```js
const mesh = await createClient({
  pubsub: { idempotent: true, producerId: 'ingest-1', sequences: { orders: lastSeqYouPersisted } },
});
// mesh.sequence('orders') / mesh.setSequence('orders', n) read/seed the counter to persist.
```

(Dedup state is in-memory on the broker and bounded — see the `dedup` broker options — so
cross-restart dedup holds only while the broker stays up and within the window/TTL.)

#### Persistence & replay (opt-in)

By default pub/sub is in-memory and at-most-once. Turn on **`pubsub.persist`** broker-side to append
published messages to the log and keep a bounded per-channel retention ring. A subscriber can then
**replay** the recent backlog — and with a persist dir, messages survive a broker restart:

```js
// broker (auto-spawn via createClient, explicit createBroker, or `procmesh serve --pubsub-persist`)
createBroker({ persist: { dir: '/var/lib/procmesh' }, pubsub: { persist: true, retention: 1000 } });

// consumer opts in to replay
await mesh.subscribe('orders', handler, { replay: true });  // all retained; or { replay: 100 } for last N
```
With `acks:'all'` and persistence on, the broker fsyncs the message **before** acking (durable ack).

Each retained message gets a monotonic **offset**, and each subscriber tracks the highest offset it
has seen. On reconnect the client resumes from that offset, so messages published while it was
disconnected are **caught up** (delivered exactly once) rather than lost — this makes a reconnecting
subscriber effectively at-least-once for anything still in the retention window. Retention is bounded
(`pubsub.retention` per channel, `pubsub.maxChannels` total channels), so a message evicted before a
slow consumer resumes is a gap in that best-effort window — this is a bounded replay buffer, not an
infinite Kafka-style log.

### RPC (request/response across processes)
```js
// worker process
await mesh.register('resize', async (w, h) => doResize(w, h));

// caller process
const result = await mesh.call('resize', [800, 600], { timeout: 5000 });
```
**Worker pools:** if several processes `register` the *same* name, the broker
load-balances calls across them (least-busy first), so RPC doubles as a work queue.
A call in flight when its worker dies is failed with `EHANDLERGONE`; the pool keeps
serving from the remaining workers. A worker that *hangs* while staying connected can't leak broker
state either: the broker reaps the pending call shortly after the caller's `timeout` elapses, frees
the worker's in-flight slot (so dispatch isn't skewed), and — for a caller without its own timeout —
fails it with `ECALLTIMEOUT`.

### Locks
```js
// manual
const release = await mesh.lock('job:42', { wait: 10_000, ttl: 30_000 });
if (release) { try { /* critical section */ } finally { await release(); } }

// scoped — acquires, runs, always releases
await mesh.withLock('job:42', async () => { /* critical section */ });
```
`wait` is how long to block for the lock (0 = fail immediately, returning `null`).
`ttl` auto-releases the lock if the holder crashes, preventing deadlocks. A holder's
locks are also released automatically when it disconnects.

## Advanced

### Fencing tokens (safe under TTL overrun)

If a critical section runs longer than the lock's `ttl`, the lock auto-releases and another
client can acquire it — the classic "two holders" hazard. Each grant therefore carries a
monotonically increasing **fencing token**, and procmesh-mediated writes can be *fenced*: a
write from a holder whose lock already expired is rejected with `EFENCED`.

```js
// withLock hands you a fenced context — set/cas/del here are guarded by this grant's token:
await mesh.withLock('account:42', async ({ token, set, cas }) => {
  await set('account:42:balance', 100);   // rejected with EFENCED if our lock has expired
});

// manual: the release closure carries the token
const release = await mesh.lock('account:42', { ttl: 30_000 });
await mesh.fencedSet('account:42', release.token, 'account:42:balance', 100);
await release();
```

**Limitation:** procmesh can only *enforce* fencing for state kept in its own store. If your
critical section writes to an external resource (DB row, file, API), that resource must check
`token` itself — procmesh gives you the token but cannot police a foreign system.

### Sharding (scale past one core)

A single broker serializes everything on one event loop — correct by construction, but a
one-core ceiling (~100k small ops/sec). To go faster, **shard** across N brokers (N cores)
with one option. The returned handle has the *identical* API, so nothing else in your code changes:

```js
// dev: auto-spawn 4 shard brokers (named default#0 .. default#3)
const mesh = await createClient({ shards: 4 });

// prod: point at explicit, supervised brokers
const mesh = await createClient({
  shards: ['/run/procmesh/0.sock', '/run/procmesh/1.sock', '/run/procmesh/2.sock'],
});

await mesh.set('user:1', { name: 'Ada' });   // same API, transparently routed
```

**How work is routed.** Every key/name/channel hashes (FNV-1a, mod N) to exactly one broker:

| Primitive                         | Routes by      | Scales across cores?                          |
|-----------------------------------|----------------|-----------------------------------------------|
| cache / atomic (`get`/`incr`/…)   | the **key**    | yes — keyspace spread evenly                   |
| locks + fenced writes             | the **lock key** | yes                                          |
| RPC (`register`/`call`)           | the **proc name** | yes, across *distinct* names (one busy name still pins to one broker) |
| pub/sub publish + exact subscribe | the **channel**  | yes                                          |
| pub/sub **wildcard** subscribe    | **all shards** | the pattern occupies a slot on every broker    |

Because each key lands on exactly one broker, atomic ops and locks stay correct by construction —
exactly as with a single broker. A publish still hits exactly one broker (no duplicate delivery);
wildcard subscriptions register everywhere so they catch a publish on any shard.

**Invariant — agree on N.** Every process joining the same mesh **must** use the same shard count
and the same per-shard naming/addresses, or a given key hashes to different brokers across processes
(writer/reader split-brain). Pin `shards` in shared config. (Numeric `shards` is incompatible with
`PROCMESH_SOCKET`, which would collapse every shard onto one socket — that throws.)

**Lock/data colocation gotcha.** Inside `withLock(L, fn)`, `ctx.set(k, …)` writes to **`L`'s** shard
(fencing is per-broker), *not* `k`'s shard. So a fenced write may not be visible via `mesh.get(k)`
when `k` and `L` hash differently. Keep them colocated: use the same string for the lock and the
guarded key, or namespace guarded keys under the lock key.

**Events & stats.** The sharded handle emits `connect` once **all** shards are up and `disconnect`
once **all** are down, plus per-shard `shard-reconnect(i)` / `shard-disconnect(i)` (a forwarded
`error` carries `err.shard`). `mesh.stats()` returns summed counters plus a `shards: [...]` array for
per-shard drill-down; `cpuCoreFraction` is the **sum** across brokers (so > 1.0 is expected and good),
and `subscriptions` over-counts wildcards (present on every shard).

### Persistence & crash survival (HA)

By default the broker is purely in-memory (zero-config). For production you can enable
**snapshot + append-only-log** persistence so cache and atomic counters survive a broker crash
or restart:

```bash
npx procmesh serve --persist-dir /var/lib/procmesh   # persistence on (fsync everysec)
# or: PROCMESH_PERSIST_DIR=/var/lib/procmesh npx procmesh serve
npx procmesh serve --no-persist                       # explicitly in-memory only
```
```js
createBroker({ persist: { dir: '/var/lib/procmesh', mode: 'everysec' } });
// mode: 'no' (OS-buffered) | 'everysec' (default, ≤1s loss on power failure) | 'always' (sync, durable)
```

What survives a restart and what doesn't — **by design**:
- **Survives:** cache values, atomic counters (with correct remaining TTL), and the fencing
  counter (so a stale pre-crash token is still rejected after restart).
- **Released:** **all locks.** A lock's owner is a live connection that dies with the broker;
  treat a broker restart exactly as you treat a lock TTL expiry — clients re-acquire on reconnect.
- **Rebuilt automatically:** subscriptions and RPC registrations (clients replay them on reconnect).

Run a **dedicated, supervised** broker (not auto-spawn) so it outlives your workers, with an
**explicit** persist dir (not the OS temp dir, which may clear on reboot):

```ini
# systemd: /etc/systemd/system/procmesh.service
[Service]
ExecStart=/usr/bin/node /path/to/node_modules/procmesh-js/src/broker-bin.js
Environment=PROCMESH_BROKER_OPTS={"name":"default","idleTimeout":0,"persist":{"dir":"/var/lib/procmesh"}}
Restart=always
RestartSec=1
```
(PM2: `pm2 start node_modules/procmesh-js/src/broker-bin.js`; Windows: wrap it with NSSM.)

## Operations

### Running the broker explicitly

Auto-spawn is convenient, but you can also run a long-lived broker (e.g. under
systemd or PM2) so it never idles out:

```bash
npx procmesh serve            # foreground broker
npx procmesh status           # is a broker up?
npx procmesh stop             # ask it to shut down
# all accept [--name <name>] [--socket <addr>] [--token <secret>]
```

### Observability

```js
const s = await mesh.stats();   // { uptimeMs, connections, cacheSize, ops, dropped, duplicates,
                                //   dedupSize, reaped, locks, lockWaiters, pendingCalls,
                                //   subscriptions, procs, memory, cpuCoreFraction }
```
Or from the shell: `npx procmesh stats [--json]`. Counters are bumped on the hot path with a
single integer increment per op, so this is cheap to poll for a Prometheus exporter, etc.

### Events

- **Client:** `connect`, `disconnect`, `reconnect`, `error`.
- **Broker:** `connect`/`disconnect` (connId), `drop` (`{ channel, connId }`),
  `reap` (connId reaped), `stats` (snapshot, if `statsInterval` set), `persist-error` (err).

## Configuration reference

```js
await createClient({
  name: 'default',        // logical mesh; maps to a socket. Use distinct names to isolate meshes.
  codec: 'json',          // 'json' (default) | 'msgpack' (needs optional `msgpackr`) | custom { encode, decode }
                          //   NOTE: a custom { encode, decode } codec can't be forwarded to an
                          //   auto-spawned broker (functions can't cross `spawn`). Run the broker
                          //   yourself with the same codec and pass autoSpawn:false (else it throws).
  autoSpawn: true,        // spawn a broker if none is running
  reconnect: true,        // auto-reconnect + replay subscriptions/registrations
  callTimeout: 30_000,    // default per-request timeout (ms)
  pingInterval: 0,        // ms; client-side keepalive. >0 = ping on this interval and reconnect if a
                          //   PONG doesn't return in time (detects a dead broker on a half-open
                          //   socket / when the broker heartbeat is off). 0 = rely on broker heartbeat.
  token: undefined,       // shared secret; if the broker requires one, must match (else EAUTH)
  cache: { max: 10_000, ttl: 0, maxSize: 0 },     // broker cache bounds (used when this client spawns the broker)
  shards: undefined,      // scale past one core: a count N, or an array of broker addresses/names (see Sharding)
  pubsub: {               // producer defaults for publish() (per-call opts override); also carries the
                          //   broker persist knobs below, forwarded to an auto-spawned broker
    acks: 1,              //   default acks level: 0 | 1 | 'all'
    idempotent: false,    //   attach pid+seq so retries dedupe
    producerId: undefined,//   stable producer id (survives a process restart; else broker-minted)
    sequences: undefined, //   { channel: lastSeq } to seed per-channel seqs across a restart
    retries: 3,           //   publish auto-retries (idempotent + acks>=1 only)
    retryBackoff: 50,     //   base ms, exponential with jitter
    retryMaxDelay: 2000,  //   ceiling per retry (ms)
  },
});
```
Per-call overrides win over the client defaults: `publish(ch, msg, { acks: 'all', idempotent: true, timeout, retries })`.

Broker-side options (passed to `createBroker`, `procmesh serve`, or forwarded by an
auto-spawning client):
```js
createBroker({
  token: undefined,            // require this shared secret on HELLO (omit = allow all, zero-config default)
  callTimeout: 30_000,         // ms; backstop deadline to reap an RPC call whose worker hangs while
                               //   connected (frees pending state + worker inflight). Used when a
                               //   caller doesn't send its own per-call timeout; otherwise caller's + grace.
  heartbeatInterval: 30_000,   // ms; broker pings idle conns and reaps unresponsive ones (3× interval)
  sendHighWaterMark: 16<<20,   // bytes; pub/sub frames dropped for a consumer buffered past this
  sendHardLimit: 64<<20,       // bytes; a consumer buffered past this is disconnected (slow-consumer protection)
  idleTimeout: 0,              // ms; auto-shutdown after the last client leaves (0 = never)
  statsInterval: 0,            // ms; if set, emit a 'stats' snapshot on this interval (0 = off)
  cache: { max: 10_000, ttl: 0, maxSize: 0 },
  persist: undefined,          // crash-survival persistence — see below (off by default)
  dedup: {                     // idempotent-producer dedup store (bounds retry-dedup memory)
    enabled: true,             //   false = ignore pid/seq entirely (pure at-most-once, zero memory)
    max: 100_000,              //   max (producerId, channel) entries
    ttl: 600_000,              //   ms idle before a producer's dedup state ages out
    window: 1024,              //   per-entry sliding window: how far below the highwater a retried/
                               //   out-of-order seq can still be recognized (gap-fill vs duplicate)
  },
  pubsub: {                    // opt-in pub/sub message persistence (orthogonal to acks)
    persist: false,            //   append published messages to the AOF + keep a retention ring
    retention: 1000,           //   retained messages per channel for replay (count)
    retentionMs: 0,            //   OR age-based retention in ms (0 = count-only)
    maxChannels: 10_000,       //   cap on the number of retained channels (LRU-evicted; DoS guard)
    durableAcks: 'all',        //   which acks level fsyncs BEFORE acking: 'all' | 1 | false
  },
});
```
CLI equivalents on `procmesh serve`: `--dedup-max`, `--dedup-ttl`, `--no-dedup`, `--pubsub-persist`,
`--pubsub-retention`.

## How it works

```
node app-a   node app-b   node worker
        \         |         /
         \        |        /
        [ procmesh broker process ]
        Unix socket / Windows named pipe
   cache · pub/sub · rpc router · lock manager
```

- **Transport:** Node's built-in `net` only — zero native dependencies.
- **Framing:** 4-byte length-prefixed frames; JSON payloads by default.
- **Eviction/TTL:** backed by [`lru-cache`](https://www.npmjs.com/package/lru-cache).
- **Robustness:** per-connection high-water-mark backpressure (slow consumers can't OOM the
  broker), heartbeats that reap dead connections, worker-pool RPC with least-busy dispatch,
  and optional shared-secret auth.

## Limitations (v1)

- **Same machine only.** No cross-host networking (use Redis/NATS for that).
- **Single broker = single core.** All ops serialize through one event loop; that's what makes
  cache/atomic/locks correct by construction, but it caps throughput at one core (≈100k small
  ops/sec). To scale past it, **shard** across N brokers with `createClient({ shards: N })` — see
  [Sharding](#sharding-scale-past-one-core).
- **Persistence is opt-in** (see above). Without it, state is gone when the broker exits. Even
  with it, locks are released on restart (they're connection-scoped). Pub/sub is at-most-once by
  default (messages published during a disconnect aren't buffered); for stronger guarantees use
  `acks:'all'` (no-drop fan-out), `idempotent` (dedup'd retries), and `pubsub.persist` (durable
  publish + bounded replay-on-subscribe) — see [Pub/Sub](#pubsub).
- Every cache op is a local IPC round-trip (~0.05–0.2 ms). Fine for coordination;
  not a substitute for a per-process hot cache in ultra-hot paths.

## License

MIT
