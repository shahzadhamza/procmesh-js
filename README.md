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
npm install procmesh
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
const { createClient } = require('procmesh');

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
A slow subscriber can't make the broker run out of memory: pub/sub frames to a
backed-up consumer are dropped past the high-water mark (see `sendHighWaterMark`),
while replies/RPC stay reliable.

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
ExecStart=/usr/bin/node /path/to/node_modules/procmesh/src/broker-bin.js
Environment=PROCMESH_BROKER_OPTS={"name":"default","idleTimeout":0,"persist":{"dir":"/var/lib/procmesh"}}
Restart=always
RestartSec=1
```
(PM2: `pm2 start node_modules/procmesh/src/broker-bin.js`; Windows: wrap it with NSSM.)

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
const s = await mesh.stats();   // { uptimeMs, connections, cacheSize, ops, dropped, reaped,
                                //   locks, lockWaiters, pendingCalls, subscriptions, procs,
                                //   memory, cpuCoreFraction }
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
});
```

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
});
```

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
  with it, locks are released on restart (they're connection-scoped), and pub/sub is at-most-once
  (messages published during a disconnect are not buffered).
- Every cache op is a local IPC round-trip (~0.05–0.2 ms). Fine for coordination;
  not a substitute for a per-process hot cache in ultra-hot paths.

## License

MIT
