'use strict';

const EventEmitter = require('events');
const Client = require('./client');
const { resolveAddress } = require('./transport');
const { isPattern } = require('./protocol');
const { shardIndex } = require('./hashring');

/**
 * A client handle that spreads work across N broker processes (N cores), exposing the
 * exact same API as {@link Client} so callers never branch. It composes N plain Clients
 * — one per shard broker — and routes each operation to the shard that owns it:
 *
 *   - cache / atomic (get/set/incr/cas/…)  → hash(key)
 *   - locks + fenced mutations             → hash(lockKey)
 *   - RPC (register/call)                  → hash(procName)
 *   - pub/sub publish + exact subscribe    → hash(channel)
 *   - pub/sub wildcard subscribe           → ALL shards (so it catches a publish on any shard)
 *
 * Because each key/name/channel hashes to exactly one broker, atomic ops and locks stay
 * correct by construction — the same way a single broker is. All reconnect, replay, and
 * delivery logic lives in the child Clients; this class is a thin router + event aggregator.
 *
 * INVARIANT: every process joining the same logical mesh MUST use the same shard count and
 * the same per-shard naming/addresses, or a given key will hash to different brokers across
 * processes (writer/reader split-brain).
 *
 * Emits: 'connect' (all shards up), 'disconnect' (all shards down), 'error' (err.shard = i),
 * and the additive per-shard events 'shard-reconnect' (i) / 'shard-disconnect' (i).
 */
class ShardedClient extends EventEmitter {
  constructor(opts = {}) {
    super();
    this.opts = opts;
    this.name = opts.name || 'default';
    this.closed = false;
    this._allUp = false;

    const specs = resolveShardSpecs(opts, this.name);
    this.n = specs.length;
    if (this.n < 1) throw new TypeError('shards must resolve to >= 1 shard');

    // resolveAddress short-circuits to PROCMESH_SOCKET for every name, which would collapse
    // all shards onto one socket. Distinct addresses are required for sharding to mean anything.
    const addrs = specs.map((s) => s.address);
    if (new Set(addrs).size !== this.n) {
      throw new Error('sharded client requires a distinct address per shard (is PROCMESH_SOCKET set?)');
    }

    this.clients = specs.map((spec, i) => {
      const child = new Client({ ...opts, shards: undefined, name: spec.name, address: spec.address });
      this._wireChild(child, i);
      return child;
    });
  }

  // ----------------------------------------------------------------- routing helpers

  _clientForKey(key) {
    return this.clients[shardIndex(key, this.n)];
  }

  _clientForChannel(channel) {
    return this.clients[shardIndex(channel, this.n)];
  }

  _clientForProc(name) {
    return this.clients[shardIndex(name, this.n)];
  }

  _fanOut(fn) {
    return Promise.all(this.clients.map(fn));
  }

  // ----------------------------------------------------------------------- events

  _wireChild(child, i) {
    child.on('error', (err) => {
      if (err && typeof err === 'object') err.shard = i;
      this.emit('error', err);
    });
    child.on('connect', () => this._onChildState());
    child.on('reconnect', () => {
      this.emit('shard-reconnect', i);
      this._onChildState();
    });
    child.on('disconnect', () => {
      this.emit('shard-disconnect', i);
      if (this.clients.every((c) => !c.connected)) this.emit('disconnect');
    });
  }

  /** Edge-trigger the mesh-level 'connect' once every shard is connected. */
  _onChildState() {
    const allUp = this.clients.every((c) => c.connected);
    if (allUp && !this._allUp) this.emit('connect');
    this._allUp = allUp;
  }

  // -------------------------------------------------------------------- connection

  async connect() {
    // Fail-fast: a half-connected mesh would silently drop a slice of the keyspace.
    await this._fanOut((c) => c.connect());
    this._allUp = true;
    return this;
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await this._fanOut((c) => c.close());
  }

  // --------------------------------------------------------------------- cache

  get(key) {
    return this._clientForKey(key).get(key);
  }

  set(key, value, opts = {}) {
    return this._clientForKey(key).set(key, value, opts);
  }

  del(key) {
    return this._clientForKey(key).del(key);
  }

  has(key) {
    return this._clientForKey(key).has(key);
  }

  async keys() {
    const lists = await this._fanOut((c) => c.keys());
    return [].concat(...lists);
  }

  async clear() {
    await this._fanOut((c) => c.clear());
  }

  /** Split keys per shard, fan out, recombine aligned to input order (undefined for misses). */
  async mget(keys) {
    const buckets = this.clients.map(() => []);
    keys.forEach((key, idx) => buckets[shardIndex(key, this.n)].push({ key, idx }));
    const out = new Array(keys.length);
    await Promise.all(
      buckets.map(async (bucket, s) => {
        if (!bucket.length) return;
        const values = await this.clients[s].mget(bucket.map((x) => x.key));
        bucket.forEach((x, j) => {
          out[x.idx] = values[j];
        });
      })
    );
    return out;
  }

  /** Group entries per shard, fan out the writes. */
  async mset(entries) {
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    const buckets = this.clients.map(() => []);
    for (const entry of list) buckets[shardIndex(entry[0], this.n)].push(entry);
    await Promise.all(buckets.map((bucket, s) => (bucket.length ? this.clients[s].mset(bucket) : null)));
  }

  // -------------------------------------------------------------------- atomic

  incr(key, by = 1) {
    return this._clientForKey(key).incr(key, by);
  }

  decr(key, by = 1) {
    return this._clientForKey(key).decr(key, by);
  }

  cas(key, prev, next) {
    return this._clientForKey(key).cas(key, prev, next);
  }

  // -------------------------------------------------------------------- pub/sub

  /**
   * Exact channels subscribe on the channel's owning shard; wildcard patterns subscribe
   * on EVERY shard (a publish can land on any of them). The returned off() mirrors the
   * subscription's footprint.
   */
  async subscribe(channel, handler) {
    if (!isPattern(channel)) return this._clientForChannel(channel).subscribe(channel, handler);
    const offs = await this._fanOut((c) => c.subscribe(channel, handler));
    return async () => {
      await Promise.all(offs.map((off) => off()));
    };
  }

  unsubscribe(channel, handler) {
    if (!isPattern(channel)) return this._clientForChannel(channel).unsubscribe(channel, handler);
    return this._fanOut((c) => c.unsubscribe(channel, handler));
  }

  /**
   * Publish to exactly ONE broker (the channel's owning shard). Exact subscribers hash to
   * that same shard and wildcard subscribers are present on every shard, so every matching
   * subscriber is reachable there — and a message can never be delivered twice. The returned
   * `delivered` count is that broker's matching-subscriber count (which is all of them).
   */
  publish(channel, payload) {
    return this._clientForChannel(channel).publish(channel, payload);
  }

  // ------------------------------------------------------------------------ rpc

  register(name, fn) {
    return this._clientForProc(name).register(name, fn);
  }

  unregister(name) {
    return this._clientForProc(name).unregister(name);
  }

  call(name, args = [], opts = {}) {
    return this._clientForProc(name).call(name, args, opts);
  }

  // ---------------------------------------------------------------------- locks

  /**
   * Locks and their fenced mutations route by the LOCK key, so a lock and every fenced write
   * guarded by it share one broker (the fencing counter is per-broker). NOTE: this means a
   * fenced write to data key `k` lands on the lock's shard, not shardIndex(k) — keep the lock
   * key and guarded data keys colocated (use the same string, or namespace data under the lock).
   */
  lock(key, opts = {}) {
    return this._clientForKey(key).lock(key, opts);
  }

  withLock(key, fn, opts = {}) {
    return this._clientForKey(key).withLock(key, fn, opts);
  }

  // ------------------------------------------------------- fenced mutations (lock-guarded)

  fencedSet(lockKey, token, key, value, opts = {}) {
    return this._clientForKey(lockKey).fencedSet(lockKey, token, key, value, opts);
  }

  fencedCas(lockKey, token, key, prev, next) {
    return this._clientForKey(lockKey).fencedCas(lockKey, token, key, prev, next);
  }

  fencedDel(lockKey, token, key) {
    return this._clientForKey(lockKey).fencedDel(lockKey, token, key);
  }

  // ---------------------------------------------------------------- misc / admin

  /** True only if every shard answered its ping. */
  async ping() {
    const replies = await this._fanOut((c) => c.ping());
    return replies.every(Boolean);
  }

  /** Aggregated snapshot across all shards, with a per-shard `shards` array for drill-down. */
  async stats() {
    const per = await this._fanOut((c) => c.stats());
    return aggregateStats(per);
  }

  /** Ask every shard broker to shut down (best-effort). */
  async shutdownBroker() {
    await this._fanOut((c) => c.shutdownBroker().catch(() => {}));
  }
}

/**
 * Resolve opts.shards to an array of { name, address } specs.
 * - number N            → name#0 .. name#(N-1), each address derived from resolveAddress.
 * - Array<string>       → each string is a NAME (resolved via resolveAddress).
 * - Array<{name|address}> → {address} used verbatim; {name} resolved.
 */
function resolveShardSpecs(opts, baseName) {
  const { shards } = opts;
  if (typeof shards === 'number') {
    const specs = [];
    for (let i = 0; i < shards; i++) {
      const name = `${baseName}#${i}`;
      specs.push({ name, address: resolveAddress(name) });
    }
    return specs;
  }
  if (Array.isArray(shards)) {
    return shards.map((s) => {
      if (typeof s === 'string') return { name: s, address: resolveAddress(s) };
      if (s && typeof s === 'object') {
        if (s.address) return { name: s.name, address: s.address };
        if (s.name) return { name: s.name, address: resolveAddress(s.name) };
      }
      throw new TypeError('each shard spec must be a name string or { name } / { address }');
    });
  }
  throw new TypeError('opts.shards must be a number or an array of names/specs');
}

/** Merge per-shard snapshots into one mesh-level snapshot (superset of Broker.snapshot()). */
function aggregateStats(per) {
  const sum = (field) => per.reduce((acc, s) => acc + (s[field] || 0), 0);
  const memField = (field) => per.reduce((acc, s) => acc + (s.memory ? s.memory[field] || 0 : 0), 0);
  const ops = {};
  for (const s of per) {
    for (const [k, v] of Object.entries(s.ops || {})) ops[k] = (ops[k] || 0) + v;
  }
  return {
    shardCount: per.length,
    shards: per,
    uptimeMs: per.reduce((m, s) => Math.max(m, s.uptimeMs || 0), 0),
    connections: sum('connections'),
    cacheSize: sum('cacheSize'),
    ops,
    dropped: sum('dropped'),
    reaped: sum('reaped'),
    locks: sum('locks'),
    lockWaiters: sum('lockWaiters'),
    pendingCalls: sum('pendingCalls'),
    // Over-counts wildcard subscriptions (present on every shard) — documented.
    subscriptions: sum('subscriptions'),
    procs: [].concat(...per.map((s) => s.procs || [])),
    // SUM across brokers — can exceed 1.0, which is the whole point of sharding.
    cpuCoreFraction: per.reduce((acc, s) => acc + (s.cpuCoreFraction || 0), 0),
    memory: {
      rss: memField('rss'),
      heapTotal: memField('heapTotal'),
      heapUsed: memField('heapUsed'),
      external: memField('external'),
    },
  };
}

module.exports = ShardedClient;
