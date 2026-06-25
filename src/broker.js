'use strict';

const net = require('net');
const fs = require('fs');
const EventEmitter = require('events');
const { Peer, TYPES, PROTOCOL_VERSION, matchTopic, isPattern } = require('./protocol');
const { resolveCodec } = require('./codec');
const { resolveAddress, isPipe } = require('./transport');
const Store = require('./store');
const LockManager = require('./locks');
const { createPersistence } = require('./persistence');

/** Every valid inbound message tag, used to bound the per-type ops counter. */
const KNOWN_TYPES = new Set(Object.values(TYPES));

/** Extra time past a caller's deadline before the broker reaps a hung RPC call (cleanup backstop). */
const CALL_TIMEOUT_GRACE = 5000;

/**
 * The central broker. Holds the authoritative cache, routes pub/sub and RPC,
 * and manages locks. Because everything runs in this one process on a single
 * event loop, cache/atomic/lock semantics are correct without any coordination.
 *
 * Emits: 'drop' ({ channel, connId }) when a pub/sub frame is dropped for a slow
 * consumer (backpressure), and 'reap' (connId) when an idle connection is reaped.
 */
class Broker extends EventEmitter {
  constructor(opts = {}) {
    super();
    this.opts = opts;
    this.name = opts.name || 'default';
    this.address = opts.address || resolveAddress(this.name);
    this.codec = resolveCodec(opts.codec);
    this.store = new Store(opts.cache || {});
    // Global monotonic fencing-token counter. Seeded from persisted state (Phase 3) so tokens
    // stay strictly increasing across broker restarts — a stale pre-crash token can never pass.
    this.nextToken = opts.fenceSeed || 0;
    this.locks = new LockManager({ mintToken: () => this._mintToken() });
    this.token = opts.token || null;
    // Fallback deadline for an RPC call whose caller didn't send its own timeout (older clients).
    this.callTimeout = opts.callTimeout || 30000;
    // Grace past a caller's deadline before the broker reaps a hung call (cleanup backstop).
    this.callTimeoutGrace = opts.callTimeoutGrace == null ? CALL_TIMEOUT_GRACE : opts.callTimeoutGrace;

    // Crash-survival persistence (no-op unless opts.persist / PROCMESH_PERSIST_DIR is set).
    this.persist = createPersistence(opts.persist, this.name, this.codec);
    this.persist.onError = (err) => this.emit('persist-error', err);

    this.peerOpts = {
      sendHighWaterMark: opts.sendHighWaterMark,
      sendHardLimit: opts.sendHardLimit,
    };

    this.conns = new Map(); // connId -> conn
    this.channels = new Map(); // exact channel -> Set<connId>
    this.patterns = new Map(); // wildcard pattern -> Set<connId>
    this.procs = new Map(); // procName -> { workers: Set<connId>, inflight: Map<connId, n> }
    this.pending = new Map(); // brokerCallId -> { callerConnId, callerCallId, ownerConnId, name }

    this.nextConnId = 1;
    this.nextCallId = 1;
    this.dropped = 0; // count of pub/sub frames dropped due to backpressure

    // Observability: cheap monotonic counters bumped on the hot path (one integer
    // increment per message), surfaced via the STATS request and optional 'stats' emit.
    this.startedAt = nowMs();
    this.stats = { ops: {}, reaped: 0 };
    this._cpuBase = process.cpuUsage();
    this._cpuBaseAt = nowMs();
    this.statsInterval = opts.statsInterval || 0; // ms; 0 = no periodic emit
    this._statsTimer = null;

    this.idleTimeout = opts.idleTimeout || 0; // ms; 0 = never auto-shutdown
    this._idleTimer = null;
    this.heartbeatInterval = opts.heartbeatInterval == null ? 30000 : opts.heartbeatInterval;
    this._heartbeatTimer = null;
    this.server = null;
  }

  async start() {
    // Recover persisted cache/atomics BEFORE listening, so no request is ever served against
    // half-loaded state. Seed the fencing counter past every token issued before the crash.
    await this.persist.load(this.store);
    this.nextToken = Math.max(this.nextToken, this.persist.loadedToken || 0);
    await this._listen();
    this._startHeartbeat();
    this._startStats();
    this.persist.start();
    return this;
  }

  async _listen() {
    this.server = net.createServer((socket) => this._onConnection(socket));
    try {
      await this._tryListen();
    } catch (err) {
      if (err.code !== 'EADDRINUSE') throw err;
      // Address in use: is a live broker already there, or is this a stale socket?
      const alive = await this._probe();
      if (alive) throw err; // genuine — caller should just connect instead
      if (!isPipe(this.address)) {
        try {
          fs.unlinkSync(this.address);
        } catch {
          /* ignore */
        }
      }
      this.server = net.createServer((socket) => this._onConnection(socket));
      await this._tryListen();
    }
  }

  _tryListen() {
    return new Promise((resolve, reject) => {
      const onError = (err) => reject(err);
      this.server.once('error', onError);
      this.server.listen(this.address, () => {
        this.server.removeListener('error', onError);
        resolve();
      });
    });
  }

  _probe() {
    return new Promise((resolve) => {
      const socket = net.connect(this.address);
      const done = (alive) => {
        socket.destroy();
        resolve(alive);
      };
      socket.once('connect', () => done(true));
      socket.once('error', () => done(false));
      socket.setTimeout(500, () => done(false));
    });
  }

  _onConnection(socket) {
    socket.setNoDelay(true);
    const conn = {
      id: this.nextConnId++,
      peer: new Peer(socket, this.codec, this.peerOpts),
      name: null,
      authed: !this.token, // if no token configured, every conn is implicitly authed
      lastSeen: nowMs(),
      subs: new Set(),
      procs: new Set(),
    };
    this.conns.set(conn.id, conn);
    this.emit('connect', conn.id);
    this._cancelIdle();
    conn.peer.on('message', (msg) => {
      conn.lastSeen = nowMs();
      this._handle(conn, msg);
    });
    conn.peer.on('close', () => this._onClose(conn));
    conn.peer.on('error', () => {
      /* 'close' handles cleanup */
    });
  }

  _ok(conn, id, value) {
    if (id != null) conn.peer.send({ t: TYPES.OK, id, value });
  }

  _err(conn, id, message, code) {
    if (id != null) conn.peer.send({ t: TYPES.ERR, id, message, code });
  }

  // Persistence loggers. Each early-returns when persistence is disabled so the hot path
  // allocates no record object in the (default) in-memory configuration.

  /** Log a set effect with absolute expiry (0 = no TTL). */
  _logSet(key, value, ttl) {
    if (!this.persist.enabled) return;
    this.persist.logMutation({ op: 'set', k: key, v: value, e: ttl && ttl > 0 ? Date.now() + ttl : 0 });
  }

  _logDel(key) {
    if (!this.persist.enabled) return;
    this.persist.logMutation({ op: 'del', k: key });
  }

  _logClear() {
    if (!this.persist.enabled) return;
    this.persist.logMutation({ op: 'clear' });
  }

  /** A successful CAS is either a set (to `next`) or a delete (when `next` is undefined). */
  _logCasEffect(key, next) {
    if (!this.persist.enabled) return;
    if (next === undefined) this.persist.logMutation({ op: 'del', k: key });
    else this._logSet(key, next, this.store.remainingTTL(key)); // preserve any in-place TTL
  }

  /** Issue the next monotonic fencing token. Single event loop ⇒ no locking needed. */
  _mintToken() {
    if (this.nextToken >= Number.MAX_SAFE_INTEGER) {
      const err = new Error('fencing token space exhausted');
      err.code = 'EFENCEEXHAUSTED';
      throw err;
    }
    this.nextToken += 1;
    if (this.persist) this.persist.noteToken(this.nextToken);
    return this.nextToken;
  }

  /**
   * Gate a fenced mutation: reject (EFENCED) if the presented token is older than the highest
   * ever issued for the governing lock key — i.e. the caller's lock was superseded. Throws so
   * the surrounding _handle try/catch relays it as an ERR. On success, raises the bar.
   */
  _fence(lockKey, token) {
    const high = this.locks.getFenceHigh(lockKey);
    if (token == null || token < high) {
      const err = new Error(`fenced: token ${token} < ${high} for "${lockKey}"`);
      err.code = 'EFENCED';
      throw err;
    }
    this.locks.bumpFence(lockKey, token);
  }

  _handle(conn, msg) {
    const { t, id } = msg;

    // Handshake / auth gate: until a connection says HELLO with a valid token,
    // it may only send HELLO, PING, or PONG.
    if (!conn.authed && t !== TYPES.HELLO && t !== TYPES.PING && t !== TYPES.PONG) {
      this._err(conn, id, 'not authenticated', 'EAUTH');
      return;
    }

    // Count ops only for KNOWN types, and only after the auth gate. `t` is
    // attacker-controlled, so keying the counter by an arbitrary string before
    // these checks would let an (even unauthenticated) peer grow this object
    // without bound — a memory-exhaustion DoS. Unknown types fall through to the
    // `default` branch and never create a counter key.
    if (KNOWN_TYPES.has(t)) {
      this.stats.ops[t] = (this.stats.ops[t] || 0) + 1;
    }

    try {
      switch (t) {
        case TYPES.HELLO:
          if (this.token && msg.token !== this.token) {
            this._err(conn, id, 'invalid auth token', 'EAUTH');
            conn.peer.destroy();
            return;
          }
          conn.authed = true;
          conn.name = msg.name || null;
          conn.peer.send({ t: TYPES.WELCOME, id, version: PROTOCOL_VERSION, broker: this.name });
          break;
        case TYPES.PING:
          conn.peer.send({ t: TYPES.PONG, id });
          break;
        case TYPES.PONG:
          break; // lastSeen already stamped on receipt
        case TYPES.SHUTDOWN:
          this._ok(conn, id, true);
          setImmediate(() => this.close());
          break;
        case TYPES.STATS:
          this._ok(conn, id, this.snapshot());
          break;

        // ---- cache ----
        case TYPES.GET:
          this._ok(conn, id, this.store.get(msg.key));
          break;
        case TYPES.SET:
          this._ok(conn, id, this.store.set(msg.key, msg.value, msg.ttl));
          this._logSet(msg.key, msg.value, msg.ttl);
          break;
        case TYPES.DEL:
          this._ok(conn, id, this.store.del(msg.key));
          this._logDel(msg.key);
          break;
        case TYPES.HAS:
          this._ok(conn, id, this.store.has(msg.key));
          break;
        case TYPES.KEYS:
          this._ok(conn, id, this.store.keys());
          break;
        case TYPES.CLEAR:
          this._ok(conn, id, this.store.clear());
          this._logClear();
          break;
        case TYPES.MGET:
          this._ok(conn, id, this.store.mget(msg.keys || []));
          break;
        case TYPES.MSET: {
          const entries = msg.entries || [];
          this._ok(conn, id, this.store.mset(entries));
          for (const [k, v] of entries) this._logSet(k, v, 0);
          break;
        }

        // ---- atomic ----
        case TYPES.INCR: {
          const next = this.store.incr(msg.key, msg.by == null ? 1 : msg.by);
          this._logSet(msg.key, next, this.store.remainingTTL(msg.key)); // preserve any TTL
          this._ok(conn, id, next);
          break;
        }
        case TYPES.DECR: {
          const next = this.store.incr(msg.key, -(msg.by == null ? 1 : msg.by));
          this._logSet(msg.key, next, this.store.remainingTTL(msg.key)); // preserve any TTL
          this._ok(conn, id, next);
          break;
        }
        case TYPES.CAS: {
          const ok = this.store.cas(msg.key, msg.prev, msg.next);
          if (ok) this._logCasEffect(msg.key, msg.next);
          this._ok(conn, id, ok);
          break;
        }

        // ---- locks ----
        case TYPES.LOCK:
          this.locks
            .acquire(msg.key, conn.id, { ttl: msg.ttl, wait: msg.wait })
            .then((res) => this._ok(conn, id, res));
          break;
        case TYPES.UNLOCK:
          this._ok(conn, id, this.locks.release(msg.key, conn.id));
          break;

        // ---- fenced mutations (gated by a lock's fencing token) ----
        case TYPES.FSET:
          this._fence(msg.key, msg.token);
          this._ok(conn, id, this.store.set(msg.k, msg.value, msg.ttl));
          this._logSet(msg.k, msg.value, msg.ttl);
          break;
        case TYPES.FCAS: {
          this._fence(msg.key, msg.token);
          const ok = this.store.cas(msg.k, msg.prev, msg.next);
          if (ok) this._logCasEffect(msg.k, msg.next);
          this._ok(conn, id, ok);
          break;
        }
        case TYPES.FDEL:
          this._fence(msg.key, msg.token);
          this._ok(conn, id, this.store.del(msg.k));
          this._logDel(msg.k);
          break;

        // ---- pub/sub ----
        case TYPES.SUBSCRIBE:
          this._subscribe(conn, msg.channel);
          this._ok(conn, id, true);
          break;
        case TYPES.UNSUBSCRIBE:
          this._unsubscribe(conn, msg.channel);
          this._ok(conn, id, true);
          break;
        case TYPES.PUBLISH:
          this._ok(conn, id, this._publish(msg.channel, msg.payload));
          break;

        // ---- rpc ----
        case TYPES.REGISTER:
          this._register(conn, msg.name);
          this._ok(conn, id, true);
          break;
        case TYPES.UNREGISTER:
          this._unregister(conn, msg.name);
          this._ok(conn, id, true);
          break;
        case TYPES.CALL:
          this._call(conn, msg);
          break;
        case TYPES.RESULT:
          this._result(msg);
          break;

        default:
          this._err(conn, id, `unknown message type: ${t}`, 'EUNKNOWN');
      }
    } catch (err) {
      this._err(conn, id, err.message, err.code || 'EBROKER');
    }
  }

  // ------------------------------------------------------------------- pub/sub

  _subscribe(conn, channel) {
    conn.subs.add(channel);
    const map = isPattern(channel) ? this.patterns : this.channels;
    let set = map.get(channel);
    if (!set) {
      set = new Set();
      map.set(channel, set);
    }
    set.add(conn.id);
  }

  _unsubscribe(conn, channel) {
    conn.subs.delete(channel);
    const map = isPattern(channel) ? this.patterns : this.channels;
    const set = map.get(channel);
    if (set) {
      set.delete(conn.id);
      if (set.size === 0) map.delete(channel);
    }
  }

  _publish(channel, payload) {
    // Collect target conns: exact subscribers + any matching wildcard patterns.
    // Dedupe so a conn subscribed both exactly and by pattern gets one copy.
    const targets = new Set(this.channels.get(channel) || []);
    if (this.patterns.size) {
      for (const [pattern, set] of this.patterns) {
        if (matchTopic(pattern, channel)) {
          for (const cid of set) targets.add(cid);
        }
      }
    }
    let delivered = 0;
    for (const cid of targets) {
      const c = this.conns.get(cid);
      if (!c) continue;
      const r = c.peer.send({ t: TYPES.MESSAGE, channel, payload }, { droppable: true });
      if (r === 'dropped') {
        this.dropped++;
        this.emit('drop', { channel, connId: cid });
      } else {
        delivered++;
      }
    }
    return delivered;
  }

  // ----------------------------------------------------------------------- rpc

  _register(conn, name) {
    let entry = this.procs.get(name);
    if (!entry) {
      entry = { workers: new Set(), inflight: new Map() };
      this.procs.set(name, entry);
    }
    entry.workers.add(conn.id);
    if (!entry.inflight.has(conn.id)) entry.inflight.set(conn.id, 0);
    conn.procs.add(name);
  }

  _unregister(conn, name) {
    conn.procs.delete(name);
    const entry = this.procs.get(name);
    if (!entry) return;
    entry.workers.delete(conn.id);
    entry.inflight.delete(conn.id);
    if (entry.workers.size === 0) this.procs.delete(name);
  }

  /** Pick the least-busy worker for a proc (fewest in-flight calls). */
  _pickWorker(entry) {
    let best = null;
    let bestLoad = Infinity;
    for (const cid of entry.workers) {
      const load = entry.inflight.get(cid) || 0;
      if (load < bestLoad) {
        bestLoad = load;
        best = cid;
      }
    }
    return best;
  }

  _call(conn, msg) {
    const entry = this.procs.get(msg.name);
    if (!entry || entry.workers.size === 0) {
      this._err(conn, msg.id, `no handler registered for "${msg.name}"`, 'ENOHANDLER');
      return;
    }
    const workerId = this._pickWorker(entry);
    const owner = workerId != null ? this.conns.get(workerId) : null;
    if (!owner) {
      // Stale worker entry (conn already gone) — prune it and report unavailable.
      if (workerId != null) {
        entry.workers.delete(workerId);
        entry.inflight.delete(workerId);
        if (entry.workers.size === 0) this.procs.delete(msg.name);
      }
      this._err(conn, msg.id, `handler "${msg.name}" unavailable`, 'ENOHANDLER');
      return;
    }
    const brokerCallId = this.nextCallId++;
    // Backstop timeout so a worker that stays connected but never replies can't leak a `pending`
    // entry forever or pin its `inflight` count (which would permanently skew least-busy dispatch).
    // Fire a grace period AFTER the caller's own deadline so the broker is the cleanup backstop and
    // doesn't race the client-side timeout that the user actually sees.
    const callerTimeout = msg.timeout && msg.timeout > 0 ? msg.timeout : this.callTimeout;
    const timer = setTimeout(() => this._expireCall(brokerCallId), callerTimeout + this.callTimeoutGrace);
    if (timer.unref) timer.unref();
    this.pending.set(brokerCallId, {
      callerConnId: conn.id,
      callerCallId: msg.id,
      ownerConnId: owner.id,
      name: msg.name,
      timer,
    });
    entry.inflight.set(owner.id, (entry.inflight.get(owner.id) || 0) + 1);
    owner.peer.send({ t: TYPES.INVOKE, id: brokerCallId, name: msg.name, args: msg.args || [] });
  }

  /** A pending call exceeded its deadline (worker hung while connected): free state, fail the caller. */
  _expireCall(brokerCallId) {
    const p = this.pending.get(brokerCallId);
    if (!p) return;
    this.pending.delete(brokerCallId);
    this._decInflight(p.name, p.ownerConnId);
    const caller = this.conns.get(p.callerConnId);
    if (caller) this._err(caller, p.callerCallId, `rpc call "${p.name}" timed out`, 'ECALLTIMEOUT');
  }

  _decInflight(name, ownerConnId) {
    const entry = this.procs.get(name);
    if (!entry) return;
    const cur = entry.inflight.get(ownerConnId);
    if (cur != null) entry.inflight.set(ownerConnId, Math.max(0, cur - 1));
  }

  _result(msg) {
    const p = this.pending.get(msg.id);
    if (!p) return;
    this.pending.delete(msg.id);
    clearTimeout(p.timer);
    this._decInflight(p.name, p.ownerConnId);
    const caller = this.conns.get(p.callerConnId);
    if (!caller) return;
    if (msg.error) {
      this._err(caller, p.callerCallId, msg.error.message, msg.error.code || 'ECALL');
    } else {
      this._ok(caller, p.callerCallId, msg.result);
    }
  }

  // ------------------------------------------------------------------ lifecycle

  _onClose(conn) {
    this.conns.delete(conn.id);
    this.emit('disconnect', conn.id);
    for (const ch of conn.subs) {
      const map = isPattern(ch) ? this.patterns : this.channels;
      const set = map.get(ch);
      if (set) {
        set.delete(conn.id);
        if (set.size === 0) map.delete(ch);
      }
    }
    for (const name of conn.procs) {
      const entry = this.procs.get(name);
      if (entry) {
        entry.workers.delete(conn.id);
        entry.inflight.delete(conn.id);
        if (entry.workers.size === 0) this.procs.delete(name);
      }
    }
    this.locks.releaseAll(conn.id);
    // Fail in-flight calls owned by this connection; drop calls it originated.
    for (const [bid, p] of this.pending) {
      if (p.ownerConnId === conn.id) {
        clearTimeout(p.timer);
        this.pending.delete(bid);
        this._decInflight(p.name, p.ownerConnId);
        const caller = this.conns.get(p.callerConnId);
        if (caller) this._err(caller, p.callerCallId, 'rpc handler disconnected', 'EHANDLERGONE');
      } else if (p.callerConnId === conn.id) {
        clearTimeout(p.timer);
        this.pending.delete(bid);
        this._decInflight(p.name, p.ownerConnId);
      }
    }
    this._scheduleIdle();
  }

  _startHeartbeat() {
    if (!this.heartbeatInterval) return;
    this._heartbeatTimer = setInterval(() => this._sweepHeartbeat(), this.heartbeatInterval);
    if (this._heartbeatTimer.unref) this._heartbeatTimer.unref();
  }

  _sweepHeartbeat() {
    const now = nowMs();
    // Reap only after 3 intervals of silence. The ping window (idle > interval)
    // must be at least 2 sweeps wide so a healthy conn is always pinged — and
    // gets a chance to answer — before it can ever be reaped.
    const reapAfter = this.heartbeatInterval * 3;
    for (const conn of this.conns.values()) {
      const idle = now - conn.lastSeen;
      if (idle > reapAfter) {
        this.stats.reaped += 1;
        this.emit('reap', conn.id);
        conn.peer.destroy();
      } else if (idle > this.heartbeatInterval) {
        conn.peer.send({ t: TYPES.PING });
      }
    }
  }

  _startStats() {
    if (!this.statsInterval) return;
    this._statsTimer = setInterval(() => this.emit('stats', this.snapshot()), this.statsInterval);
    if (this._statsTimer.unref) this._statsTimer.unref();
  }

  /** A point-in-time operational snapshot — served on STATS and emitted periodically. */
  snapshot() {
    const cpu = process.cpuUsage(this._cpuBase);
    const windowUs = Math.max(1, (nowMs() - this._cpuBaseAt) * 1000);
    const lockStats = this.locks.stats();
    const procs = [];
    for (const [name, entry] of this.procs) procs.push({ name, workers: entry.workers.size });
    return {
      uptimeMs: nowMs() - this.startedAt,
      connections: this.conns.size,
      cacheSize: this.store.size,
      ops: { ...this.stats.ops },
      dropped: this.dropped,
      reaped: this.stats.reaped,
      locks: lockStats.locks,
      lockWaiters: lockStats.waiters,
      pendingCalls: this.pending.size,
      subscriptions: this.channels.size + this.patterns.size,
      procs,
      memory: process.memoryUsage(),
      cpuCoreFraction: (cpu.user + cpu.system) / windowUs,
    };
  }

  _scheduleIdle() {
    if (!this.idleTimeout || this.conns.size > 0) return;
    this._cancelIdle();
    this._idleTimer = setTimeout(() => this.close(), this.idleTimeout);
    if (this._idleTimer.unref) this._idleTimer.unref();
  }

  _cancelIdle() {
    if (this._idleTimer) {
      clearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
  }

  async close() {
    this._cancelIdle();
    if (this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer);
      this._heartbeatTimer = null;
    }
    if (this._statsTimer) {
      clearInterval(this._statsTimer);
      this._statsTimer = null;
    }
    for (const c of this.conns.values()) c.peer.destroy();
    this.conns.clear();
    await this.persist.flushAndClose(); // final snapshot + fsync → planned restart is lossless
    if (this.server) {
      await new Promise((resolve) => this.server.close(() => resolve()));
    }
    if (!isPipe(this.address)) {
      try {
        fs.unlinkSync(this.address);
      } catch {
        /* ignore */
      }
    }
  }
}

/** Monotonic-ish clock; avoids Date dependency on the hot path. */
function nowMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

module.exports = Broker;
