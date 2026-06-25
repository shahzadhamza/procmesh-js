'use strict';

const net = require('net');
const path = require('path');
const EventEmitter = require('events');
const { spawn } = require('child_process');
const { Peer, TYPES, matchTopic } = require('./protocol');
const { resolveCodec } = require('./codec');
const { resolveAddress, isPipe } = require('./transport');
const errors = require('./errors');

const delay = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Client handle to the shared broker. All methods are async and return Promises.
 * The client transparently spawns a broker if none is running (autoSpawn), and
 * reconnects with backoff, replaying subscriptions and RPC registrations.
 *
 * Emits: 'connect', 'disconnect', 'reconnect', 'error'.
 */
class Client extends EventEmitter {
  constructor(opts = {}) {
    super();
    this.opts = opts;
    this.name = opts.name || 'default';
    this.address = opts.address || resolveAddress(this.name);
    this.codec = resolveCodec(opts.codec);
    this.autoSpawn = opts.autoSpawn !== false;
    // A custom { encode, decode } codec is a pair of functions that can't cross the `spawn`
    // boundary, so an auto-spawned broker would silently fall back to JSON and every frame would
    // then fail to decode. Refuse the footgun up front: run the broker yourself with the same codec.
    if (this.autoSpawn && opts.codec && typeof opts.codec === 'object') {
      throw new Error(
        "a custom { encode, decode } codec can't be forwarded to an auto-spawned broker. Run the " +
          'broker yourself (procmesh serve / createBroker) with the same codec and pass ' +
          "autoSpawn: false, or use the 'json' / 'msgpack' codec."
      );
    }
    this.reconnectEnabled = opts.reconnect !== false;
    this.token = opts.token || null;
    this.callTimeout = opts.callTimeout || 30000;
    this.maxReconnectDelay = opts.maxReconnectDelay || 5000;
    // Optional client-initiated keepalive. The broker reaps dead *clients*, but only the client can
    // notice a dead *broker* on a half-open link (or when the broker's heartbeat is disabled). When
    // set (>0), ping on this interval and tear the link down — triggering reconnect — if no PONG
    // comes back in time. 0 = rely on the broker heartbeat (default; unchanged behavior).
    this.pingInterval = opts.pingInterval || 0;
    this._pingTimer = null;

    this.peer = null;
    this.connected = false;
    this.closed = false;
    this.nextId = 1;
    this.pending = new Map(); // id -> { resolve, reject, timer, t }
    this.subscriptions = new Map(); // channel -> Set<handler>
    this.handlers = new Map(); // procName -> fn

    this._connectPromise = null;
    this._reconnectAttempt = 0;
    this._reconnectTimer = null;
  }

  // ---------------------------------------------------------------- connection

  async connect() {
    if (this.connected) return this;
    if (!this._connectPromise) {
      this._connectPromise = this._doConnect().finally(() => {
        this._connectPromise = null;
      });
    }
    await this._connectPromise;
    return this;
  }

  async _doConnect() {
    let spawned = false;
    const maxAttempts = this.autoSpawn ? 100 : 1;
    for (let attempt = 1; ; attempt++) {
      if (this.closed) throw new errors.Disconnected('client is closed');
      try {
        await this._open();
        return;
      } catch (err) {
        const retriable = err.code === 'ENOENT' || err.code === 'ECONNREFUSED';
        if (!retriable || !this.autoSpawn || attempt >= maxAttempts) throw err;
        if (!spawned) {
          this._spawnBroker();
          spawned = true;
        }
        await delay(Math.min(25 * attempt, 250));
      }
    }
  }

  _open() {
    return new Promise((resolve, reject) => {
      const socket = net.connect(this.address);
      socket.setNoDelay(true);
      const onError = (err) => {
        socket.destroy();
        reject(err);
      };
      socket.once('error', onError);
      socket.once('connect', async () => {
        socket.removeListener('error', onError);
        this.peer = new Peer(socket, this.codec);
        this.connected = true;
        this._reconnectAttempt = 0;
        this.peer.on('message', (msg) => this._onMessage(msg));
        this.peer.on('close', () => this._onClose());
        this.peer.on('error', () => {
          /* 'close' handles it */
        });
        try {
          await this._request(TYPES.HELLO, { name: this.name, token: this.token });
          await this._replay();
          this._startKeepalive();
          this.emit('connect');
          resolve();
        } catch (err) {
          // A rejected auth is fatal — don't loop reconnecting with a bad token.
          if (err && err.code === 'EAUTH') this.reconnectEnabled = false;
          reject(err);
        }
      });
    });
  }

  _spawnBroker() {
    const brokerOpts = {
      address: this.address,
      name: this.name,
      codec: typeof this.opts.codec === 'string' ? this.opts.codec : 'json',
      cache: this.opts.cache,
      token: this.token,
      idleTimeout: this.opts.brokerIdleTimeout == null ? 60000 : this.opts.brokerIdleTimeout,
    };
    const child = spawn(process.execPath, [path.join(__dirname, 'broker-bin.js')], {
      detached: true,
      stdio: 'ignore',
      env: { ...process.env, PROCMESH_BROKER_OPTS: JSON.stringify(brokerOpts) },
    });
    child.unref();
  }

  async _replay() {
    for (const channel of this.subscriptions.keys()) {
      await this._request(TYPES.SUBSCRIBE, { channel });
    }
    for (const name of this.handlers.keys()) {
      await this._request(TYPES.REGISTER, { name });
    }
  }

  /** Begin client-side keepalive pings (no-op unless `pingInterval` was configured). */
  _startKeepalive() {
    this._stopKeepalive();
    if (!this.pingInterval) return;
    this._pingTimer = setInterval(() => {
      if (!this.connected || !this.peer) return;
      // An unanswered ping within the interval means the link is dead — drop it so `_onClose`
      // fires and reconnect kicks in. (A late PONG after this just settles nothing.)
      this._request(TYPES.PING, {}, this.pingInterval).catch(() => {
        if (this.peer) this.peer.destroy();
      });
    }, this.pingInterval);
    if (this._pingTimer.unref) this._pingTimer.unref();
  }

  _stopKeepalive() {
    if (this._pingTimer) {
      clearInterval(this._pingTimer);
      this._pingTimer = null;
    }
  }

  _onClose() {
    this.connected = false;
    this.peer = null;
    this._stopKeepalive();
    const err = new errors.Disconnected('connection to broker lost');
    for (const p of this.pending.values()) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
    this.emit('disconnect');
    if (!this.closed && this.reconnectEnabled) this._scheduleReconnect();
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    this._reconnectAttempt++;
    // Exponential backoff with ±50% jitter so a fleet of clients reconnecting after a broker
    // restart doesn't thunder back in lockstep.
    const base = Math.min(100 * 2 ** (this._reconnectAttempt - 1), this.maxReconnectDelay);
    const d = base * (0.5 + Math.random() * 0.5);
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      if (this.closed) return;
      this._doConnect()
        .then(() => this.emit('reconnect'))
        .catch(() => {
          if (!this.closed) this._scheduleReconnect();
        });
    }, d);
    if (this._reconnectTimer.unref) this._reconnectTimer.unref();
  }

  // ------------------------------------------------------------------ messaging

  _request(t, payload = {}, timeout = this.callTimeout) {
    return new Promise((resolve, reject) => {
      if (!this.peer || !this.connected) {
        reject(new errors.Disconnected());
        return;
      }
      const id = this.nextId++;
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new errors.CallTimeout(`request "${t}" timed out after ${timeout}ms`));
      }, timeout);
      if (timer.unref) timer.unref();
      this.pending.set(id, { resolve, reject, timer, t });
      this.peer.send({ t, id, ...payload });
    });
  }

  _settle(id, fn) {
    const p = this.pending.get(id);
    if (!p) return;
    this.pending.delete(id);
    clearTimeout(p.timer);
    fn(p);
  }

  _onMessage(msg) {
    switch (msg.t) {
      case TYPES.OK:
        this._settle(msg.id, (p) => p.resolve(msg.value));
        break;
      case TYPES.WELCOME:
      case TYPES.PONG:
        this._settle(msg.id, (p) => p.resolve(msg.t === TYPES.PONG ? true : msg));
        break;
      case TYPES.ERR:
        this._settle(msg.id, (p) => p.reject(new errors.RemoteError(msg.message, msg.code)));
        break;
      case TYPES.MESSAGE:
        this._deliver(msg.channel, msg.payload);
        break;
      case TYPES.PING:
        // Unsolicited broker heartbeat — answer so we're not reaped.
        if (this.peer) this.peer.send({ t: TYPES.PONG });
        break;
      case TYPES.INVOKE:
        this._onInvoke(msg);
        break;
      default:
        break;
    }
  }

  /** Route an incoming message to every local handler whose subscription matches. */
  _deliver(channel, payload) {
    // Collect matching handlers (deduped, so a handler subscribed both exactly and
    // by pattern only fires once for a given message).
    let matched = null;
    for (const [sub, set] of this.subscriptions) {
      if (matchTopic(sub, channel)) {
        if (!matched) matched = new Set();
        for (const h of set) matched.add(h);
      }
    }
    if (!matched) return;
    for (const h of matched) {
      try {
        h(payload, channel);
      } catch (err) {
        this.emit('error', err);
      }
    }
  }

  async _onInvoke(msg) {
    const fn = this.handlers.get(msg.name);
    if (!fn) {
      this.peer.send({
        t: TYPES.RESULT,
        id: msg.id,
        error: { message: `no handler "${msg.name}"`, code: 'ENOHANDLER' },
      });
      return;
    }
    try {
      const result = await fn(...(msg.args || []));
      if (this.peer) this.peer.send({ t: TYPES.RESULT, id: msg.id, result });
    } catch (err) {
      if (this.peer) {
        this.peer.send({
          t: TYPES.RESULT,
          id: msg.id,
          error: { message: err.message, code: err.code || 'EHANDLER' },
        });
      }
    }
  }

  // --------------------------------------------------------------------- cache

  get(key) {
    return this._request(TYPES.GET, { key });
  }

  set(key, value, opts = {}) {
    return this._request(TYPES.SET, { key, value, ttl: opts.ttl });
  }

  del(key) {
    return this._request(TYPES.DEL, { key });
  }

  has(key) {
    return this._request(TYPES.HAS, { key });
  }

  keys() {
    return this._request(TYPES.KEYS, {});
  }

  clear() {
    return this._request(TYPES.CLEAR, {});
  }

  async mget(keys) {
    const res = await this._request(TYPES.MGET, { keys });
    return res.values.map((v, i) => (res.found[i] ? v : undefined));
  }

  mset(entries) {
    const list = Array.isArray(entries) ? entries : Object.entries(entries);
    return this._request(TYPES.MSET, { entries: list });
  }

  // -------------------------------------------------------------------- atomic

  incr(key, by = 1) {
    return this._request(TYPES.INCR, { key, by });
  }

  decr(key, by = 1) {
    return this._request(TYPES.DECR, { key, by });
  }

  cas(key, prev, next) {
    return this._request(TYPES.CAS, { key, prev, next });
  }

  // -------------------------------------------------------------------- pub/sub

  async subscribe(channel, handler) {
    if (typeof handler !== 'function') throw new TypeError('subscribe requires a handler function');
    let set = this.subscriptions.get(channel);
    const isNew = !set;
    if (!set) {
      set = new Set();
      this.subscriptions.set(channel, set);
    }
    set.add(handler);
    if (isNew && this.connected) await this._request(TYPES.SUBSCRIBE, { channel });
    return () => this.unsubscribe(channel, handler);
  }

  async unsubscribe(channel, handler) {
    const set = this.subscriptions.get(channel);
    if (!set) return;
    if (handler) set.delete(handler);
    else set.clear();
    if (set.size === 0) {
      this.subscriptions.delete(channel);
      if (this.connected) await this._request(TYPES.UNSUBSCRIBE, { channel });
    }
  }

  publish(channel, payload) {
    return this._request(TYPES.PUBLISH, { channel, payload });
  }

  // ------------------------------------------------------------------------ rpc

  async register(name, fn) {
    if (typeof fn !== 'function') throw new TypeError('register requires a handler function');
    this.handlers.set(name, fn);
    if (this.connected) await this._request(TYPES.REGISTER, { name });
  }

  async unregister(name) {
    this.handlers.delete(name);
    if (this.connected) await this._request(TYPES.UNREGISTER, { name });
  }

  call(name, args = [], opts = {}) {
    const timeout = opts.timeout || this.callTimeout;
    // Send `timeout` so the broker can set a matching cleanup backstop: if the worker hangs while
    // connected, the broker frees its `pending`/`inflight` state shortly after we give up here.
    return this._request(TYPES.CALL, { name, args, timeout }, timeout);
  }

  // ---------------------------------------------------------------------- locks

  /**
   * Acquire a lock. Resolves with a release function, or null if not acquired
   * (only possible when `wait` is 0 or the wait timed out).
   */
  async lock(key, opts = {}) {
    const ttl = opts.ttl == null ? 30000 : opts.ttl;
    const wait = opts.wait == null ? 0 : opts.wait;
    const requestTimeout = wait > 0 ? wait + 5000 : this.callTimeout;
    const reply = await this._request(TYPES.LOCK, { key, ttl, wait }, requestTimeout);
    // Tolerate the legacy boolean reply shape (pre-v2 broker) as well as { acquired, token }.
    const acquired = reply === true || (reply && reply.acquired === true);
    const token = reply && reply.token != null ? reply.token : null;
    if (!acquired) return null;
    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      try {
        await this._request(TYPES.UNLOCK, { key });
      } catch {
        /* broker gone — lock expires via TTL */
      }
    };
    // The fencing token of this grant — pass to fenced* ops (or an external resource) so a
    // write from a holder whose lock expired (TTL overrun) is rejected with EFENCED.
    release.token = token;
    return release;
  }

  /**
   * Run `fn` while holding `key`; always releases. Throws LockTimeout if not acquired.
   * `fn` receives a context `{ token, set, cas, del }` whose set/cas/del are fenced by this
   * grant's token, so a write that lands after a TTL overrun (lock already re-granted) is
   * rejected with EFENCED instead of silently corrupting shared state. Callers that ignore
   * the argument keep working unchanged.
   */
  async withLock(key, fn, opts = {}) {
    const wait = opts.wait == null ? 10000 : opts.wait;
    const release = await this.lock(key, { wait, ttl: opts.ttl });
    if (!release) throw new errors.LockTimeout(`could not acquire lock "${key}"`);
    const token = release.token;
    const ctx = {
      token,
      set: (k, value, o = {}) => this.fencedSet(key, token, k, value, o),
      cas: (k, prev, next) => this.fencedCas(key, token, k, prev, next),
      del: (k) => this.fencedDel(key, token, k),
    };
    try {
      return await fn(ctx);
    } finally {
      await release();
    }
  }

  // ------------------------------------------------------- fenced mutations (lock-guarded)

  /** Set `key` only if `token` is current for lock `lockKey`; else rejects EFENCED. */
  fencedSet(lockKey, token, key, value, opts = {}) {
    return this._request(TYPES.FSET, { key: lockKey, token, k: key, value, ttl: opts.ttl });
  }

  /** Compare-and-set guarded by lock `lockKey`'s fencing token; rejects EFENCED if stale. */
  fencedCas(lockKey, token, key, prev, next) {
    return this._request(TYPES.FCAS, { key: lockKey, token, k: key, prev, next });
  }

  /** Delete guarded by lock `lockKey`'s fencing token; rejects EFENCED if stale. */
  fencedDel(lockKey, token, key) {
    return this._request(TYPES.FDEL, { key: lockKey, token, k: key });
  }

  // ---------------------------------------------------------------- misc / admin

  ping() {
    return this._request(TYPES.PING, {});
  }

  /** Fetch a point-in-time operational snapshot from the broker. */
  stats() {
    return this._request(TYPES.STATS, {});
  }

  /** Ask the broker to shut down (used by the CLI `stop` command). */
  shutdownBroker() {
    return this._request(TYPES.SHUTDOWN, {});
  }

  async close() {
    this.closed = true;
    this._stopKeepalive();
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    if (this.peer) this.peer.destroy();
    this.peer = null;
    this.connected = false;
  }
}

module.exports = Client;
