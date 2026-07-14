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

    // Producer-side pub/sub defaults (per-call publish() opts override these). The `pubsub` object
    // also carries broker-side persist/retention knobs, which are forwarded to an auto-spawned
    // broker in _spawnBroker and simply ignored here.
    this.pubsub = {
      acks: 1, // 0 (fire-and-forget) | 1 (broker received) | 'all' (reliable, no-drop fan-out)
      idempotent: false, // attach pid+seq so retries dedupe
      retries: 3, // publish auto-retries (idempotent + acks>=1 only)
      retryBackoff: 50, // base ms, exponential with jitter
      retryMaxDelay: 2000, // ceiling per retry
      ...(opts.pubsub || {}),
    };
    // Producer id. A caller-provided `pubsub.producerId` survives a full PROCESS restart so
    // idempotent dedup keeps working across restarts (within the broker's dedup window); otherwise
    // the broker mints one in WELCOME, which we re-present on reconnect (survives reconnects only).
    this.pid = (opts.pubsub && opts.pubsub.producerId) || null;
    this._seq = new Map(); // channel -> last sequence number (idempotent producer)
    // Seed sequences (e.g. persisted by the caller across a restart) so a restarted producer using a
    // stable producerId resumes ABOVE the broker's dedup high-water instead of being falsely deduped.
    if (opts.pubsub && opts.pubsub.sequences) {
      for (const [ch, n] of Object.entries(opts.pubsub.sequences)) this._seq.set(ch, n);
    }
    // Consumer-side highest pub/sub offset seen per subscription string. Sent as `since` on
    // reconnect so the broker replays anything published while we were disconnected, and used as a
    // monotonic dedup backstop in _deliver. Empty until the broker reports offsets (persist on).
    this._offsets = new Map();
    // Requested `replay` per channel, retained until the FIRST successful SUBSCRIBE lands. Lets a
    // subscribe() issued while disconnected still ask for its backlog once the link comes up
    // (`_replay`). Cleared once a `since` watermark exists (reconnect resumes by offset instead).
    this._replayOpts = new Map();

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
          // Re-present our producer id (if any) so idempotent dedup survives a reconnect.
          const welcome = await this._request(TYPES.HELLO, { name: this.name, token: this.token, pid: this.pid });
          if (welcome && welcome.pid) this.pid = welcome.pid;
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
      // Forward idempotency + pub/sub persistence config so an auto-spawned broker honors them.
      // (The `pubsub` object's producer-side fields are ignored broker-side.)
      dedup: this.opts.dedup,
      pubsub: this.opts.pubsub,
      persist: this.opts.persist,
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
      // Resume from our last-seen offset so the broker replays anything published while we were
      // disconnected (requires broker-side pub/sub persistence; a no-op otherwise).
      const since = this._offsets.get(channel);
      const payload = { channel };
      if (typeof since === 'number') {
        payload.since = since;
      } else if (this._replayOpts.has(channel)) {
        // No watermark yet ⇒ this subscription has never landed on a broker (subscribed while
        // disconnected). Honor its original `replay` request now instead of losing the backlog.
        payload.replay = this._replayOpts.get(channel);
      }
      const res = await this._request(TYPES.SUBSCRIBE, payload);
      this._replayOpts.delete(channel); // one-shot: spent once it lands on a broker
      this._recordBaseline(channel, res);
    }
    for (const name of this.handlers.keys()) {
      await this._request(TYPES.REGISTER, { name });
    }
  }

  /**
   * Record the broker's offset high-water reported at subscribe time, so a subscription that
   * receives zero messages still has a baseline to resume from after a disconnect. Only sets it
   * when unset — live/replayed deliveries (via _deliver) always win with a higher watermark.
   */
  _recordBaseline(channel, res) {
    if (res && typeof res.offset === 'number' && !this._offsets.has(channel)) {
      this._offsets.set(channel, res.offset);
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
        this._deliver(msg.channel, msg.payload, msg.offset);
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
  _deliver(channel, payload, offset) {
    // Collect matching handlers (deduped, so a handler subscribed both exactly and
    // by pattern only fires once for a given message).
    let matched = null;
    for (const [sub, set] of this.subscriptions) {
      if (!matchTopic(sub, channel)) continue;
      // Monotonic offset backstop: skip a subscription that has already seen this offset (or a
      // newer one), so a redelivered replay after reconnect can't double-fire. Track the max seen
      // so `_replay` can resume from it as `since`.
      if (offset != null) {
        const seen = this._offsets.get(sub);
        if (seen != null && offset <= seen) continue;
        this._offsets.set(sub, seen == null ? offset : Math.max(seen, offset));
      }
      if (!matched) matched = new Set();
      for (const h of set) matched.add(h);
    }
    if (!matched) return;
    for (const h of matched) {
      try {
        const r = h(payload, channel);
        // An async handler that rejects would otherwise escape as an unhandledRejection; route it
        // to 'error' just like a synchronous throw (caught below).
        if (r && typeof r.then === 'function') r.then(undefined, (err) => this.emit('error', err));
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

  async subscribe(channel, handler, opts = {}) {
    if (typeof handler !== 'function') throw new TypeError('subscribe requires a handler function');
    let set = this.subscriptions.get(channel);
    const isNew = !set;
    if (!set) {
      set = new Set();
      this.subscriptions.set(channel, set);
    }
    set.add(handler);
    // `replay` (true = all retained, N = last N) asks the broker to replay retained messages for
    // this channel — requires broker-side pub/sub persistence. Only meaningful on the first
    // subscribe to a channel; on reconnect `_replay` instead resumes from the last-seen offset, so
    // messages published during the outage are caught up without re-delivering everything.
    if (isNew && opts.replay != null) this._replayOpts.set(channel, opts.replay);
    if (isNew && this.connected) {
      const res = await this._request(TYPES.SUBSCRIBE, { channel, replay: opts.replay });
      // The request landed: reconnect now resumes by offset, so the one-shot replay request is spent.
      this._replayOpts.delete(channel);
      this._recordBaseline(channel, res);
    }
    return () => this.unsubscribe(channel, handler);
  }

  async unsubscribe(channel, handler) {
    const set = this.subscriptions.get(channel);
    if (!set) return;
    if (handler) set.delete(handler);
    else set.clear();
    if (set.size === 0) {
      this.subscriptions.delete(channel);
      // Drop the offset high-water too: it exists only to resume a *live* subscription across a
      // reconnect. Leaving it set would make a later subscribe(..., { replay: true }) silently
      // skip the replayed backlog (every retained offset is <= the stale watermark).
      this._offsets.delete(channel);
      this._replayOpts.delete(channel);
      if (this.connected) await this._request(TYPES.UNSUBSCRIBE, { channel });
    }
  }

  /**
   * Publish to a channel. Options (per-call, overriding the client's `pubsub` defaults):
   *   - acks: 0 (fire-and-forget, resolves immediately, returns undefined)
   *           1 (default — broker received + fanned out best-effort, resolves with delivered count)
   *           'all' (reliable no-drop fan-out, resolves with delivered count)
   *   - idempotent: attach a producer id + per-channel sequence so a retried publish is delivered
   *                 at most once; enables safe auto-retry for acks>=1.
   *   - retries / timeout: override retry count / request timeout for this call.
   *
   * Resolves with:
   *   - a number    — the delivered count: subscribers the message was QUEUED to (written to the
   *                   socket send buffer, not dropped/overflowed). NOT an end-to-end consumer ack.
   *   - null        — the publish was a deduped retry (already accepted earlier); distinct from 0,
   *                   which means a real fan-out that reached zero subscribers.
   *   - undefined   — acks:0 (fire-and-forget).
   */
  publish(channel, payload, opts = {}) {
    const acks = opts.acks == null ? this.pubsub.acks : opts.acks;
    const idempotent = opts.idempotent == null ? this.pubsub.idempotent : opts.idempotent;
    const frame = { channel, payload };
    if (acks !== 1) frame.acks = acks; // omit for the default to stay wire-minimal
    if (idempotent) {
      frame.pid = this.pid;
      const seq = (this._seq.get(channel) || 0) + 1;
      this._seq.set(channel, seq);
      frame.seq = seq;
    }
    if (acks === 0) {
      // Fire-and-forget: no request id, don't wait for (or expect) a reply.
      if (!this.peer || !this.connected) return Promise.reject(new errors.Disconnected());
      this.peer.send({ t: TYPES.PUBLISH, ...frame });
      return Promise.resolve(undefined);
    }
    const timeout = opts.timeout || this.callTimeout;
    const retries = opts.retries == null ? this.pubsub.retries : opts.retries;
    // Auto-retry is only safe when idempotent (else a retry after a landed publish would duplicate).
    if (idempotent && retries > 0) return this._publishWithRetry(frame, retries, timeout, opts);
    return this._request(TYPES.PUBLISH, frame, timeout);
  }

  /** Retry an idempotent publish (same seq) on timeout/disconnect; the broker dedupes on landing. */
  async _publishWithRetry(frame, retries, timeout, opts) {
    const base = opts.retryBackoff == null ? this.pubsub.retryBackoff : opts.retryBackoff;
    const maxDelay = opts.retryMaxDelay == null ? this.pubsub.retryMaxDelay : opts.retryMaxDelay;
    // Bound the whole retry sequence (including any reconnect wait) by the caller's timeout.
    const deadline = Date.now() + timeout;
    for (let attempt = 0; ; attempt++) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new errors.CallTimeout(`publish timed out after ${timeout}ms`);
      try {
        // Resolve the producer id at SEND time, not build time. If this frame was created before
        // the first connection completed (and no explicit producerId was set), this.pid was null
        // when publish() built it; the broker only minted one in WELCOME after _waitForConnect
        // below. Refreshing here keeps the stable seq while picking up the real pid, so the broker
        // actually dedupes retries instead of skipping dedup on a null pid (double-delivery).
        frame.pid = this.pid;
        return await this._request(TYPES.PUBLISH, frame, remaining);
      } catch (err) {
        const retriable = err.code === 'ETIMEOUT' || err.code === 'EDISCONNECTED';
        if (!retriable || attempt >= retries) throw err;
        const left = deadline - Date.now();
        if (left <= 0) throw err;
        if (err.code === 'EDISCONNECTED' || !this.connected) {
          // The link is down. A fixed backoff would just burn the retry budget before we've even
          // reconnected, so wait for reconnection instead (bounded by the remaining time). If no
          // reconnect is coming, fail fast rather than stall until the deadline.
          if (!this.reconnectEnabled) throw err;
          await this._waitForConnect(left);
        } else {
          const d = Math.min(base * 2 ** attempt, maxDelay);
          await delay(Math.min(d * (0.5 + Math.random() * 0.5), left));
        }
      }
    }
  }

  /** Resolve once connected (immediately if already), or reject with CallTimeout after `ms`. */
  _waitForConnect(ms) {
    if (this.connected) return Promise.resolve();
    return new Promise((resolve, reject) => {
      let done = false;
      const settle = (fn, arg) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        this.removeListener('connect', onUp);
        this.removeListener('reconnect', onUp);
        fn(arg);
      };
      const onUp = () => settle(resolve);
      const timer = setTimeout(
        () => settle(reject, new errors.CallTimeout(`timed out waiting to reconnect after ${ms}ms`)),
        ms
      );
      if (timer.unref) timer.unref();
      // Both fire on a successful reconnect ('connect' from _open, then 'reconnect'); onUp is idempotent.
      this.on('connect', onUp);
      this.on('reconnect', onUp);
    });
  }

  /**
   * Seed/override the last sequence number used for `channel` (idempotent producer). Persist these
   * across a process restart alongside a stable `pubsub.producerId` so retries keep deduping.
   */
  setSequence(channel, n) {
    this._seq.set(channel, n);
  }

  /** The last sequence number used for `channel` (0 if none yet) — persist to survive a restart. */
  sequence(channel) {
    return this._seq.get(channel) || 0;
  }

  /**
   * A Kafka-style producer handle: binds default publish options so callers don't repeat them.
   *   const p = mesh.producer({ acks: 'all', idempotent: true });
   *   await p.publish('orders', msg);
   */
  producer(defaults = {}) {
    return {
      publish: (channel, payload, opts = {}) => this.publish(channel, payload, { ...defaults, ...opts }),
    };
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
