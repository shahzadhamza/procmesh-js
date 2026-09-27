'use strict';

const EventEmitter = require('events');

const PROTOCOL_VERSION = 4;

/** Message type tags. Kept short to minimize JSON overhead. */
const TYPES = {
  // connection / control
  HELLO: 'hello',
  WELCOME: 'welcome',
  PING: 'ping',
  PONG: 'pong',
  OK: 'ok',
  ERR: 'err',
  SHUTDOWN: 'shutdown',
  STATS: 'stats',
  // cache
  GET: 'get',
  SET: 'set',
  DEL: 'del',
  HAS: 'has',
  KEYS: 'keys',
  CLEAR: 'clear',
  MGET: 'mget',
  MSET: 'mset',
  // atomic
  INCR: 'incr',
  DECR: 'decr',
  CAS: 'cas',
  // locks
  LOCK: 'lock',
  UNLOCK: 'unlock',
  // fenced mutations (guarded by a lock's fencing token)
  FSET: 'fset',
  FCAS: 'fcas',
  FDEL: 'fdel',
  // pub/sub
  SUBSCRIBE: 'sub',
  UNSUBSCRIBE: 'unsub',
  PUBLISH: 'pub',
  MESSAGE: 'msg',
  // rpc
  REGISTER: 'reg',
  UNREGISTER: 'unreg',
  CALL: 'call',
  INVOKE: 'invoke',
  RESULT: 'result',
};

/**
 * Topic match. A subscription ending in `*` matches by prefix (everything before
 * the `*`); otherwise it must match the channel exactly. Predictable and cheap —
 * no regex. e.g. `matchTopic('orders.*', 'orders.created') === true`.
 */
function matchTopic(pattern, channel) {
  if (pattern === channel) return true;
  if (pattern.endsWith('*')) return channel.startsWith(pattern.slice(0, -1));
  return false;
}

/** Whether a subscription string is a wildcard pattern rather than an exact channel. */
function isPattern(sub) {
  return typeof sub === 'string' && sub.endsWith('*');
}

/** Encode an object as a length-prefixed frame: [uint32 BE length][payload]. */
function encodeFrame(codec, obj) {
  const payload = codec.encode(obj);
  const frame = Buffer.allocUnsafe(4 + payload.length);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, 4);
  return frame;
}

/**
 * Incremental decoder that buffers partial reads and yields complete frames.
 *
 * Chunks are queued, not concatenated on arrival: re-concatenating the whole buffer on every read
 * made a large frame quadratic (a 100MB frame in 64KB reads copied ~80GB). Here each byte is copied
 * at most once — only when a frame straddles chunks.
 */
class FrameDecoder {
  constructor(codec, { maxFrameSize } = {}) {
    this.codec = codec;
    // No frame-size check unless the user configures one.
    this.maxFrameSize = maxFrameSize > 0 ? maxFrameSize : undefined;
    this.chunks = [];
    this.length = 0; // total buffered bytes across `chunks`
  }

  push(chunk, onMessage) {
    if (chunk.length) {
      this.chunks.push(chunk);
      this.length += chunk.length;
    }
    for (;;) {
      if (this.length < 4) return;
      const len = this._peekLength();
      if (this.maxFrameSize !== undefined && len > this.maxFrameSize) {
        throw new Error(`frame too large: ${len} > ${this.maxFrameSize}`);
      }
      if (this.length < 4 + len) return;
      this._consume(4);
      const obj = this.codec.decode(this._consume(len));
      onMessage(obj);
    }
  }

  /** Read the uint32 length prefix without consuming it (it may straddle chunks). */
  _peekLength() {
    const first = this.chunks[0];
    if (first.length >= 4) return first.readUInt32BE(0);
    const head = Buffer.allocUnsafe(4);
    let off = 0;
    for (const c of this.chunks) {
      off += c.copy(head, off, 0, Math.min(c.length, 4 - off));
      if (off === 4) break;
    }
    return head.readUInt32BE(0);
  }

  /** Remove and return the next `n` bytes: zero-copy when they sit in one chunk. */
  _consume(n) {
    this.length -= n;
    const first = this.chunks[0];
    if (n === 0) return Buffer.alloc(0);
    if (first.length >= n) {
      if (first.length === n) this.chunks.shift();
      else this.chunks[0] = first.subarray(n);
      return first.subarray(0, n);
    }
    const out = Buffer.allocUnsafe(n);
    let off = 0;
    while (off < n) {
      const c = this.chunks[0];
      const k = Math.min(c.length, n - off);
      c.copy(out, off, 0, k);
      off += k;
      if (k === c.length) this.chunks.shift();
      else this.chunks[0] = c.subarray(k);
    }
    return out;
  }
}

/**
 * Wraps a duplex socket with framed message send/receive. Both the client and
 * the broker use this so framing lives in exactly one place.
 *
 * Emits: 'message' (obj), 'close', 'error' (err).
 */
class Peer extends EventEmitter {
  constructor(socket, codec, opts = {}) {
    super();
    this.socket = socket;
    this.codec = codec;
    this.decoder = new FrameDecoder(codec, opts);
    // undefined = never drop / never disconnect. Limits apply only when configured.
    this.sendHighWaterMark = opts.sendHighWaterMark > 0 ? opts.sendHighWaterMark : undefined;
    this.sendHardLimit = opts.sendHardLimit > 0 ? opts.sendHardLimit : undefined;
    socket.on('data', (chunk) => {
      // Only a DECODE failure is a protocol error that kills the link. An exception thrown by a
      // 'message' listener is isolated to that one message, so a single bad handler can't tear
      // down the connection and drop the rest of the chunk. Frames are dispatched as they are
      // decoded, so a listener can change decoder limits (e.g. lift the pre-auth frame cap on
      // HELLO) before the next frame in the same chunk is decoded.
      try {
        this.decoder.push(chunk, (msg) => {
          try {
            this.emit('message', msg);
          } catch (err) {
            process.emitWarning(err instanceof Error ? err : new Error(String(err)), 'ProcMeshWarning');
          }
        });
      } catch (err) {
        this.emit('error', err);
        socket.destroy(err);
      }
    });
    socket.on('error', (err) => this.emit('error', err));
    socket.on('close', () => this.emit('close'));
  }

  /**
   * Send a framed message, applying High-Water-Mark backpressure when configured
   * (no limits by default — see sendHighWaterMark / sendHardLimit).
   *
   * - `droppable: true` (e.g. pub/sub fan-out): if the socket's outbound buffer
   *   already exceeds the soft HWM, the frame is DROPPED (returns 'dropped') so a
   *   slow consumer can't make us buffer without bound. Favors liveness.
   * - otherwise (replies/RPC): if the buffer exceeds the hard limit, the slow
   *   consumer is DISCONNECTED ('overflow') to protect the broker; below that it
   *   writes normally and returns socket.write()'s drain boolean.
   *
   * @returns {boolean|'dropped'|'overflow'}
   */
  send(obj, { droppable = false } = {}) {
    if (this.socket.destroyed) return false;
    const queued = this.socket.writableLength;
    if (droppable && this.sendHighWaterMark !== undefined && queued > this.sendHighWaterMark) {
      return 'dropped';
    }
    if (!droppable && this.sendHardLimit !== undefined && queued > this.sendHardLimit) {
      this.socket.destroy(new Error('send buffer overflow (slow consumer)'));
      return 'overflow';
    }
    return this.socket.write(encodeFrame(this.codec, obj));
  }

  /** Graceful close: flush queued writes, then FIN. */
  end() {
    this.socket.end();
  }

  destroy() {
    this.socket.destroy();
  }
}

module.exports = {
  PROTOCOL_VERSION,
  TYPES,
  encodeFrame,
  FrameDecoder,
  Peer,
  matchTopic,
  isPattern,
};
