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

const DEFAULT_MAX_FRAME = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_SEND_HWM = 16 * 1024 * 1024; // 16 MiB: soft cap, droppable frames dropped beyond this
const DEFAULT_SEND_HARD_LIMIT = 64 * 1024 * 1024; // 64 MiB: hard cap, slow consumer is disconnected

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

/** Incremental decoder that buffers partial reads and yields complete frames. */
class FrameDecoder {
  constructor(codec, { maxFrameSize = DEFAULT_MAX_FRAME } = {}) {
    this.codec = codec;
    this.maxFrameSize = maxFrameSize;
    this.buffer = Buffer.alloc(0);
  }

  push(chunk, onMessage) {
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    for (;;) {
      if (this.buffer.length < 4) return;
      const len = this.buffer.readUInt32BE(0);
      if (len > this.maxFrameSize) {
        throw new Error(`frame too large: ${len} > ${this.maxFrameSize}`);
      }
      if (this.buffer.length < 4 + len) return;
      const payload = this.buffer.subarray(4, 4 + len);
      const obj = this.codec.decode(payload);
      this.buffer = this.buffer.subarray(4 + len);
      onMessage(obj);
    }
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
    this.sendHighWaterMark = opts.sendHighWaterMark || DEFAULT_SEND_HWM;
    this.sendHardLimit = opts.sendHardLimit || DEFAULT_SEND_HARD_LIMIT;
    socket.on('data', (chunk) => {
      try {
        this.decoder.push(chunk, (msg) => this.emit('message', msg));
      } catch (err) {
        this.emit('error', err);
        socket.destroy(err);
      }
    });
    socket.on('error', (err) => this.emit('error', err));
    socket.on('close', () => this.emit('close'));
  }

  /**
   * Send a framed message, applying High-Water-Mark backpressure.
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
    if (droppable && queued > this.sendHighWaterMark) {
      return 'dropped';
    }
    if (!droppable && queued > this.sendHardLimit) {
      this.socket.destroy(new Error('send buffer overflow (slow consumer)'));
      return 'overflow';
    }
    return this.socket.write(encodeFrame(this.codec, obj));
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
