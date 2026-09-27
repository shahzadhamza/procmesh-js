'use strict';

// No limits unless configured: the store, dedup map, retention, and per-connection
// send/frame caps are all unbounded by default, and explicit limits still enforce.

const { test } = require('node:test');
const assert = require('node:assert');
const { LRUCache } = require('lru-cache');
const { startBroker, client, delay } = require('./helpers');
const Store = require('../src/store');
const { FrameDecoder, Peer } = require('../src/protocol');

const jsonCodec = {
  encode: (obj) => Buffer.from(JSON.stringify(obj)),
  decode: (buf) => JSON.parse(buf.toString()),
};

// Collect process warnings so we can assert the ttl-only lru-cache constructions
// don't emit LRU_CACHE_UNBOUNDED (warnings arrive on a later tick).
const warnings = [];
process.on('warning', (w) => warnings.push(w));

// ---------------------------------------------------------------------- store

test('a store with no bounds accepts more keys than the old 10k default', () => {
  const s = new Store({});
  assert.ok(!(s.cache instanceof LRUCache), 'no-bounds store uses the Map fallback');
  for (let i = 0; i < 10050; i++) s.set(`k${i}`, i);
  assert.strictEqual(s.size, 10050);
  assert.strictEqual(s.get('k0'), 0, 'nothing evicted');
  assert.strictEqual(s.keys().length, 10050);
});

test('per-item TTL expires on the unbounded path', async () => {
  const s = new Store({});
  s.set('ephemeral', 'soon-gone', 60);
  assert.strictEqual(s.get('ephemeral'), 'soon-gone');
  assert.ok(s.remainingTTL('ephemeral') > 0);
  await delay(120);
  assert.strictEqual(s.get('ephemeral'), undefined);
  assert.strictEqual(s.has('ephemeral'), false);
  assert.ok(!s.keys().includes('ephemeral'));
  assert.strictEqual(s.remainingTTL('ephemeral'), 0);
});

test('a plain set over a TTLd key clears the TTL (matches lru-cache)', async () => {
  const s = new Store({});
  s.set('k', 'a', 60);
  s.set('k', 'b'); // no ttl -> entry becomes immortal
  assert.strictEqual(s.remainingTTL('k'), 0, '0 means "no expiry" through Store.remainingTTL');
  await delay(140);
  assert.strictEqual(s.get('k'), 'b');
});

test('incr/cas preserve a live TTL on the unbounded path', async () => {
  const s = new Store({});
  s.set('counter', 5, 80);
  assert.strictEqual(s.incr('counter', 1), 6);
  s.set('flag', 'a', 80);
  assert.strictEqual(s.cas('flag', 'a', 'b'), true);
  assert.strictEqual(s.get('flag'), 'b');
  await delay(160);
  assert.strictEqual(s.get('counter'), undefined, 'TTL still fired after incr');
  assert.strictEqual(s.get('flag'), undefined, 'TTL still fired after cas');
});

test('incr on a missing key creates an immortal counter', () => {
  const s = new Store({});
  assert.strictEqual(s.incr('n'), 1);
  assert.strictEqual(s.remainingTTL('n'), 0);
});

test('dump/load round-trips no-TTL and live-TTL entries, skips expired ones', async () => {
  const s = new Store({});
  s.set('immortal', 'v');
  s.set('mortal', 'w', 5000);
  s.set('dead', 'x', 30);
  await delay(80);

  const entries = s.dump();
  const byKey = Object.fromEntries(entries.map((e) => [e.k, e]));
  assert.strictEqual(byKey.dead, undefined, 'expired entry not dumped');
  assert.strictEqual(byKey.immortal.e, 0, 'no expiry dumps as e:0');
  assert.ok(byKey.mortal.e > Date.now(), 'live TTL dumps as an absolute future expiry');

  const s2 = new Store({});
  s2.load(entries);
  assert.strictEqual(s2.get('immortal'), 'v');
  assert.strictEqual(s2.get('mortal'), 'w');
  assert.strictEqual(s2.remainingTTL('immortal'), 0);
  const remaining = s2.remainingTTL('mortal');
  assert.ok(remaining > 0 && remaining <= 5000, `restored TTL keeps counting down (${remaining})`);
});

test('a ttl-only cache config still evicts (bounded lru-cache path)', async () => {
  const s = new Store({ ttl: 60 });
  assert.ok(s.cache instanceof LRUCache, 'any configured bound keeps lru-cache');
  s.set('k', 'v');
  assert.strictEqual(s.get('k'), 'v');
  await delay(150);
  assert.strictEqual(s.get('k'), undefined);
});

test('a maxSize (byte budget) cache config still uses lru-cache', () => {
  const s = new Store({ maxSize: 10_000 });
  assert.ok(s.cache instanceof LRUCache);
  s.set('k', { a: 1 });
  assert.deepStrictEqual(s.get('k'), { a: 1 });
});

// --------------------------------------------------------------------- broker

test('a default broker is unbounded: Map-backed store, dedup, and retention', async () => {
  const b = await startBroker();
  try {
    assert.ok(!(b.store.cache instanceof LRUCache), 'store: Map fallback');
    assert.ok(b.dedup instanceof Map, 'dedup: plain Map');
    assert.ok(b.pubRetention instanceof Map, 'retention: plain Map');
  } finally {
    await b.close();
  }
});

test('dedup.max still caps dedup entries with an LRU', async () => {
  const b = await startBroker({ dedup: { max: 2 } });
  try {
    assert.ok(b.dedup instanceof LRUCache);
    b._dedupEntry('1 a');
    b._dedupEntry('2 b');
    b._dedupEntry('3 c');
    assert.ok(b.dedup.size <= 2, `oldest entry evicted (size ${b.dedup.size})`);
  } finally {
    await b.close();
  }
});

test('a ttl-only dedup config uses lru-cache without an LRU_CACHE_UNBOUNDED warning', async () => {
  const b = await startBroker({ dedup: { ttl: 50 } });
  try {
    assert.ok(b.dedup instanceof LRUCache);
    b._dedupEntry('1 a');
    await delay(120); // let the ttl fire and any process warning arrive
    assert.strictEqual(b.dedup.get('1 a'), undefined, 'dedup state ages out');
    const unbounded = warnings.filter((w) => w.code === 'LRU_CACHE_UNBOUNDED');
    assert.deepStrictEqual(unbounded, []);
  } finally {
    await b.close();
  }
});

test('persist-on retention is unlimited by default (no 1000-per-channel trim)', async () => {
  const b = await startBroker({ pubsub: { persist: true } });
  const p = await client(b);
  try {
    await Promise.all(Array.from({ length: 1050 }, (_, i) => p.publish('bulk', i, { acks: 1 })));
    assert.strictEqual(b.pubRetention.get('bulk').length, 1050);
  } finally {
    await p.close();
    await b.close();
  }
});

test('pubsub.retention still trims the per-channel ring', async () => {
  const b = await startBroker({ pubsub: { persist: true, retention: 5 } });
  const p = await client(b);
  try {
    for (let i = 0; i < 8; i++) await p.publish('ring', i, { acks: 1 });
    const ring = b.pubRetention.get('ring');
    assert.strictEqual(ring.length, 5);
    assert.strictEqual(ring[0].payload, 3, 'oldest messages trimmed');
  } finally {
    await p.close();
    await b.close();
  }
});

// ------------------------------------------------------------------- protocol

function stubSocket(writableLength) {
  return {
    destroyed: false,
    writableLength,
    destroyErr: null,
    write() {
      return true;
    },
    destroy(err) {
      this.destroyed = true;
      this.destroyErr = err;
    },
    on() {},
  };
}

test('FrameDecoder has no frame-size cap unless configured', () => {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(0xfffffff0, 0); // advertise a ~4 GiB frame

  const unbounded = new FrameDecoder(jsonCodec);
  unbounded.push(header, () => assert.fail('no complete frame expected')); // no throw: waits for data

  const capped = new FrameDecoder(jsonCodec, { maxFrameSize: 1024 });
  assert.throws(() => capped.push(header, () => {}), /frame too large/);
});

test('Peer.send never drops or disconnects unless limits are configured', () => {
  const hugeBacklog = 1 << 30;

  const unbounded = new Peer(stubSocket(hugeBacklog), jsonCodec);
  assert.strictEqual(unbounded.send({ t: 'msg' }, { droppable: true }), true);
  assert.strictEqual(unbounded.send({ t: 'result' }), true);

  const soft = new Peer(stubSocket(hugeBacklog), jsonCodec, { sendHighWaterMark: 1024 });
  assert.strictEqual(soft.send({ t: 'msg' }, { droppable: true }), 'dropped');

  const hardSocket = stubSocket(hugeBacklog);
  const hard = new Peer(hardSocket, jsonCodec, { sendHardLimit: 1024 });
  assert.strictEqual(hard.send({ t: 'result' }), 'overflow');
  assert.ok(hardSocket.destroyed, 'slow consumer disconnected when the limit is set');
});

test('maxFrameSize is plumbed to the client peer and enforced broker-side', async () => {
  const b = await startBroker({ maxFrameSize: 256 });
  const c = await client(b, { maxFrameSize: 1234 });
  try {
    assert.strictEqual(c.peer.decoder.maxFrameSize, 1234, 'client option reaches its decoder');
    // A frame over the broker's cap kills the connection (reconnect is off in tests).
    await assert.rejects(c.set('big', 'x'.repeat(2048)), /connection to broker lost/);
  } finally {
    await c.close();
    await b.close();
  }
});

test('unbounded store purges expired TTL entries that are never touched again', async () => {
  const s = new Store({});
  for (let i = 0; i < 500; i++) s.set(`sess${i}`, i, 30);
  s.set('forever', 1);
  assert.strictEqual(s.size, 501);
  await delay(600); // > ttl + a couple of sweep ticks
  assert.strictEqual(s.size, 1, 'expired entries swept without being read');
  assert.strictEqual(s.cache.ttlCount, 0);
  assert.strictEqual(s.cache._sweepTimer, null, 'sweep stops once no TTL entries remain');
  assert.strictEqual(s.get('forever'), 1);
  s.close();
});

test('FrameDecoder decodes a large frame split into many chunks in linear time', () => {
  const { encodeFrame } = require('../src/protocol');
  const big = 'x'.repeat(32 * 1024 * 1024);
  const frame = encodeFrame(jsonCodec, { big });
  const dec = new FrameDecoder(jsonCodec);
  const out = [];
  const started = Date.now();
  for (let i = 0; i < frame.length; i += 16 * 1024) dec.push(frame.subarray(i, i + 16 * 1024), (m) => out.push(m));
  assert.strictEqual(out.length, 1);
  assert.strictEqual(out[0].big.length, big.length);
  // The old concat-per-chunk decoder copied ~32GB here; linear decoding is well under a second.
  assert.ok(Date.now() - started < 3000, `took ${Date.now() - started}ms`);
});

test('FrameDecoder handles headers and frames straddling 1-byte chunks', () => {
  const { encodeFrame } = require('../src/protocol');
  const frames = Buffer.concat([1, 2, 3].map((n) => encodeFrame(jsonCodec, { n })));
  const dec = new FrameDecoder(jsonCodec);
  const out = [];
  for (let i = 0; i < frames.length; i++) dec.push(frames.subarray(i, i + 1), (m) => out.push(m.n));
  assert.deepStrictEqual(out, [1, 2, 3]);
  assert.strictEqual(dec.length, 0);
});

test('CAS equality ignores key order and distinguishes Maps', () => {
  const s = new Store({});
  s.set('o', { a: 1, b: { c: [1, 2] } });
  assert.strictEqual(s.cas('o', { b: { c: [1, 2] }, a: 1 }, 'next'), true, 'key order is irrelevant');
  s.set('m', new Map([['k', 1]]));
  assert.strictEqual(s.cas('m', new Map([['k', 2]]), 'bad'), false, 'different Maps are not equal');
  assert.strictEqual(s.cas('m', new Map([['k', 1]]), 'good'), true);
  assert.strictEqual(s.cas('missing', null, 'created'), true, 'absent still matches null');
});
