'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createBroker, createClient } = require('../src');
const Client = require('../src/client');
const { startBroker, client, delay, once, uniqueName } = require('./helpers');

let broker;
let pub;
let sub;

before(async () => {
  broker = await startBroker();
  pub = await client(broker);
  sub = await client(broker);
});

after(async () => {
  await pub.close();
  await sub.close();
  await broker.close();
});

test('acks:0 resolves immediately with undefined yet still delivers', async () => {
  const received = [];
  await sub.subscribe('a0', (p) => received.push(p));

  const result = await pub.publish('a0', 'hi', { acks: 0 });
  assert.strictEqual(result, undefined, 'fire-and-forget has no delivered count');

  await delay(30);
  assert.deepStrictEqual(received, ['hi']);
});

test('acks:1 (default) returns the delivered count — unchanged behavior', async () => {
  await sub.subscribe('a1', () => {});
  assert.strictEqual(await pub.publish('a1', 'x'), 1);
  assert.strictEqual(await pub.publish('a1', 'x', { acks: 1 }), 1);
});

test("acks:'all' delivers reliably with delivered === subscriberCount", async () => {
  const sub2 = await client(broker);
  try {
    await sub.subscribe('aall', () => {});
    await sub2.subscribe('aall', () => {});
    const delivered = await pub.publish('aall', 'y', { acks: 'all' });
    assert.strictEqual(delivered, 2);
  } finally {
    await sub2.close();
  }
});

test('a producer id is assigned by the broker on connect', () => {
  assert.ok(pub.pid, 'client should have a broker-assigned producer id');
});

test('idempotent producer: a retried (same seq) publish is delivered at most once', async () => {
  const idem = await client(broker, { pubsub: { idempotent: true } });
  try {
    const received = [];
    await sub.subscribe('idem', (p) => received.push(p));

    await idem.publish('idem', 'once'); // seq 1
    await delay(30);
    assert.deepStrictEqual(received, ['once']);

    // Simulate a producer retry of the same message: reuse the previous sequence number.
    idem._seq.set('idem', 0);
    const dup = await idem.publish('idem', 'once'); // seq 1 again → deduped
    assert.strictEqual(dup, null, 'a deduped retry reports null (distinct from 0 = no subscribers)');

    await delay(30);
    assert.deepStrictEqual(received, ['once'], 'the duplicate must not be re-delivered');

    const s = await idem.stats();
    assert.ok(s.duplicates >= 1, 'broker should count the dropped duplicate');
  } finally {
    await idem.close();
  }
});

test('producer id survives a reconnect so dedup keeps working', async () => {
  const c = await client(broker, { reconnect: true, pubsub: { idempotent: true } });
  try {
    const pidBefore = c.pid;
    assert.ok(pidBefore);
    c.peer.destroy(); // force a reconnect
    await once(c, 'reconnect');
    assert.strictEqual(c.pid, pidBefore, 'the client must re-present the same producer id');
  } finally {
    await c.close();
  }
});

test('dedup can be disabled broker-side (retries then double-deliver)', async () => {
  const b = await startBroker({ dedup: { enabled: false } });
  const p = await client(b, { pubsub: { idempotent: true } });
  const s = await client(b);
  try {
    const received = [];
    await s.subscribe('nod', (m) => received.push(m));
    await p.publish('nod', 'm'); // seq 1
    p._seq.set('nod', 0);
    await p.publish('nod', 'm'); // seq 1 again — NOT deduped (dedup off)
    await delay(40);
    assert.deepStrictEqual(received, ['m', 'm']);
  } finally {
    await p.close();
    await s.close();
    await b.close();
  }
});

test('idempotent producer: a legitimate gap seq is delivered, not mistaken for a duplicate', async () => {
  const idem = await client(broker, { pubsub: { idempotent: true } });
  try {
    const received = [];
    await sub.subscribe('gap', (p) => received.push(p));

    // seq 2 lands first (advances the highwater), then a *lower* seq 1 that never landed before.
    idem.setSequence('gap', 1);
    await idem.publish('gap', 'm2'); // seq 2
    idem.setSequence('gap', 0);
    const filled = await idem.publish('gap', 'm1'); // seq 1 — a gap-fill, must be delivered
    assert.strictEqual(filled, 1, 'the gap seq is a real delivery, not a dedup drop');

    // Now an exact duplicate of seq 1 → truly deduped.
    idem.setSequence('gap', 0);
    const dup = await idem.publish('gap', 'm1-again'); // seq 1 again
    assert.strictEqual(dup, null, 'an exact duplicate within the window is dropped');

    await delay(30);
    assert.deepStrictEqual(received, ['m2', 'm1'], 'gap delivered once; duplicate never delivered');
  } finally {
    await idem.close();
  }
});

test('reconnect catch-up: messages published while a subscriber is down are replayed by offset', async () => {
  const b = await startBroker({ pubsub: { persist: true } });
  const pub2 = await client(b);
  const s = await client(b, { reconnect: true });
  try {
    const received = [];
    await s.subscribe('evt', (m) => received.push(m));
    await pub2.publish('evt', 'a', { acks: 'all' });
    await delay(30);
    assert.deepStrictEqual(received, ['a']);

    // Drop the subscriber's link and publish while it's down.
    s.peer.destroy();
    await pub2.publish('evt', 'b', { acks: 'all' });
    await pub2.publish('evt', 'c', { acks: 'all' });
    await once(s, 'reconnect');
    await delay(40);

    assert.deepStrictEqual(received, ['a', 'b', 'c'], 'down-time messages caught up exactly once');
  } finally {
    await pub2.close();
    await s.close();
    await b.close();
  }
});

test('retention is bounded by maxChannels (no unbounded channel growth)', async () => {
  const b = await startBroker({ pubsub: { persist: true, maxChannels: 5 } });
  const p = await client(b);
  try {
    for (let i = 0; i < 12; i++) await p.publish(`ch.${i}`, i, { acks: 1 });
    assert.ok(b.pubRetention.size <= 5, `retained channels capped: ${b.pubRetention.size} <= 5`);
  } finally {
    await p.close();
    await b.close();
  }
});

test('an async subscriber handler that rejects surfaces as an error event', async () => {
  const received = once(sub, 'error');
  await sub.subscribe('boom', async () => {
    throw new Error('kaboom');
  });
  await pub.publish('boom', 'x');
  const [err] = await received;
  assert.match(err.message, /kaboom/);
});

test('publish retries wait for reconnection instead of exhausting the retry budget', async () => {
  const name = uniqueName();
  const opts = { name, idleTimeout: 0 };
  const b1 = createBroker(opts);
  await b1.start();
  const p = await createClient({ address: b1.address, autoSpawn: false, reconnect: true });
  try {
    await b1.close(); // broker goes down
    const inflight = p.publish('r', 'm', { idempotent: true, acks: 1, timeout: 4000 });
    const b2 = createBroker(opts); // bring it back on the same address
    await b2.start();
    const delivered = await inflight; // resolves after reconnect (0 subscribers)
    assert.strictEqual(delivered, 0);
    await b2.close();
  } finally {
    await p.close();
  }
});

test('cross-restart idempotency: a stable producerId + seeded sequence dedupes after a restart', async () => {
  const b = await startBroker();
  const s = await client(b);
  try {
    const received = [];
    await s.subscribe('cr', (m) => received.push(m));

    const c1 = await client(b, { pubsub: { producerId: 'p-fixed', idempotent: true } });
    assert.strictEqual(await c1.publish('cr', 'm1'), 1); // seq 1
    await c1.close();

    // A "restarted" producer: same id, sequence seeded to just-below the last one it sent.
    const c2 = await client(b, { pubsub: { producerId: 'p-fixed', idempotent: true, sequences: { cr: 0 } } });
    const dup = await c2.publish('cr', 'm1'); // seq 1 again → deduped
    assert.strictEqual(dup, null, 'the pre-restart message is recognized as a duplicate');
    assert.strictEqual(await c2.publish('cr', 'm2'), 1); // seq 2 → delivered
    await c2.close();

    await delay(30);
    assert.deepStrictEqual(received, ['m1', 'm2']);
  } finally {
    await s.close();
    await b.close();
  }
});

test('pub/sub persistence: replay-on-subscribe returns retained messages (in-memory)', async () => {
  const b = await startBroker({ pubsub: { persist: true, retention: 100 } });
  const p = await client(b);
  const late = await client(b);
  try {
    await p.publish('orders', { id: 1 }, { acks: 'all' });
    await p.publish('orders', { id: 2 }, { acks: 'all' });
    await delay(20);

    // A brand-new subscriber that opts into replay sees the retained backlog.
    const replayed = [];
    await late.subscribe('orders', (m) => replayed.push(m), { replay: true });
    await delay(30);
    assert.deepStrictEqual(replayed, [{ id: 1 }, { id: 2 }]);

    // Without replay, only live messages are delivered.
    const liveOnly = [];
    const other = await client(b);
    await other.subscribe('orders', (m) => liveOnly.push(m));
    await delay(20);
    assert.deepStrictEqual(liveOnly, [], 'no replay opt-in → no backlog');
    await other.close();
  } finally {
    await p.close();
    await late.close();
    await b.close();
  }
});

test('unsubscribe then re-subscribe with replay delivers the retained backlog', async () => {
  const b = await startBroker({ pubsub: { persist: true, retention: 100 } });
  const p = await client(b);
  const s = await client(b);
  try {
    await p.publish('re', 'a', { acks: 'all' });
    await p.publish('re', 'b', { acks: 'all' });

    // First subscription consumes the backlog live, advancing the offset watermark.
    const first = [];
    const off = await s.subscribe('re', (m) => first.push(m), { replay: true });
    await delay(30);
    assert.deepStrictEqual(first, ['a', 'b']);
    await off();

    // Re-subscribing with replay must see the backlog again — not be silently suppressed by the
    // stale offset watermark left over from the first subscription.
    const second = [];
    await s.subscribe('re', (m) => second.push(m), { replay: true });
    await delay(30);
    assert.deepStrictEqual(second, ['a', 'b'], 'replay must not be suppressed by a stale offset watermark');
  } finally {
    await p.close();
    await s.close();
    await b.close();
  }
});

test('subscribe issued before connect still replays the backlog after connecting', async () => {
  const b = await startBroker({ pubsub: { persist: true, retention: 100 } });
  const p = await client(b);
  const s = new Client({ address: b.address, autoSpawn: false, reconnect: false });
  try {
    await p.publish('pre', 'x', { acks: 'all' });
    await p.publish('pre', 'y', { acks: 'all' });

    const got = [];
    // Subscribe BEFORE connecting — the SUBSCRIBE is queued locally, carrying its replay request.
    await s.subscribe('pre', (m) => got.push(m), { replay: true });
    await s.connect();
    await delay(40);
    assert.deepStrictEqual(got, ['x', 'y'], 'a replay requested while offline is honored on connect');
  } finally {
    await p.close();
    await s.close();
    await b.close();
  }
});

test('retention:0 retains nothing (a replay subscriber sees an empty backlog)', async () => {
  const b = await startBroker({ pubsub: { persist: true, retention: 0 } });
  const p = await client(b);
  const late = await client(b);
  try {
    await p.publish('r0', 'a', { acks: 'all' });
    await p.publish('r0', 'b', { acks: 'all' });
    await delay(20);
    assert.strictEqual(b.pubRetention.size, 0, 'nothing retained when retention is 0');

    const replayed = [];
    await late.subscribe('r0', (m) => replayed.push(m), { replay: true });
    await delay(30);
    assert.deepStrictEqual(replayed, [], 'retention:0 → no backlog to replay');
  } finally {
    await p.close();
    await late.close();
    await b.close();
  }
});

test('pub/sub persistence: retained messages survive a broker restart (durable acks)', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'procmesh-pubsub-'));
  const name = uniqueName();
  const opts = { name, idleTimeout: 0, persist: { dir, mode: 'always' }, pubsub: { persist: true, durableAcks: 'all' } };

  const b1 = createBroker(opts);
  await b1.start();
  const p = await createClient({ address: b1.address, autoSpawn: false, reconnect: false });
  await p.publish('events', 'boot', { acks: 'all' }); // durable: fsync before ack
  await p.close();
  await b1.close(); // clean shutdown flushes snapshot + AOF

  // Fresh broker, same dir/name → recovers the retained pub log.
  const b2 = createBroker(opts);
  await b2.start();
  const c = await createClient({ address: b2.address, autoSpawn: false, reconnect: false });
  try {
    const got = [];
    await c.subscribe('events', (m) => got.push(m), { replay: true });
    await delay(40);
    assert.deepStrictEqual(got, ['boot'], 'the pre-restart message should replay');
  } finally {
    await c.close();
    await b2.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
