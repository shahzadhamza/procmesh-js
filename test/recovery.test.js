'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { fork } = require('child_process');
const { createClient, createBroker, resolveAddress } = require('../src');
const { Persistence, NullPersistence } = require('../src/persistence');
const Store = require('../src/store');
const { jsonCodec } = require('../src/codec');
const { encodeFrame } = require('../src/protocol');

const BROKER_BIN = path.join(__dirname, '..', 'src', 'broker-bin.js');
const delay = (ms) => new Promise((r) => setTimeout(r, ms));

function tmpDir() {
  const d = path.join(os.tmpdir(), `procmesh-rec-${process.pid}-${Math.random().toString(36).slice(2, 8)}`);
  fs.mkdirSync(d, { recursive: true });
  return d;
}

/** Fork a real broker process with persistence; resolve once it reports ready. */
function spawnBroker(name, address, dir, extra = {}) {
  const child = fork(BROKER_BIN, [], {
    env: {
      ...process.env,
      PROCMESH_BROKER_OPTS: JSON.stringify({
        name,
        address,
        idleTimeout: 0,
        heartbeatInterval: 0,
        persist: { dir, mode: 'always' }, // synchronous fsync → deterministic across kill -9
        ...extra,
      }),
    },
  });
  return new Promise((resolve, reject) => {
    child.once('message', (m) => (m === 'ready' ? resolve(child) : reject(new Error(`unexpected: ${m}`))));
    child.once('error', reject);
  });
}

test('cache + counter survive kill -9; TTL honored; locks released', async () => {
  const name = `rec-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const address = resolveAddress(name);
  const dir = tmpDir();

  let broker = await spawnBroker(name, address, dir);
  let c = await createClient({ address, autoSpawn: false, reconnect: false });

  await c.set('keep', { hello: 'world' });
  await c.incr('counter', 5);
  await c.set('cfg', 'v1');
  await c.cas('cfg', 'v1', 'v2');
  await c.set('short', 'gone-soon', { ttl: 80 });
  await c.set('long', 'still-here', { ttl: 60000 });
  // Hold a lock that must NOT survive the crash.
  const rel = await c.lock('joblock', { ttl: 60000, wait: 0 });
  assert.ok(rel);

  await c.close();
  broker.kill('SIGKILL');
  await delay(150); // let the short-TTL key expire and the OS reap the process

  broker = await spawnBroker(name, address, dir);
  c = await createClient({ address, autoSpawn: false, reconnect: false });
  try {
    assert.deepStrictEqual(await c.get('keep'), { hello: 'world' }, 'value survived');
    assert.strictEqual(await c.get('counter'), 5, 'counter survived');
    assert.strictEqual(await c.get('cfg'), 'v2', 'cas effect survived');
    assert.strictEqual(await c.get('short'), undefined, 'short-TTL key expired across restart');
    assert.strictEqual(await c.get('long'), 'still-here', 'long-TTL key survived');

    // Lock state did NOT survive — a fresh client acquires immediately.
    const rel2 = await c.lock('joblock', { wait: 0 });
    assert.ok(rel2, 'locks are released on restart');
    await rel2();
  } finally {
    await c.close();
    broker.kill('SIGTERM');
  }
});

test('torn AOF tail recovers the valid prefix without throwing', async () => {
  const dir = tmpDir();
  // Author an AOF by hand: two good records, then a truncated frame.
  const good = Buffer.concat([
    encodeFrame(jsonCodec, { op: 'set', k: 'a', v: 1, e: 0 }),
    encodeFrame(jsonCodec, { op: 'set', k: 'b', v: 2, e: 0 }),
  ]);
  const torn = encodeFrame(jsonCodec, { op: 'set', k: 'c', v: 3, e: 0 }).subarray(0, 5); // partial
  fs.writeFileSync(path.join(dir, 'aof.bin'), Buffer.concat([good, torn]));

  const store = new Store({});
  const p = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  await p.load(store);
  assert.strictEqual(store.get('a'), 1);
  assert.strictEqual(store.get('b'), 2);
  assert.strictEqual(store.get('c'), undefined, 'torn final record dropped');
  await p.flushAndClose();
});

test('corrupt snapshot falls back to AOF instead of refusing to start', async () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, 'snapshot.bin'), Buffer.from('not a valid snapshot'));
  fs.writeFileSync(path.join(dir, 'aof.bin'), encodeFrame(jsonCodec, { op: 'set', k: 'x', v: 42, e: 0 }));

  const store = new Store({});
  const p = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  await p.load(store); // must not throw
  assert.strictEqual(store.get('x'), 42, 'recovered from AOF despite corrupt snapshot');
  await p.flushAndClose();
});

test('fence counter is restored monotonically across restart', async () => {
  const dir = tmpDir();
  const store = new Store({});
  const p1 = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  await p1.load(store);
  // Simulate the broker minting tokens past a block boundary.
  for (let n = 1; n <= TOKEN_PAST_BLOCK; n++) p1.noteToken(n);
  await p1.flushAndClose();

  const p2 = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  await p2.load(new Store({}));
  assert.ok(p2.loadedToken >= TOKEN_PAST_BLOCK, `restored seed ${p2.loadedToken} >= ${TOKEN_PAST_BLOCK}`);
  await p2.flushAndClose();
});

const TOKEN_PAST_BLOCK = 1100; // crosses the 1024 token-reservation block

test('an incr that preserves a TTL also persists the remaining TTL (not 0)', async () => {
  const dir = tmpDir();
  const store = new Store({});
  const p = new Persistence({ dir, mode: 'always', codec: jsonCodec });
  await p.load(store);

  // Mirror what the broker does for SET then INCR: log the *remaining* TTL each time (absolute `e`).
  const log = (k, v) => {
    const rem = store.remainingTTL(k);
    p.logMutation({ op: 'set', k, v, e: rem > 0 ? Date.now() + rem : 0 });
  };
  store.set('n', 5, 60000);
  log('n', 5);
  assert.strictEqual(store.incr('n', 1), 6);
  log('n', 6); // logs the preserved remaining TTL, not 0
  await p.flushAndClose();

  const store2 = new Store({});
  const p2 = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  await p2.load(store2);
  assert.strictEqual(store2.get('n'), 6, 'incremented value recovered');
  assert.ok(store2.remainingTTL('n') > 0, 'TTL survived the reload (was not wiped to 0)');
  await p2.flushAndClose();
});

test('NullPersistence is a no-op and reports loadedToken 0', async () => {
  const p = new NullPersistence();
  await p.load(new Store({}));
  p.logMutation({ op: 'set', k: 'a', v: 1 });
  p.noteToken(5);
  assert.strictEqual(p.loadedToken, 0);
  await p.flushAndClose();
});

test('a superseded pre-crash fencing token is rejected after a broker restart', async () => {
  const name = `fence-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const address = resolveAddress(name);
  const dir = tmpDir();
  const opts = { name, address, idleTimeout: 0, heartbeatInterval: 0, persist: { dir, mode: 'always' } };

  let broker = await createBroker(opts).start();
  let a = await createClient({ address, autoSpawn: false, reconnect: false });
  let b = await createClient({ address, autoSpawn: false, reconnect: false });
  const relA = await a.lock('acct', { ttl: 50, wait: 0 });
  await delay(100); // A overruns its TTL
  const relB = await b.lock('acct', { wait: 0 });
  assert.strictEqual(await b.fencedSet('acct', relB.token, 'bal', 'from-B'), true);
  await a.close();
  await b.close();
  await broker.close();

  broker = await createBroker(opts).start();
  a = await createClient({ address, autoSpawn: false, reconnect: false });
  try {
    await assert.rejects(() => a.fencedSet('acct', relA.token, 'bal', 'stale-A'), (e) => e.code === 'EFENCED');
    assert.strictEqual(await a.get('bal'), 'from-B', 'stale write did not land');
    const rel = await a.lock('acct', { wait: 0 });
    assert.ok(rel.token > relB.token, 'new grants still outrank every pre-crash token');
    assert.strictEqual(await a.fencedSet('acct', rel.token, 'bal', 'fresh'), true);
    await rel();
  } finally {
    await a.close();
    await broker.close();
  }
});

test('acked retained messages survive kill -9 right after an AOF compaction, without duplicates', async () => {
  const name = `pubc-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const address = resolveAddress(name);
  const dir = tmpDir();
  const extra = { persist: { dir, mode: 'no', aofRewriteOps: 5 }, pubsub: { persist: true } };

  let broker = await spawnBroker(name, address, dir, extra);
  let c = await createClient({ address, autoSpawn: false, reconnect: false });
  for (let i = 1; i <= 12; i++) await c.publish('orders', { i }, { acks: 'all' }); // compacts twice
  await c.close();
  broker.kill('SIGKILL');
  await delay(100);

  for (let round = 0; round < 2; round++) {
    // Restart twice: recovered records must not be re-retained on each boot.
    broker = await spawnBroker(name, address, dir, extra);
    c = await createClient({ address, autoSpawn: false, reconnect: false });
    const got = [];
    await c.subscribe('orders', (m) => got.push(m.i), { replay: true });
    await delay(100);
    await c.close();
    broker.kill('SIGKILL');
    await delay(100);
    assert.deepStrictEqual(got, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], `round ${round}`);
  }
});

test('a recovered pub record raises the offset seed past a lost reservation', async () => {
  const dir = tmpDir();
  // An fsync'd pub record whose covering offset reservation never made it to disk.
  fs.writeFileSync(path.join(dir, 'aof.bin'), encodeFrame(jsonCodec, { op: 'pub', ch: 'c', payload: 1, ts: Date.now(), offset: 5000 }));
  const name = `off-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const broker = await createBroker({ name, idleTimeout: 0, persist: { dir, mode: 'no' }, pubsub: { persist: true } }).start();
  try {
    assert.ok(broker.nextOffset >= 5000, `seed ${broker.nextOffset} covers the recovered offset`);
  } finally {
    await broker.close();
  }
});

test('a graceful restart never rolls a key back to an older value', async () => {
  const name = `roll-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const dir = tmpDir();
  const opts = { name, idleTimeout: 0, heartbeatInterval: 0, persist: { dir, mode: 'no' } };
  let broker = await createBroker(opts).start();
  let c = await createClient({ address: broker.address, autoSpawn: false, reconnect: false });
  await Promise.all(Array.from({ length: 300 }, (_, i) => c.set('k', i)));
  await c.close();
  await broker.close();

  broker = await createBroker(opts).start();
  c = await createClient({ address: broker.address, autoSpawn: false, reconnect: false });
  try {
    assert.strictEqual(await c.get('k'), 299);
  } finally {
    await c.close();
    await broker.close();
  }
});

test('idempotent dedup is rebuilt from retained messages after a restart', async () => {
  const name = `dd-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const dir = tmpDir();
  const opts = { name, idleTimeout: 0, heartbeatInterval: 0, persist: { dir, mode: 'always' }, pubsub: { persist: true } };
  const producer = { pubsub: { producerId: 'stable-p', idempotent: true } };
  let broker = await createBroker(opts).start();
  let c = await createClient({ address: broker.address, autoSpawn: false, reconnect: false, ...producer });
  assert.strictEqual(await c.publish('jobs', 'j1'), 0);
  await c.close();
  await broker.close();

  broker = await createBroker(opts).start();
  c = await createClient({ address: broker.address, autoSpawn: false, reconnect: false, ...producer });
  try {
    c.setSequence('jobs', 0); // the retry re-sends seq 1 after the restart
    assert.strictEqual(await c.publish('jobs', 'j1'), null, 'recognized as a duplicate');
    const got = [];
    await c.subscribe('jobs', (m) => got.push(m), { replay: true });
    await delay(50);
    assert.deepStrictEqual(got, ['j1'], 'retained once');
  } finally {
    await c.close();
    await broker.close();
  }
});

test('corrupt snapshot is preserved on disk and reported', async () => {
  const dir = tmpDir();
  fs.writeFileSync(path.join(dir, 'snapshot.bin'), Buffer.from('garbage'));
  const store = new Store({});
  const p = new Persistence({ dir, mode: 'no', codec: jsonCodec });
  const errs = [];
  p.onError = (e) => errs.push(e);
  await p.load(store);
  await p.flushAndClose();
  assert.ok(fs.readdirSync(dir).some((f) => f.startsWith('snapshot.bin.corrupt-')), 'kept aside');
  assert.ok(errs.some((e) => e.code === 'EPERSISTCORRUPT'));
});

test('replay honors retentionMs even on a channel that went quiet', async () => {
  const name = `rms-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const broker = await createBroker({ name, idleTimeout: 0, pubsub: { persist: true, retentionMs: 50 } }).start();
  const c = await createClient({ address: broker.address, autoSpawn: false, reconnect: false });
  try {
    await c.publish('quiet', 'stale');
    await delay(100);
    const got = [];
    await c.subscribe('quiet', (m) => got.push(m), { replay: true });
    await delay(30);
    assert.deepStrictEqual(got, [], 'expired message not replayed');
  } finally {
    await c.close();
    await broker.close();
  }
});

test('two brokers racing on a stale socket file never both listen', { skip: process.platform === 'win32' }, async () => {
  const name = `race-${process.pid}-${Math.random().toString(36).slice(2, 7)}`;
  const address = resolveAddress(name);
  fs.writeFileSync(address, ''); // a stale non-socket entry at the address
  const results = await Promise.allSettled([0, 1, 2].map(() => createBroker({ name, address, idleTimeout: 0 }).start()));
  const up = results.filter((r) => r.status === 'fulfilled').map((r) => r.value);
  try {
    assert.strictEqual(up.length, 1, 'exactly one broker owns the address');
  } finally {
    await Promise.all(up.map((b) => b.close()));
  }
});
