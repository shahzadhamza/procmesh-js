'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { fork } = require('child_process');
const { createClient, resolveAddress } = require('../src');
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
function spawnBroker(name, address, dir) {
  const child = fork(BROKER_BIN, [], {
    env: {
      ...process.env,
      PROCMESH_BROKER_OPTS: JSON.stringify({
        name,
        address,
        idleTimeout: 0,
        heartbeatInterval: 0,
        persist: { dir, mode: 'always' }, // synchronous fsync → deterministic across kill -9
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
