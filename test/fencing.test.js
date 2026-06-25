'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay } = require('./helpers');

let broker;

before(async () => {
  broker = await startBroker();
});

after(async () => {
  await broker.close();
});

test('stale holder is fenced after TTL overrun while the new holder succeeds', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    const relA = await a.lock('job', { ttl: 60, wait: 0 });
    assert.ok(relA, 'A acquires');
    const tokenA = relA.token;
    assert.ok(typeof tokenA === 'number', 'grant carries a numeric token');

    await delay(120); // A's critical section overruns its TTL → lock auto-releases

    const relB = await b.lock('job', { wait: 0 });
    assert.ok(relB, 'B acquires after A’s TTL expiry');
    const tokenB = relB.token;
    assert.ok(tokenB > tokenA, 'B’s token is strictly greater');

    // A still thinks it holds the lock; its fenced write must be rejected.
    await assert.rejects(
      () => a.fencedSet('job', tokenA, 'state', 'from-A'),
      (e) => e.code === 'EFENCED'
    );

    // B's current token is accepted.
    assert.strictEqual(await b.fencedSet('job', tokenB, 'state', 'from-B'), true);
    assert.strictEqual(await a.get('state'), 'from-B');

    await relB();
  } finally {
    await a.close();
    await b.close();
  }
});

test('sequential grants of the same key yield strictly increasing tokens', async () => {
  const c = await client(broker);
  try {
    const r1 = await c.lock('mono', { wait: 0 });
    const t1 = r1.token;
    await r1();
    const r2 = await c.lock('mono', { wait: 0 });
    const t2 = r2.token;
    await r2();
    assert.ok(t2 > t1, 'token increases across re-acquire');
  } finally {
    await c.close();
  }
});

test('fencedCas honors both CAS semantics and the fence', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    const relA = await a.lock('cfg-lock', { ttl: 60, wait: 0 });
    await a.fencedSet('cfg-lock', relA.token, 'cfg', 'v1');

    await delay(120); // A expires
    const relB = await b.lock('cfg-lock', { wait: 0 });

    // A's CAS is fenced even though prev matches.
    await assert.rejects(
      () => a.fencedCas('cfg-lock', relA.token, 'cfg', 'v1', 'v2-from-A'),
      (e) => e.code === 'EFENCED'
    );
    // B's CAS with current token and matching prev succeeds.
    assert.strictEqual(await b.fencedCas('cfg-lock', relB.token, 'cfg', 'v1', 'v2-from-B'), true);
    assert.strictEqual(await a.get('cfg'), 'v2-from-B');
    await relB();
  } finally {
    await a.close();
    await b.close();
  }
});

test('withLock ctx exposes a fenced set/cas/del', async () => {
  const c = await client(broker);
  try {
    const out = await c.withLock('wl', async ({ token, set, cas }) => {
      assert.ok(typeof token === 'number');
      assert.strictEqual(await set('wlk', 1), true);
      assert.strictEqual(await cas('wlk', 1, 2), true);
      return 'done';
    });
    assert.strictEqual(out, 'done');
    assert.strictEqual(await c.get('wlk'), 2);
  } finally {
    await c.close();
  }
});

test('a never-locked key accepts the first legitimate token', async () => {
  const c = await client(broker);
  try {
    const rel = await c.lock('fresh', { wait: 0 });
    assert.strictEqual(await c.fencedSet('fresh', rel.token, 'fk', 'ok'), true);
    await rel();
  } finally {
    await c.close();
  }
});
