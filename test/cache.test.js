'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay } = require('./helpers');

let broker;
let a;
let b;

before(async () => {
  broker = await startBroker({ cache: { max: 3 } });
  a = await client(broker);
  b = await client(broker);
});

after(async () => {
  await a.close();
  await b.close();
  await broker.close();
});

test('value set by one client is visible to another', async () => {
  await a.set('user:1', { name: 'Ada', roles: ['admin'] });
  assert.deepStrictEqual(await b.get('user:1'), { name: 'Ada', roles: ['admin'] });
});

test('get returns undefined for a missing key', async () => {
  assert.strictEqual(await b.get('nope'), undefined);
  assert.strictEqual(await b.has('nope'), false);
});

test('del removes a key', async () => {
  await a.set('temp', 42);
  assert.strictEqual(await b.has('temp'), true);
  assert.strictEqual(await a.del('temp'), true);
  assert.strictEqual(await b.has('temp'), false);
});

test('per-item TTL expires the value', async () => {
  await a.set('ephemeral', 'soon-gone', { ttl: 60 });
  assert.strictEqual(await b.get('ephemeral'), 'soon-gone');
  await delay(120);
  assert.strictEqual(await b.get('ephemeral'), undefined);
});

test('LRU eviction respects max size', async () => {
  await a.clear();
  await a.set('k1', 1);
  await a.set('k2', 2);
  await a.set('k3', 3);
  await a.set('k4', 4); // exceeds max:3 -> oldest (k1) evicted
  const keys = await b.keys();
  assert.strictEqual(keys.length, 3);
  assert.strictEqual(await b.has('k1'), false);
  assert.strictEqual(await b.has('k4'), true);
});

test('mget / mset operate in bulk', async () => {
  await a.clear();
  await a.mset({ x: 1, y: 2, z: 3 });
  assert.deepStrictEqual(await b.mget(['x', 'y', 'missing', 'z']), [1, 2, undefined, 3]);
});

test('incr preserves an existing per-item TTL (does not make the key immortal)', async () => {
  await a.clear();
  await a.set('counter', 5, { ttl: 120 });
  assert.strictEqual(await a.incr('counter', 1), 6); // read-modify-write must keep the TTL
  assert.strictEqual(await b.get('counter'), 6);
  await delay(180);
  assert.strictEqual(await b.get('counter'), undefined, 'TTL still fired after incr');
});
