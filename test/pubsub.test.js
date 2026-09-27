'use strict';

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const { startBroker, client, delay } = require('./helpers');

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

test('subscriber receives messages published by another client', async () => {
  const received = [];
  await sub.subscribe('news', (payload) => received.push(payload));

  const delivered = await pub.publish('news', { headline: 'hello' });
  assert.strictEqual(delivered, 1);

  await delay(30);
  assert.deepStrictEqual(received, [{ headline: 'hello' }]);
});

test('publish to a channel with no subscribers delivers to zero', async () => {
  assert.strictEqual(await pub.publish('empty-channel', 1), 0);
});

test('unsubscribe stops delivery', async () => {
  const received = [];
  const off = await sub.subscribe('updates', (p) => received.push(p));

  await pub.publish('updates', 'first');
  await delay(30);

  await off();
  await pub.publish('updates', 'second');
  await delay(30);

  assert.deepStrictEqual(received, ['first']);
});

test('wildcard pattern subscription matches by prefix', async () => {
  const received = [];
  const off = await sub.subscribe('orders.*', (payload, channel) => received.push([channel, payload]));

  await pub.publish('orders.created', { id: 1 });
  await pub.publish('orders.shipped', { id: 1 });
  await pub.publish('users.new', { id: 99 }); // must NOT match
  await delay(40);

  assert.deepStrictEqual(received, [
    ['orders.created', { id: 1 }],
    ['orders.shipped', { id: 1 }],
  ]);
  await off();
});

test('a single handler subscribed both exactly and by pattern fires only once', async () => {
  let count = 0;
  const handler = () => { count += 1; };
  const off1 = await sub.subscribe('metrics.cpu', handler); // exact
  const off2 = await sub.subscribe('metrics.*', handler); // pattern — also matches

  await pub.publish('metrics.cpu', 1);
  await delay(40);
  assert.strictEqual(count, 1, 'handler should be deduped across matching subscriptions');

  await off1();
  await off2();
});

test('multiple subscribers all receive a message', async () => {
  const sub2 = await client(broker);
  try {
    const got1 = [];
    const got2 = [];
    await sub.subscribe('broadcast', (p) => got1.push(p));
    await sub2.subscribe('broadcast', (p) => got2.push(p));

    const delivered = await pub.publish('broadcast', 'ping');
    assert.strictEqual(delivered, 2);

    await delay(30);
    assert.deepStrictEqual(got1, ['ping']);
    assert.deepStrictEqual(got2, ['ping']);
  } finally {
    await sub2.close();
  }
});

test('a subscriber keeps receiving after an in-memory broker restart resets offsets', async () => {
  const { createBroker, createClient } = require('../src');
  const { uniqueName, once } = require('./helpers');
  const name = uniqueName();
  const opts = { name, idleTimeout: 0, pubsub: { persist: true } }; // retention on, no persist dir
  let b1 = await createBroker(opts).start();
  const s = await createClient({ address: b1.address, autoSpawn: false, reconnect: true });
  const got = [];
  await s.subscribe('restart-ch', (m) => got.push(m));
  const p1 = await createClient({ address: b1.address, autoSpawn: false, reconnect: false });
  for (let i = 1; i <= 3; i++) await p1.publish('restart-ch', `old${i}`);
  await p1.close();
  await delay(30);
  assert.deepStrictEqual(got, ['old1', 'old2', 'old3']);

  const reconnected = once(s, 'reconnect');
  await b1.close();
  const b2 = await createBroker(opts).start(); // same address, offsets start at 0 again
  try {
    await reconnected;
    const p2 = await createClient({ address: b2.address, autoSpawn: false, reconnect: false });
    await p2.publish('restart-ch', 'new1');
    await p2.close();
    await delay(50);
    assert.deepStrictEqual(got, ['old1', 'old2', 'old3', 'new1'], 'post-restart message not skipped');
  } finally {
    await s.close();
    await b2.close();
  }
});

test('a throwing subscriber handler does not drop the connection or later messages', async () => {
  const warnings = [];
  const onWarn = (w) => warnings.push(w);
  process.on('warning', onWarn);
  const c = await client(broker);
  const got = [];
  try {
    await c.subscribe('boom', () => {
      throw new Error('handler bug');
    });
    await c.subscribe('boom-async', async () => {
      throw new Error('async handler bug');
    });
    await c.subscribe('fine', (m) => got.push(m));
    await pub.publish('boom', 1);
    await pub.publish('boom-async', 1);
    await pub.publish('fine', 'still-here');
    await delay(50);
    assert.ok(c.connected, 'connection survived the throwing handlers');
    assert.deepStrictEqual(got, ['still-here']);
    assert.ok(warnings.some((w) => /handler bug/.test(w.message)), 'failure surfaced as a warning');
  } finally {
    process.removeListener('warning', onWarn);
    await c.close();
  }
});
