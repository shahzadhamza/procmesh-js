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

test('concurrent incr from many clients loses no updates', async () => {
  const clients = await Promise.all(Array.from({ length: 5 }, () => client(broker)));
  try {
    await clients[0].set('counter', 0);
    const per = 50;
    await Promise.all(
      clients.flatMap((c) => Array.from({ length: per }, () => c.incr('counter')))
    );
    assert.strictEqual(await clients[0].get('counter'), clients.length * per);
  } finally {
    await Promise.all(clients.map((c) => c.close()));
  }
});

test('cas only sets when the expected value matches', async () => {
  const c = await client(broker);
  try {
    await c.set('cfg', 'v1');
    assert.strictEqual(await c.cas('cfg', 'wrong', 'v2'), false);
    assert.strictEqual(await c.get('cfg'), 'v1');
    assert.strictEqual(await c.cas('cfg', 'v1', 'v2'), true);
    assert.strictEqual(await c.get('cfg'), 'v2');
  } finally {
    await c.close();
  }
});

test('lock provides mutual exclusion', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    const release = await a.lock('resource', { wait: 0 });
    assert.ok(release, 'first lock should be acquired');

    // b cannot acquire while a holds it (wait: 0 -> immediate null)
    assert.strictEqual(await b.lock('resource', { wait: 0 }), null);

    await release();
    const release2 = await b.lock('resource', { wait: 0 });
    assert.ok(release2, 'lock should be acquirable after release');
    await release2();
  } finally {
    await a.close();
    await b.close();
  }
});

test('waiters are granted the lock FIFO when it is released', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    const releaseA = await a.lock('queue', { wait: 0 });
    assert.ok(releaseA);

    let bGotIt = false;
    const bWaiting = b.lock('queue', { wait: 2000 }).then((rel) => {
      bGotIt = true;
      return rel;
    });

    await delay(50);
    assert.strictEqual(bGotIt, false, 'b should still be waiting');

    await releaseA();
    const releaseB = await bWaiting;
    assert.ok(releaseB, 'b should acquire after a releases');
    assert.strictEqual(bGotIt, true);
    await releaseB();
  } finally {
    await a.close();
    await b.close();
  }
});

test('lock auto-releases after its TTL (crash protection)', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    await a.lock('short', { ttl: 60, wait: 0 });
    assert.strictEqual(await b.lock('short', { wait: 0 }), null);
    await delay(100);
    const rel = await b.lock('short', { wait: 0 });
    assert.ok(rel, 'lock should be free after TTL expiry');
    await rel();
  } finally {
    await a.close();
    await b.close();
  }
});

test('disconnect releases held locks', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    await a.lock('held', { ttl: 60000, wait: 0 });
    assert.strictEqual(await b.lock('held', { wait: 0 }), null);
    await a.close();
    await delay(30);
    const rel = await b.lock('held', { wait: 0 });
    assert.ok(rel, 'lock should free up when owner disconnects');
    await rel();
  } finally {
    await b.close();
  }
});

test('withLock runs the critical section and always releases', async () => {
  const a = await client(broker);
  const b = await client(broker);
  try {
    const order = [];
    const t1 = a.withLock('cs', async () => {
      order.push('a-start');
      await delay(60);
      order.push('a-end');
    });
    await delay(10);
    const t2 = b.withLock('cs', async () => {
      order.push('b-start');
      order.push('b-end');
    }, { wait: 2000 });
    await Promise.all([t1, t2]);
    assert.deepStrictEqual(order, ['a-start', 'a-end', 'b-start', 'b-end']);
  } finally {
    await a.close();
    await b.close();
  }
});
