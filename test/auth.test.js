'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { createClient } = require('../src');
const { startBroker } = require('./helpers');

test('correct token connects; wrong/absent token is rejected with EAUTH', async () => {
  const broker = await startBroker({ token: 's3cret' });
  try {
    // correct token works
    const ok = await createClient({ address: broker.address, token: 's3cret', autoSpawn: false, reconnect: false });
    await ok.set('k', 1);
    assert.strictEqual(await ok.get('k'), 1);
    await ok.close();

    // wrong token rejected
    await assert.rejects(
      () => createClient({ address: broker.address, token: 'wrong', autoSpawn: false, reconnect: false }),
      (err) => {
        assert.strictEqual(err.code, 'EAUTH');
        return true;
      }
    );

    // missing token rejected
    await assert.rejects(
      () => createClient({ address: broker.address, autoSpawn: false, reconnect: false }),
      (err) => {
        assert.strictEqual(err.code, 'EAUTH');
        return true;
      }
    );
  } finally {
    await broker.close();
  }
});

test('broker without a token accepts everyone (zero-config default preserved)', async () => {
  const broker = await startBroker();
  try {
    const c = await createClient({ address: broker.address, token: 'ignored', autoSpawn: false, reconnect: false });
    await c.set('k', 2);
    assert.strictEqual(await c.get('k'), 2);
    await c.close();
  } finally {
    await broker.close();
  }
});
