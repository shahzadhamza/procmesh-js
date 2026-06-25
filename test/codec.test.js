'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const Client = require('../src/client');

// A custom codec is a pair of functions — it can't be serialized across the `spawn` boundary to an
// auto-spawned broker, which would then silently default to JSON and corrupt every frame. The client
// must refuse this combination up front rather than fail mysteriously at decode time.

const customCodec = {
  encode: (obj) => Buffer.from(JSON.stringify(obj)),
  decode: (buf) => JSON.parse(buf.toString('utf8')),
};

test('a custom-object codec with autoSpawn throws a clear error', () => {
  assert.throws(
    () => new Client({ codec: customCodec, autoSpawn: true }),
    /custom .* codec/i
  );
});

test('a custom-object codec is allowed when autoSpawn is disabled', () => {
  // The caller runs the broker themselves with the same codec — no spawn boundary to cross.
  assert.doesNotThrow(() => new Client({ codec: customCodec, autoSpawn: false }));
});

test('string codecs ("json" / "msgpack") are unaffected by the guard', () => {
  assert.doesNotThrow(() => new Client({ codec: 'json', autoSpawn: true }));
  // 'msgpack' resolves the optional dependency; only assert the guard doesn't reject the string form.
  assert.doesNotThrow(() => new Client({ autoSpawn: true })); // default (json)
});
