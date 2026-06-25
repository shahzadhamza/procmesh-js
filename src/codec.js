'use strict';

/**
 * A codec serializes/deserializes message objects to/from Buffers.
 * Shape: { encode(obj) -> Buffer, decode(Buffer) -> obj }.
 *
 * The default JSON codec is zero-dependency and debuggable. `msgpackr` is
 * offered as an optional faster binary codec (graceful fallback if not installed).
 */

const jsonCodec = {
  name: 'json',
  encode(obj) {
    return Buffer.from(JSON.stringify(obj));
  },
  decode(buf) {
    return JSON.parse(buf.toString('utf8'));
  },
};

let msgpackCodec = null;
function loadMsgpack() {
  if (msgpackCodec) return msgpackCodec;
  let mod;
  try {
    mod = require('msgpackr');
  } catch (err) {
    throw new Error(
      'codec "msgpack" requires the optional dependency "msgpackr". Run `npm install msgpackr`, or use the default "json" codec.'
    );
  }
  const packer = new mod.Packr({ structuredClone: true });
  msgpackCodec = {
    name: 'msgpack',
    encode: (obj) => packer.pack(obj),
    decode: (buf) => packer.unpack(buf),
  };
  return msgpackCodec;
}

/**
 * Normalize a codec option into a concrete codec object.
 * Accepts: undefined | 'json' | 'msgpack' | a custom { encode, decode } object.
 */
function resolveCodec(codec) {
  if (!codec || codec === 'json') return jsonCodec;
  if (codec === 'msgpack' || codec === 'msgpackr') return loadMsgpack();
  if (typeof codec === 'object' && typeof codec.encode === 'function' && typeof codec.decode === 'function') {
    return codec;
  }
  throw new Error(`invalid codec: ${String(codec)} (expected "json", "msgpack", or a { encode, decode } object)`);
}

module.exports = { jsonCodec, resolveCodec };
