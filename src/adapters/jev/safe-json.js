import { TextDecoder, types as utilTypes } from 'node:util';

export const JEV_MAX_DEPTH = 48;
export const JEV_MAX_NODES = 12_000;
export const JEV_MAX_KEYS = 4_096;
export const JEV_MAX_STRING_UNITS = 262_144;
export const JEV_MAX_ARRAY_ITEMS = 4_096;
export const JEV_MAX_RESPONSE_BYTES = 512 * 1024;
const TYPED_ARRAY_PROTOTYPE = Object.getPrototypeOf(Uint8Array.prototype);
const TYPED_ARRAY_BYTE_LENGTH = Object.getOwnPropertyDescriptor(TYPED_ARRAY_PROTOTYPE, 'byteLength')?.get;
const BUFFER_METADATA_KEYS = ['length', 'byteLength', 'byteOffset', 'buffer'];

export class JevWireError extends Error {
  constructor(code, message = 'The JEV wire value is invalid.') {
    super(message);
    this.name = 'JevWireError';
    this.code = code;
  }
}

export function isProxy(value) {
  try {
    return utilTypes.isProxy(value);
  } catch {
    return true;
  }
}

function invalid(code = 'INVALID_REQUEST') {
  throw new JevWireError(code);
}

function plainRecord(value) {
  if (value === null || typeof value !== 'object' || isProxy(value) || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function safeString(value, context, maximum = JEV_MAX_STRING_UNITS) {
  if (typeof value !== 'string' || value.length > maximum) invalid();
  context.stringUnits += value.length;
  if (context.stringUnits > JEV_MAX_STRING_UNITS) invalid();
  return value;
}

function snapshot(value, context, depth) {
  context.nodes += 1;
  if (context.nodes > JEV_MAX_NODES || depth > JEV_MAX_DEPTH) invalid();
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') return safeString(value, context);
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || isProxy(value) || context.seen.has(value)) invalid();

  context.seen.add(value);
  try {
    if (Array.isArray(value)) {
      const prototype = Object.getPrototypeOf(value);
      if (prototype !== Array.prototype && prototype !== null) invalid();
      const keys = Reflect.ownKeys(value);
      const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
      const length = lengthDescriptor?.value;
      if (!Number.isSafeInteger(length)
          || length < 0
          || length > JEV_MAX_ARRAY_ITEMS
          || keys.length !== length + 1
          || context.nodes + length > JEV_MAX_NODES) {
        invalid();
      }
      const result = [];
      for (let index = 0; index < length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) invalid();
        result.push(snapshot(descriptor.value, context, depth + 1));
      }
      Object.setPrototypeOf(result, null);
      return Object.freeze(result);
    }

    if (!plainRecord(value)) invalid();
    const keys = Reflect.ownKeys(value);
    if (keys.length > JEV_MAX_KEYS || context.keys + keys.length > JEV_MAX_KEYS) invalid();
    context.keys += keys.length;
    const result = Object.create(null);
    for (const key of keys) {
      if (typeof key !== 'string') invalid();
      safeString(key, context, 256);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) invalid();
      Object.defineProperty(result, key, {
        value: snapshot(descriptor.value, context, depth + 1),
        enumerable: true,
        configurable: false,
        writable: false
      });
    }
    return Object.freeze(result);
  } finally {
    context.seen.delete(value);
  }
}

export function snapshotJsonValue(value, { allowPrimitive = true } = {}) {
  const result = snapshot(value, { nodes: 0, keys: 0, stringUnits: 0, seen: new Set() }, 0);
  if (!allowPrimitive && (result === null || typeof result !== 'object')) invalid();
  return result;
}

function makeParser(text) {
  let offset = 0;
  let nodes = 0;
  let keys = 0;
  let stringUnits = 0;

  function fail() {
    invalid('INVALID_RESPONSE');
  }

  function whitespace() {
    while (offset < text.length) {
      const code = text.charCodeAt(offset);
      if (code === 0x20 || code === 0x09 || code === 0x0a || code === 0x0d) offset += 1;
      else break;
    }
  }

  function parseString() {
    const start = offset;
    if (text.charCodeAt(offset) !== 0x22) fail();
    offset += 1;
    while (offset < text.length) {
      const code = text.charCodeAt(offset);
      if (code === 0x22) {
        offset += 1;
        let value;
        try {
          value = JSON.parse(text.slice(start, offset));
        } catch {
          fail();
        }
        if (typeof value !== 'string') fail();
        stringUnits += value.length;
        if (value.length > JEV_MAX_STRING_UNITS || stringUnits > JEV_MAX_STRING_UNITS) fail();
        return value;
      }
      if (code < 0x20) fail();
      if (code === 0x5c) {
        offset += 1;
        const escape = text.charCodeAt(offset);
        if (escape === 0x75) {
          const hex = text.slice(offset + 1, offset + 5);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail();
          offset += 5;
          continue;
        }
        if (![0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74].includes(escape)) fail();
      }
      offset += 1;
    }
    fail();
  }

  function parseNumber() {
    const rest = text.slice(offset);
    const match = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(rest);
    if (!match) fail();
    offset += match[0].length;
    const value = Number(match[0]);
    if (!Number.isFinite(value)) fail();
    return value;
  }

  function parseArray(depth) {
    offset += 1;
    whitespace();
    const result = [];
    if (text.charCodeAt(offset) === 0x5d) {
      offset += 1;
      return Object.freeze(Object.setPrototypeOf(result, null));
    }
    while (true) {
      if (result.length >= JEV_MAX_ARRAY_ITEMS) fail();
      result.push(parseValue(depth + 1));
      whitespace();
      const code = text.charCodeAt(offset);
      if (code === 0x5d) {
        offset += 1;
        return Object.freeze(Object.setPrototypeOf(result, null));
      }
      if (code !== 0x2c) fail();
      offset += 1;
      whitespace();
    }
  }

  function parseObject(depth) {
    offset += 1;
    whitespace();
    const result = Object.create(null);
    const seen = new Set();
    if (text.charCodeAt(offset) === 0x7d) {
      offset += 1;
      return Object.freeze(result);
    }
    while (true) {
      if (text.charCodeAt(offset) !== 0x22) fail();
      const key = parseString();
      if (seen.has(key)) fail();
      seen.add(key);
      keys += 1;
      if (keys > JEV_MAX_KEYS) fail();
      whitespace();
      if (text.charCodeAt(offset) !== 0x3a) fail();
      offset += 1;
      whitespace();
      Object.defineProperty(result, key, {
        value: parseValue(depth + 1),
        enumerable: true,
        configurable: false,
        writable: false
      });
      whitespace();
      const code = text.charCodeAt(offset);
      if (code === 0x7d) {
        offset += 1;
        return Object.freeze(result);
      }
      if (code !== 0x2c) fail();
      offset += 1;
      whitespace();
    }
  }

  function parseValue(depth) {
    nodes += 1;
    if (nodes > JEV_MAX_NODES || depth > JEV_MAX_DEPTH) fail();
    whitespace();
    const code = text.charCodeAt(offset);
    if (code === 0x22) return parseString();
    if (code === 0x7b) return parseObject(depth);
    if (code === 0x5b) return parseArray(depth);
    if (code === 0x74 && text.startsWith('true', offset)) { offset += 4; return true; }
    if (code === 0x66 && text.startsWith('false', offset)) { offset += 5; return false; }
    if (code === 0x6e && text.startsWith('null', offset)) { offset += 4; return null; }
    if (code === 0x2d || (code >= 0x30 && code <= 0x39)) return parseNumber();
    fail();
  }

  return () => {
    const value = parseValue(0);
    whitespace();
    if (offset !== text.length) fail();
    return value;
  };
}

function actualBufferByteLength(bytes, maximumBytes) {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || maximumBytes > JEV_MAX_RESPONSE_BYTES
      || isProxy(bytes) || !Buffer.isBuffer(bytes)) {
    invalid('INVALID_RESPONSE');
  }
  let byteLength;
  try {
    if (typeof TYPED_ARRAY_BYTE_LENGTH !== 'function') invalid('INVALID_RESPONSE');
    byteLength = TYPED_ARRAY_BYTE_LENGTH.call(bytes);
    if (!Number.isSafeInteger(byteLength) || byteLength < 1 || byteLength > maximumBytes) {
      invalid('INVALID_RESPONSE');
    }
    if (Object.getPrototypeOf(bytes) !== Buffer.prototype) invalid('INVALID_RESPONSE');
    if (BUFFER_METADATA_KEYS.some(key => Object.getOwnPropertyDescriptor(bytes, key) !== undefined)) {
      invalid('INVALID_RESPONSE');
    }
  } catch {
    invalid('INVALID_RESPONSE');
  }
  return byteLength;
}

export function parseStrictJsonBytes(bytes, maximumBytes = JEV_MAX_RESPONSE_BYTES) {
  const byteLength = actualBufferByteLength(bytes, maximumBytes);
  try {
    if (byteLength >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
      invalid('INVALID_RESPONSE');
    }
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    if (text.charCodeAt(0) === 0xfeff) invalid('INVALID_RESPONSE');
    return makeParser(text)();
  } catch {
    invalid('INVALID_RESPONSE');
  }
}

export function isPlainDataRecord(value) {
  return plainRecord(value);
}
