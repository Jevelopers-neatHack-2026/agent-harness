import { createHash } from 'node:crypto';
import { types as utilTypes } from 'node:util';

const REGISTRY_DATA = new WeakMap();
const REGISTRY_HANDLES = new WeakSet();
const REGISTRY_SEMANTIC_VERSION = 1;

const DEFINITIONS = Object.freeze([
  Object.freeze({
    key: 'number.add',
    version: 1,
    effect: 'local-pure',
    availability: 'available',
    input: 'finite-safe-integer-pair',
    output: 'finite-safe-integer'
  }),
  Object.freeze({
    key: 'text.concat',
    version: 1,
    effect: 'local-pure',
    availability: 'available',
    input: 'bounded-string-parts',
    output: 'bounded-string'
  }),
  Object.freeze({
    key: 'network.fetch',
    version: 1,
    effect: 'external',
    availability: 'unavailable',
    input: 'none',
    output: 'none'
  }),
  Object.freeze({
    key: 'provider.image',
    version: 1,
    effect: 'provider',
    availability: 'unavailable',
    input: 'none',
    output: 'none'
  }),
  Object.freeze({
    key: 'provider.reason',
    version: 1,
    effect: 'provider',
    availability: 'unavailable',
    input: 'none',
    output: 'none'
  }),
  Object.freeze({
    key: 'system.shell',
    version: 1,
    effect: 'privileged',
    availability: 'unavailable',
    input: 'none',
    output: 'none'
  })
]);

const MAX_INTEGER_ABS = 1_000_000_000;
const MAX_STRING_PARTS = 32;
const MAX_STRING_PART_UNITS = 512;
const MAX_COMBINED_STRING_BYTES = 2_048;

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function reject(code) {
  return Object.freeze({ ok: false, code });
}

function registryData(registry) {
  if (!REGISTRY_HANDLES.has(registry)) return null;
  return REGISTRY_DATA.get(registry);
}

function ownArgsRecord(value, allowed, required) {
  if (
    value === null
    || typeof value !== 'object'
    || Array.isArray(value)
    || utilTypes.isProxy(value)
  ) {
    return null;
  }

  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) return null;
    keys = Reflect.ownKeys(value);
  } catch {
    return null;
  }

  if (keys.length > allowed.size) return null;
  const output = Object.create(null);
  for (const key of keys) {
    if (typeof key !== 'string' || !allowed.has(key)) return null;
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    output[key] = descriptor.value;
  }
  for (const key of required) {
    if (!Object.hasOwn(output, key)) return null;
  }
  return output;
}

function add(args) {
  const input = ownArgsRecord(args, new Set(['left', 'right']), ['left', 'right']);
  if (!input) return reject('INVALID_LOCAL_INPUT');
  for (const value of [input.left, input.right]) {
    if (
      !Number.isSafeInteger(value)
      || Math.abs(value) > MAX_INTEGER_ABS
    ) {
      return reject('INVALID_LOCAL_INPUT');
    }
  }
  const result = input.left + input.right;
  if (!Number.isSafeInteger(result) || Math.abs(result) > MAX_INTEGER_ABS * 2) {
    return reject('INVALID_LOCAL_RESULT');
  }
  return Object.freeze({ ok: true, value: result });
}

function concat(args) {
  const input = ownArgsRecord(args, new Set(['parts']), ['parts']);
  if (!input || !Array.isArray(input.parts) || utilTypes.isProxy(input.parts)) {
    return reject('INVALID_LOCAL_INPUT');
  }

  const parts = input.parts;
  const prototype = Object.getPrototypeOf(parts);
  if (
    (prototype !== Array.prototype && prototype !== null)
    || parts.length < 1
    || parts.length > MAX_STRING_PARTS
    || Reflect.ownKeys(parts).length !== parts.length + 1
  ) {
    return reject('INVALID_LOCAL_INPUT');
  }
  const safeParts = [];
  for (let index = 0; index < parts.length; index += 1) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(parts, String(index));
    } catch {
      return reject('INVALID_LOCAL_INPUT');
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) {
      return reject('INVALID_LOCAL_INPUT');
    }
    const part = descriptor.value;
    if (typeof part !== 'string' || part.length > MAX_STRING_PART_UNITS) {
      return reject('INVALID_LOCAL_INPUT');
    }
    safeParts.push(part);
  }
  const result = safeParts.join('');
  if (Buffer.byteLength(result, 'utf8') > MAX_COMBINED_STRING_BYTES) {
    return reject('INVALID_LOCAL_RESULT');
  }
  return Object.freeze({ ok: true, value: result });
}

const HANDLERS = new Map([
  ['number.add', add],
  ['text.concat', concat]
]);

function createRegistry() {
  // Bump this when handler behavior or validation semantics change. This is
  // an explicit compatibility revision, not a hash of JavaScript function text.
  const operationManifest = JSON.stringify(DEFINITIONS.map(definition => [
    definition.key,
    definition.version,
    definition.effect,
    definition.availability,
    definition.input,
    definition.output
  ]));
  // Semantic version 1 keeps its shipped manifest hash for recovery compatibility.
  // A later semantic revision changes both the prefix and hashed manifest.
  const manifest = REGISTRY_SEMANTIC_VERSION === 1
    ? operationManifest
    : JSON.stringify([REGISTRY_SEMANTIC_VERSION, operationManifest]);
  const version = 'registry' + REGISTRY_SEMANTIC_VERSION + '_' + sha256(manifest);
  const metadata = Object.freeze(DEFINITIONS.map(definition => Object.freeze({ ...definition })));
  const operations = new Map(DEFINITIONS.map(definition => [definition.key, definition]));
  const handle = Object.freeze({
    kind: 'agent-harness.private-static-registry',
    semanticVersion: REGISTRY_SEMANTIC_VERSION,
    version,
    operations: metadata
  });
  REGISTRY_DATA.set(handle, Object.freeze({ version, metadata, operations }));
  REGISTRY_HANDLES.add(handle);
  return handle;
}

const DEFAULT_REGISTRY = createRegistry();

export function createDefaultRegistry() {
  return DEFAULT_REGISTRY;
}

export function isDefaultRegistry(registry) {
  return registryData(registry) !== null;
}

export function getRegistryVersion(registry = DEFAULT_REGISTRY) {
  return registryData(registry)?.version ?? null;
}

export function getRegistrySemanticVersion(registry = DEFAULT_REGISTRY) {
  return registryData(registry) ? REGISTRY_SEMANTIC_VERSION : null;
}

export function listRegistryOperations(registry = DEFAULT_REGISTRY) {
  return registryData(registry)?.metadata ?? Object.freeze([]);
}

export function inspectOperation(registry, operationKey) {
  const data = registryData(registry);
  if (!data || typeof operationKey !== 'string') {
    return Object.freeze({ status: 'invalid-registry', effect: 'unknown' });
  }
  const definition = data.operations.get(operationKey);
  if (!definition) return Object.freeze({ status: 'unknown-operation', effect: 'unknown' });
  return Object.freeze({
    status: definition.availability,
    effect: definition.effect,
    input: definition.input,
    output: definition.output,
    version: definition.version
  });
}

export function executeLocalOperation(registry, operationKey, args) {
  const data = registryData(registry);
  if (!data) return reject('INVALID_REGISTRY');
  if (typeof operationKey !== 'string') return reject('UNKNOWN_OPERATION');

  const definition = data.operations.get(operationKey);
  if (!definition) return reject('UNKNOWN_OPERATION');
  if (definition.effect !== 'local-pure' || definition.availability !== 'available') {
    return reject('EFFECT_UNAVAILABLE');
  }

  const handler = HANDLERS.get(operationKey);
  if (typeof handler !== 'function') return reject('LOCAL_HANDLER_UNAVAILABLE');
  try {
    return handler(args);
  } catch {
    return reject('LOCAL_COMPUTATION_FAILED');
  }
}

export const DEFAULT_TOOL_REGISTRY = DEFAULT_REGISTRY;
