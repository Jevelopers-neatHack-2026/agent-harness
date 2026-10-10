const INTEGER_TOKEN = /^-?(?:0|[1-9][0-9]*)$/u;
const CANONICAL_INTEGER_TOKEN = /^(?:0|[1-9][0-9]*|-[1-9][0-9]*)$/u;

export class BoundedJsonError extends Error {
  constructor(code) {
    super('The bounded JSON input is invalid.');
    this.name = 'BoundedJsonError';
    this.code = code;
  }
}

function fail(code) {
  throw new BoundedJsonError(code);
}

/**
 * Validate bounded JSON before JSON.parse so duplicate object members and
 * integer spellings that normalize to a different value cannot be adopted.
 * When integerNumbersOnly is set, accepted numeric tokens are safe canonical
 * integers; that is the complete numeric domain for plans and limits.
 */
export function parseBoundedJson(source, {
  maxBytes,
  maxDepth,
  maxNodes = 8_192,
  integerNumbersOnly = false,
  allowUnsafeFiniteNumbers = false,
  allowUnsafeFiniteNumberPaths = []
} = {}) {
  if (
    typeof source !== 'string'
    || !Number.isSafeInteger(maxBytes)
    || maxBytes < 0
    || !Number.isSafeInteger(maxDepth)
    || maxDepth < 0
    || !Number.isSafeInteger(maxNodes)
    || maxNodes < 1
    || !Array.isArray(allowUnsafeFiniteNumberPaths)
    || allowUnsafeFiniteNumberPaths.some(path => (
      !Array.isArray(path)
      || path.length === 0
      || path.length > maxDepth
      || path.some(segment => typeof segment !== 'string')
    ))
  ) fail('JSON_INVALID');
  if (Buffer.byteLength(source, 'utf8') > maxBytes) fail('JSON_TOO_LARGE');

  let index = 0;
  let nodes = 0;

  function whitespace() {
    while (index < source.length && /[\u0009\u000a\u000d\u0020]/u.test(source[index])) index += 1;
  }

  function countNode() {
    nodes += 1;
    if (nodes > maxNodes) fail('JSON_TOO_LARGE');
  }

  function stringToken() {
    if (source[index] !== '"') fail('JSON_INVALID');
    const start = index;
    index += 1;
    while (index < source.length) {
      const code = source.charCodeAt(index);
      if (code === 0x22) {
        index += 1;
        try {
          return JSON.parse(source.slice(start, index));
        } catch {
          fail('JSON_INVALID');
        }
      }
      if (code < 0x20) fail('JSON_INVALID');
      if (code === 0x5c) {
        index += 1;
        if (index >= source.length) fail('JSON_INVALID');
        const escaped = source[index];
        if (escaped === 'u') {
          if (!/^[0-9a-fA-F]{4}$/u.test(source.slice(index + 1, index + 5))) {
            fail('JSON_INVALID');
          }
          index += 5;
          continue;
        }
        if (!'"\\/bfnrt'.includes(escaped)) fail('JSON_INVALID');
      }
      index += 1;
    }
    fail('JSON_INVALID');
  }

  function unsafeFiniteNumbersAllowedAt(path) {
    return allowUnsafeFiniteNumberPaths.some(prefix => (
      prefix.length <= path.length
      && prefix.every((segment, index) => path[index] === segment)
    ));
  }

  function numberToken(path) {
    const start = index;
    if (source[index] === '-') index += 1;
    if (source[index] === '0') {
      index += 1;
      if (/[0-9]/u.test(source[index] ?? '')) fail('JSON_INVALID');
    } else {
      if (!/[1-9]/u.test(source[index] ?? '')) fail('JSON_INVALID');
      while (/[0-9]/u.test(source[index] ?? '')) index += 1;
    }
    if (source[index] === '.') {
      index += 1;
      if (!/[0-9]/u.test(source[index] ?? '')) fail('JSON_INVALID');
      while (/[0-9]/u.test(source[index] ?? '')) index += 1;
    }
    if (source[index] === 'e' || source[index] === 'E') {
      index += 1;
      if (source[index] === '+' || source[index] === '-') index += 1;
      if (!/[0-9]/u.test(source[index] ?? '')) fail('JSON_INVALID');
      while (/[0-9]/u.test(source[index] ?? '')) index += 1;
    }

    const token = source.slice(start, index);
    if (token === '-0') fail('JSON_NUMBER_INVALID');
    if (integerNumbersOnly && !CANONICAL_INTEGER_TOKEN.test(token)) {
      fail('JSON_NUMBER_INVALID');
    }
    const value = Number(token);
    if (!Number.isFinite(value)) fail('JSON_NUMBER_INVALID');
    if (integerNumbersOnly && !Number.isSafeInteger(value)) fail('JSON_NUMBER_INVALID');
    if (
      !integerNumbersOnly
      && !allowUnsafeFiniteNumbers
      && !unsafeFiniteNumbersAllowedAt(path)
    ) {
      if (INTEGER_TOKEN.test(token) && !Number.isSafeInteger(value)) {
        fail('JSON_NUMBER_INVALID');
      }
      if (!INTEGER_TOKEN.test(token) && Number.isSafeInteger(value)) {
        fail('JSON_NUMBER_INVALID');
      }
    }
  }

  function value(depth, path) {
    whitespace();
    countNode();
    const current = source[index];
    if (current === '"') {
      stringToken();
      return;
    }
    if (current === '{' || current === '[') {
      const containerDepth = depth + 1;
      if (containerDepth > maxDepth) fail('JSON_TOO_DEEP');
      const isObject = current === '{';
      index += 1;
      whitespace();
      if (source[index] === (isObject ? '}' : ']')) {
        index += 1;
        return;
      }
      const members = isObject ? new Set() : null;
      while (index < source.length) {
        whitespace();
        if (isObject) {
          countNode();
          const key = stringToken();
          if (members.has(key)) fail('JSON_DUPLICATE_KEY');
          members.add(key);
          whitespace();
          if (source[index] !== ':') fail('JSON_INVALID');
          index += 1;
          value(containerDepth, [...path, key]);
        } else {
          value(containerDepth, path);
        }
        whitespace();
        if (source[index] === (isObject ? '}' : ']')) {
          index += 1;
          return;
        }
        if (source[index] !== ',') fail('JSON_INVALID');
        index += 1;
        whitespace();
      }
      fail('JSON_INVALID');
    }
    if (current === 't' && source.slice(index, index + 4) === 'true') {
      index += 4;
      return;
    }
    if (current === 'f' && source.slice(index, index + 5) === 'false') {
      index += 5;
      return;
    }
    if (current === 'n' && source.slice(index, index + 4) === 'null') {
      index += 4;
      return;
    }
    if (current === '-' || /[0-9]/u.test(current ?? '')) {
      numberToken(path);
      return;
    }
    fail('JSON_INVALID');
  }

  value(0, []);
  whitespace();
  if (index !== source.length) fail('JSON_INVALID');
  try {
    return JSON.parse(source);
  } catch {
    fail('JSON_INVALID');
  }
}
