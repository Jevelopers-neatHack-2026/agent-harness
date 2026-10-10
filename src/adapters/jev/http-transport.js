import https from 'node:https';
import { Resolver } from 'node:dns/promises';
import { isIP } from 'node:net';
import { performance } from 'node:perf_hooks';
import { isPlainDataRecord, isProxy } from './safe-json.js';
import { JEV_MAX_RESPONSE_BYTES } from './safe-json.js';
import { serializeJevRequest } from './request-validation.js';
import { validateJevResponseBytes } from './response-validation.js';
import { isPublicJevAddress } from './address-policy.js';

const TARGET_HOST = 'api.typesafe.ai';
const TARGET_PORT = 443;
const TARGET_PATH = '/v1/systemone';
const TOTAL_DEADLINE_MS = 8_000;
const DNS_QUERY_TIMEOUT_MS = 2_000;
const MAX_HEADER_COUNT = 128;
const MAX_HEADER_BYTES = 16 * 1024;

const SAFE_MESSAGES = Object.freeze({
  ABORTED_BEFORE_DISPATCH: 'The JEV request was cancelled before an HTTP write.',
  DEADLINE_BEFORE_DISPATCH: 'The JEV request deadline expired before an HTTP write.',
  ABORTED_OUTCOME_UNKNOWN: 'The JEV request was cancelled after a possible HTTP write.',
  DEADLINE_OUTCOME_UNKNOWN: 'The JEV request deadline expired after a possible HTTP write.',
  DNS_UNAVAILABLE: 'The fixed JEV destination could not be resolved safely.',
  DESTINATION_REJECTED: 'The fixed JEV destination resolved to an address outside the allowed range.',
  TRANSPORT_FAILED_BEFORE_DISPATCH: 'The fixed JEV transport failed before an HTTP write.',
  TRANSPORT_OUTCOME_UNKNOWN: 'The fixed JEV transport failed after a possible HTTP write.',
  RESPONSE_LIMIT_EXCEEDED: 'The JEV response exceeded a transport safety limit.',
  INVALID_TRANSPORT_INPUT: 'The private JEV transport input is invalid.'
});

export class JevTransportError extends Error {
  constructor(code, dispatchCertainty) {
    super(SAFE_MESSAGES[code] ?? SAFE_MESSAGES.TRANSPORT_OUTCOME_UNKNOWN);
    this.name = 'JevTransportError';
    this.code = code;
    this.dispatchCertainty = dispatchCertainty;
  }
}

function isAbortSignal(value) {
  if (value === undefined) return true;
  if (value === null || typeof value !== 'object' || isProxy(value)) return false;
  try {
    if (Object.getPrototypeOf(value) !== AbortSignal.prototype) return false;
    const getter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')?.get;
    if (typeof getter !== 'function') return false;
    getter.call(value);
    return true;
  } catch {
    return false;
  }
}

function signalAborted(signal) {
  if (signal === undefined) return false;
  const getter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;
  return getter.call(signal);
}

function readInput(input) {
  if (!isPlainDataRecord(input)) return null;
  let keys;
  try {
    keys = Reflect.ownKeys(input);
  } catch {
    return null;
  }
  const allowed = new Set(['request', 'apiKey', 'signal']);
  if (keys.length < 2 || keys.length > 3
      || keys.some(key => typeof key !== 'string' || !allowed.has(key))) return null;
  const fields = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(input, key);
    } catch {
      return null;
    }
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) return null;
    fields[key] = descriptor.value;
  }
  if (!Object.hasOwn(fields, 'request') || !Object.hasOwn(fields, 'apiKey')) return null;
  if (Object.hasOwn(fields, 'signal') && !isAbortSignal(fields.signal)) return null;
  if (typeof fields.apiKey !== 'string'
      || fields.apiKey.length < 8
      || fields.apiKey.length > 512
      || /[\u0000-\u0020\u007f]/u.test(fields.apiKey)) return null;
  return fields;
}

function resolveNoData(error) {
  const code = typeof error?.code === 'string' ? error.code : '';
  return code === 'ENODATA' || code === 'ENOTFOUND' || code === 'NODATA';
}

function readDnsRecord(result) {
  if (!result || !Array.isArray(result.values)) return [];
  if (result.values.length > 16) throw new JevTransportError('DESTINATION_REJECTED', 'not_dispatched');
  return result.values;
}

function readResponseHeaders(response) {
  const raw = response.rawHeaders;
  if (!Array.isArray(raw) || raw.length % 2 !== 0 || raw.length / 2 > MAX_HEADER_COUNT) {
    throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
  }
  let byteCount = 0;
  for (let index = 0; index < raw.length; index += 2) {
    const name = raw[index];
    const value = raw[index + 1];
    if (typeof name !== 'string' || typeof value !== 'string') {
      throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
    }
    byteCount += Buffer.byteLength(name, 'latin1') + Buffer.byteLength(value, 'latin1') + 4;
    if (byteCount > MAX_HEADER_BYTES || /[\r\n\u0000]/u.test(value)) {
      throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
    }
  }
  const headers = response.headers;
  if (headers === null || typeof headers !== 'object' || isProxy(headers)) {
    throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
  }
  const contentLength = headers['content-length'];
  const transferEncoding = headers['transfer-encoding'];
  if (transferEncoding !== undefined
      && (transferEncoding !== 'chunked' || contentLength !== undefined)) {
    throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
  }
  if (contentLength !== undefined) {
    if (typeof contentLength !== 'string' || !/^(?:0|[1-9][0-9]*)$/u.test(contentLength)) {
      throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
    }
    const length = Number(contentLength);
    if (!Number.isSafeInteger(length) || length > JEV_MAX_RESPONSE_BYTES) {
      throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
    }
  }
  const retryAfter = headers['retry-after'];
  const safeRetryAfter = typeof retryAfter === 'string'
      && retryAfter.length <= 128
      && !/[\u0000-\u001f\u007f]/u.test(retryAfter)
    ? retryAfter
    : undefined;
  const requestId = headers['x-typesafe-request-id'];
  const safeRequestId = typeof requestId === 'string'
      && /^[A-Za-z0-9._:-]{1,128}$/u.test(requestId)
    ? requestId
    : null;
  return { retryAfter: safeRetryAfter, providerRequestId: safeRequestId };
}

function resolveAddress(resolver, method) {
  return resolver[method](TARGET_HOST).then(
    values => ({ values, error: null }),
    error => ({ values: null, error: resolveNoData(error) ? null : 'failed' })
  );
}

function pinnedLookup(address) {
  const family = isIP(address);
  return (hostname, options, callback) => {
    if (hostname !== TARGET_HOST) {
      callback(new Error('Fixed destination lookup rejected.'));
      return;
    }
    if (options?.all === true) {
      callback(null, [{ address, family }]);
      return;
    }
    callback(null, address, family);
  };
}

function destroySocket(socket) {
  try {
    if (socket && typeof socket.destroy === 'function' && !socket.destroyed) socket.destroy();
  } catch {}
}

function requestOptions(address, apiKey, body, agent) {
  return {
    protocol: 'https:',
    hostname: TARGET_HOST,
    port: TARGET_PORT,
    method: 'POST',
    path: TARGET_PATH,
    servername: TARGET_HOST,
    rejectUnauthorized: true,
    minVersion: 'TLSv1.2',
    lookup: pinnedLookup(address),
    agent,
    headers: {
      authorization: 'Bearer ' + apiKey,
      accept: 'application/json',
      'content-type': 'application/json',
      'content-length': String(body.length),
      connection: 'close'
    }
  };
}

export function sendJevOnce(input) {
  const fields = readInput(input);
  if (!fields) throw new JevTransportError('INVALID_TRANSPORT_INPUT', 'not_dispatched');
  let serialized;
  try {
    serialized = serializeJevRequest(fields.request);
  } catch {
    throw new JevTransportError('INVALID_TRANSPORT_INPUT', 'not_dispatched');
  }
  const body = Buffer.from(serialized.json, 'utf8');
  const request = serialized.request;
  const signal = fields.signal;
  if (signalAborted(signal)) throw new JevTransportError('ABORTED_BEFORE_DISPATCH', 'not_dispatched');

  const deadlineAt = performance.now() + TOTAL_DEADLINE_MS;
  return new Promise((resolve, reject) => {
    let settled = false;
    let possibleWrite = false;
    let resolver;
    let requestObject;
    let activeSocket;
    let activeAgent;
    let timeout;
    let chunks = [];
    let responseBytes = 0;
    let responseStatus = null;
    let responseHeaders = null;

    const cleanup = () => {
      clearTimeout(timeout);
      if (signal !== undefined) EventTarget.prototype.removeEventListener.call(signal, 'abort', onAbort);
      try { resolver?.cancel(); } catch {}
    };

    const destroyResources = () => {
      try { requestObject?.destroy(); } catch {}
      destroySocket(activeSocket);
      try { activeAgent?.destroy(); } catch {}
    };

    const finishError = code => {
      if (settled) return;
      settled = true;
      cleanup();
      destroyResources();
      const certainty = possibleWrite ? 'possible' : 'not_dispatched';
      if (code === 'ABORTED') {
        reject(new JevTransportError(possibleWrite ? 'ABORTED_OUTCOME_UNKNOWN' : 'ABORTED_BEFORE_DISPATCH', certainty));
      } else if (code === 'DEADLINE') {
        reject(new JevTransportError(possibleWrite ? 'DEADLINE_OUTCOME_UNKNOWN' : 'DEADLINE_BEFORE_DISPATCH', certainty));
      } else {
        const safeCode = code === 'TRANSPORT_OUTCOME_UNKNOWN' && !possibleWrite
          ? 'TRANSPORT_FAILED_BEFORE_DISPATCH'
          : code;
        reject(new JevTransportError(safeCode, certainty));
      }
    };

    const finishResult = result => {
      if (settled) return;
      if (performance.now() >= deadlineAt) {
        finishError('DEADLINE');
        return;
      }
      settled = true;
      cleanup();
      destroyResources();
      resolve(Object.freeze(result));
    };

    const checkDeadline = () => {
      if (performance.now() >= deadlineAt) {
        finishError('DEADLINE');
        return false;
      }
      return true;
    };

    const onAbort = () => finishError('ABORTED');
    if (signal !== undefined) EventTarget.prototype.addEventListener.call(signal, 'abort', onAbort, { once: true });
    timeout = setTimeout(() => finishError('DEADLINE'), TOTAL_DEADLINE_MS);

    try {
      resolver = new Resolver({ timeout: DNS_QUERY_TIMEOUT_MS, tries: 1 });
    } catch {
      finishError('DNS_UNAVAILABLE');
      return;
    }

    Promise.all([resolveAddress(resolver, 'resolve4'), resolveAddress(resolver, 'resolve6')]).then(results => {
      if (settled || !checkDeadline()) return;
      if (results.some(result => result.error !== null)) {
        finishError('DNS_UNAVAILABLE');
        return;
      }
      let addresses;
      try {
        addresses = [...readDnsRecord(results[0]), ...readDnsRecord(results[1])];
      } catch {
        finishError('DESTINATION_REJECTED');
        return;
      }
      if (addresses.length === 0 || addresses.some(address => !isPublicJevAddress(address))) {
        finishError(addresses.length === 0 ? 'DNS_UNAVAILABLE' : 'DESTINATION_REJECTED');
        return;
      }
      const pinnedAddress = addresses[0];
      try {
        activeAgent = new https.Agent({ keepAlive: false, maxSockets: 1, maxFreeSockets: 0 });
        requestObject = https.request(requestOptions(pinnedAddress, fields.apiKey, body, activeAgent), response => {
          if (settled || !checkDeadline()) return;
          try {
            responseStatus = response.statusCode;
            if (!Number.isInteger(responseStatus) || responseStatus < 100 || responseStatus > 599) {
              throw new JevTransportError('RESPONSE_LIMIT_EXCEEDED', 'possible');
            }
            responseHeaders = readResponseHeaders(response);
          } catch (error) {
            finishError(error instanceof JevTransportError ? 'RESPONSE_LIMIT_EXCEEDED' : 'TRANSPORT_OUTCOME_UNKNOWN');
            return;
          }
          response.on('data', chunk => {
            if (settled) return;
            if (!checkDeadline()) return;
            if (!Buffer.isBuffer(chunk)) {
              finishError('RESPONSE_LIMIT_EXCEEDED');
              return;
            }
            responseBytes += chunk.length;
            if (responseBytes > JEV_MAX_RESPONSE_BYTES) {
              finishError('RESPONSE_LIMIT_EXCEEDED');
              return;
            }
            if (responseStatus === 200) chunks.push(chunk);
          });
          response.once('aborted', () => finishError('TRANSPORT_OUTCOME_UNKNOWN'));
          response.once('error', () => finishError('TRANSPORT_OUTCOME_UNKNOWN'));
          response.once('end', () => {
            if (!checkDeadline()) return;
            if (responseStatus !== 200) {
              finishResult({
                kind: 'http_error',
                status: responseStatus,
                retryAfter: responseHeaders.retryAfter,
                providerRequestId: responseHeaders.providerRequestId,
                dispatchCertainty: 'possible'
              });
              return;
            }
            const responseBody = Buffer.concat(chunks, responseBytes);
            let validation;
            try {
              validation = validateJevResponseBytes(responseBody, request);
            } catch {
              finishError('TRANSPORT_OUTCOME_UNKNOWN');
              return;
            }
            finishResult({
              kind: 'response',
              status: responseStatus,
              validation,
              providerRequestId: responseHeaders.providerRequestId,
              dispatchCertainty: 'response_received'
            });
          });
        });
      } catch {
        finishError('TRANSPORT_FAILED_BEFORE_DISPATCH');
        return;
      }

      requestObject.once('error', () => finishError('TRANSPORT_OUTCOME_UNKNOWN'));
      requestObject.once('socket', socket => {
        activeSocket = socket;
        const send = () => {
          if (settled || !checkDeadline()) return;
          if (signalAborted(signal)) {
            finishError('ABORTED');
            return;
          }
          if (socket.authorized !== true) {
            finishError('TRANSPORT_FAILED_BEFORE_DISPATCH');
            return;
          }
          possibleWrite = true;
          try {
            requestObject.end(body);
          } catch {
            finishError('TRANSPORT_OUTCOME_UNKNOWN');
          }
        };
        if (socket && socket.encrypted === true) {
          if (socket.secureConnecting === false) send();
          else socket.once('secureConnect', send);
          socket.once('error', () => {
            if (!possibleWrite) finishError('TRANSPORT_FAILED_BEFORE_DISPATCH');
            else finishError('TRANSPORT_OUTCOME_UNKNOWN');
          });
        } else {
          finishError('TRANSPORT_FAILED_BEFORE_DISPATCH');
        }
      });
    }).catch(() => finishError('DNS_UNAVAILABLE'));
  });
}
