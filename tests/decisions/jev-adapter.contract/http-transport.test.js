import assert from 'node:assert/strict';
import https from 'node:https';
import { Resolver } from 'node:dns/promises';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import './network-guard.js';
import { isPublicJevAddress } from '../../../src/adapters/jev/address-policy.js';
import { sendJevOnce, JevTransportError } from '../../../src/adapters/jev/http-transport.js';
import { JEV_MAX_RESPONSE_BYTES } from '../../../src/adapters/jev/safe-json.js';
import { recordSyntheticMetric } from './synthetic-metrics.js';

const API_KEY_FIXTURE = 'synthetic-offline-key';

function fixture(name) {
  return JSON.parse(readFileSync(new URL(`../../fixtures/jev/${name}`, import.meta.url), 'utf8'));
}

function successBytes() {
  return readFileSync(new URL('../../fixtures/jev/positive-response.json', import.meta.url));
}

function installSyntheticDns(t, { ipv4 = ['93.184.216.34'], ipv6 = [] } = {}) {
  const calls = { resolve4: 0, resolve6: 0 };
  t.mock.method(Resolver.prototype, 'resolve4', function resolve4(host) {
    assert.equal(host, 'api.typesafe.ai');
    recordSyntheticMetric('controlled_dns_resolve4');
    calls.resolve4 += 1;
    return Promise.resolve([...ipv4]);
  });
  t.mock.method(Resolver.prototype, 'resolve6', function resolve6(host) {
    assert.equal(host, 'api.typesafe.ai');
    recordSyntheticMetric('controlled_dns_resolve6');
    calls.resolve6 += 1;
    return Promise.resolve([...ipv6]);
  });
  return calls;
}

function fakeSocket({ secure = true } = {}) {
  const socket = new EventEmitter();
  socket.encrypted = true;
  socket.authorized = false;
  socket.secureConnecting = true;
  socket.destroyed = false;
  socket.destroy = () => { socket.destroyed = true; };
  if (secure) {
    setImmediate(() => {
      socket.secureConnecting = false;
      socket.authorized = true;
      socket.emit('secureConnect');
    });
  }
  return socket;
}

function fakeResponse(statusCode, body, { headers = {}, rawHeaders = null } = {}) {
  const response = new EventEmitter();
  response.statusCode = statusCode;
  const defaults = {
    'content-type': 'application/json',
    'content-length': String(body.length)
  };
  response.headers = Object.assign(Object.create(null), defaults, headers);
  response.rawHeaders = rawHeaders ?? Object.entries(response.headers).flatMap(([name, value]) => [name, value]);
  return response;
}

function installSyntheticHttps(t, {
  status = 200,
  body = successBytes(),
  headers = {},
  rawHeaders = null,
  beforeResponse,
  onWrite
} = {}) {
  const observed = { calls: 0, sends: 0, options: null, requestBody: null, request: null };
  t.mock.method(https, 'request', (options, callback) => {
    recordSyntheticMetric('controlled_https_request');
    observed.calls += 1;
    observed.options = options;
    const request = new EventEmitter();
    request.destroyed = false;
    request.destroy = () => { request.destroyed = true; };
    request.end = bytes => {
      recordSyntheticMetric('controlled_https_write');
      observed.sends += 1;
      observed.requestBody = Buffer.from(bytes);
      if (onWrite) {
        onWrite({ request, callback, observed, body: Buffer.from(body), status, headers, rawHeaders });
        return request;
      }
      const response = fakeResponse(status, body, { headers, rawHeaders });
      setImmediate(() => {
        callback(response);
        if (beforeResponse) beforeResponse(response);
        if (body.length > 0) response.emit('data', Buffer.from(body));
        response.emit('end');
      });
      return request;
    };
    observed.request = request;
    const socket = fakeSocket({ secure: !onWrite?.stallTls });
    setImmediate(() => request.emit('socket', socket));
    return request;
  });
  return observed;
}

function validInput(extra = {}) {
  return {
    request: fixture('positive-request.json'),
    apiKey: API_KEY_FIXTURE,
    ...extra
  };
}

test('fixed_surface_ssrf: private transport sends one fixed HTTPS request and uses a pinned, vetted address', async t => {
  const dns = installSyntheticDns(t);
  const http = installSyntheticHttps(t, { headers: { 'x-typesafe-request-id': 'req-synthetic-01' } });
  const result = await sendJevOnce(validInput());

  assert.equal(dns.resolve4, 1);
  assert.equal(dns.resolve6, 1);
  assert.equal(http.calls, 1);
  assert.equal(http.sends, 1);
  assert.equal(http.options.protocol, 'https:');
  assert.equal(http.options.hostname, 'api.typesafe.ai');
  assert.equal(http.options.port, 443);
  assert.equal(http.options.method, 'POST');
  assert.equal(http.options.path, '/v1/systemone');
  assert.equal(http.options.servername, 'api.typesafe.ai');
  assert.equal(http.options.rejectUnauthorized, true);
  assert.equal(http.options.minVersion, 'TLSv1.2');
  assert.notEqual(http.options.agent, https.globalAgent);
  assert.deepEqual(Object.keys(http.options.headers).sort(), [
    'accept', 'authorization', 'connection', 'content-length', 'content-type'
  ]);
  assert.equal(http.options.headers.authorization, 'Bearer ' + API_KEY_FIXTURE);
  assert.equal(http.requestBody.toString('utf8'), JSON.stringify(fixture('positive-request.json')));

  const pinned = await new Promise((resolve, reject) => {
    http.options.lookup('api.typesafe.ai', {}, (error, address, family) => {
      if (error) reject(error);
      else resolve({ address, family });
    });
  });
  assert.deepEqual(pinned, { address: '93.184.216.34', family: 4 });
  const alternate = await new Promise(resolve => {
    http.options.lookup('api.typesafe.ai.attacker.invalid', {}, error => resolve(error));
  });
  assert.ok(alternate instanceof Error);
  assert.equal(result.kind, 'response');
  assert.equal(result.validation.kind, 'valid');
  assert.equal(result.providerRequestId, 'req-synthetic-01');
  assert.equal(result.dispatchCertainty, 'response_received');
});

test('fixed_surface_ssrf: address vetting rejects private, special, mapped, and mixed DNS answers before HTTP', async t => {
  for (const address of [
    '0.0.0.0', '10.1.2.3', '100.64.1.2', '127.0.0.1', '169.254.1.1',
    '172.16.0.1', '192.168.1.1', '198.18.0.1', '203.0.113.4',
    '::1', '::ffff:127.0.0.1', 'fc00::1', 'fe80::1', 'ff02::1', '2001:db8::1', '64:ff9b::7f00:1',
    '2001:4860:4860:0:0:5efe:7f00:1',
    '2001:4860:4860::5efe:a00:1',
    '2001:4860:4860::5efe:a9fe:1',
    '2001:4860:4860:0:200:5efe:7f00:1',
    '2001:4860:4860:0:200:5efe:a00:1'
  ]) assert.equal(isPublicJevAddress(address), false, address);
  assert.equal(isPublicJevAddress('93.184.216.34'), true);
  assert.equal(isPublicJevAddress('2606:4700:4700::1111'), true);

  installSyntheticDns(t, { ipv4: ['93.184.216.34', '192.168.1.2'] });
  const http = installSyntheticHttps(t);
  await assert.rejects(sendJevOnce(validInput()), error => {
    assert.ok(error instanceof JevTransportError);
    assert.equal(error.code, 'DESTINATION_REJECTED');
    assert.equal(error.dispatchCertainty, 'not_dispatched');
    return true;
  });
  assert.equal(http.calls, 0);
  assert.equal(http.sends, 0);

  for (const address of [
    '2001:4860:4860:0:0:5efe:7f00:1',
    '2001:4860:4860::5efe:a00:1',
    '2001:4860:4860::5efe:a9fe:1',
    '2001:4860:4860:0:200:5efe:7f00:1',
    '2001:4860:4860:0:200:5efe:a00:1'
  ]) {
    t.mock.restoreAll();
    installSyntheticDns(t, { ipv4: ['93.184.216.34'], ipv6: [address] });
    const mixedFamilyHttp = installSyntheticHttps(t);
    await assert.rejects(sendJevOnce(validInput()), error => {
      assert.ok(error instanceof JevTransportError);
      assert.equal(error.code, 'DESTINATION_REJECTED');
      assert.equal(error.dispatchCertainty, 'not_dispatched');
      return true;
    }, address);
    assert.equal(mixedFamilyHttp.calls, 0, address);
    assert.equal(mixedFamilyHttp.sends, 0, address);
  }

  t.mock.restoreAll();
  installSyntheticDns(t, { ipv4: [], ipv6: ['2606:4700:4700::1111'] });
  const publicIpv6Http = installSyntheticHttps(t);
  const publicIpv6Result = await sendJevOnce(validInput());
  assert.equal(publicIpv6Result.kind, 'response');
  assert.equal(publicIpv6Http.calls, 1);
  const pinnedIpv6 = await new Promise((resolve, reject) => {
    publicIpv6Http.options.lookup('api.typesafe.ai', {}, (error, address, family) => {
      if (error) reject(error);
      else resolve({ address, family });
    });
  });
  assert.deepEqual(pinnedIpv6, { address: '2606:4700:4700::1111', family: 6 });
});

test('fixed_surface_ssrf: test-controlled endpoint, method, headers, proxy, and TLS overrides are not accepted', () => {
  for (const extra of [
    { endpoint: 'https://api.typesafe.ai.attacker.invalid/v1/systemone' },
    { endpoint: 'https://user:pass@api.typesafe.ai/v1/systemone' },
    { endpoint: 'https://api.typesafe.ai:444/v1/systemone' },
    { endpoint: 'http://api.typesafe.ai/v1/systemone' },
    { endpoint: 'https://api.typesafe.ai/v1/systemone?to=attacker' },
    { endpoint: 'https://[::1]/v1/systemone' },
    { endpoint: 'https://api.typesafe.ai/%2f..' },
    { method: 'GET' },
    { headers: { authorization: 'Bearer attacker' } },
    { proxy: 'http://127.0.0.1:8080' },
    { rejectUnauthorized: false },
    { redirect: 'follow' }
  ]) {
    assert.throws(() => sendJevOnce(validInput(extra)), error => {
      assert.ok(error instanceof JevTransportError);
      assert.equal(error.code, 'INVALID_TRANSPORT_INPUT');
      assert.equal(error.dispatchCertainty, 'not_dispatched');
      return true;
    });
  }
});

test('fixed_surface_ssrf: DNS results are used once and the lookup closure keeps the selected address pinned', async t => {
  const dns = installSyntheticDns(t, { ipv4: ['93.184.216.34'] });
  let options;
  t.mock.method(https, 'request', (captured, callback) => {
    recordSyntheticMetric('controlled_https_request');
    options = captured;
    const request = new EventEmitter();
    request.destroyed = false;
    request.destroy = () => { request.destroyed = true; };
    request.end = () => {
      recordSyntheticMetric('controlled_https_write');
      const response = fakeResponse(302, Buffer.from('synthetic redirect'), {
        headers: { location: 'https://127.0.0.1/private' }
      });
      setImmediate(() => { callback(response); response.emit('data', Buffer.from('synthetic redirect')); response.emit('end'); });
    };
    const socket = fakeSocket();
    setImmediate(() => request.emit('socket', socket));
    return request;
  });

  const result = await sendJevOnce(validInput());
  assert.equal(result.kind, 'http_error');
  assert.equal(result.status, 302);
  assert.equal(result.dispatchCertainty, 'possible');
  assert.equal(dns.resolve4, 1);
  assert.equal(dns.resolve6, 1);
  const secondLookup = await new Promise((resolve, reject) => {
    options.lookup('api.typesafe.ai', { all: true }, (error, addresses) => {
      if (error) reject(error);
      else resolve(addresses);
    });
  });
  assert.deepEqual(secondLookup, [{ address: '93.184.216.34', family: 4 }]);
});

test('deadline_cancel: an already-aborted request performs zero DNS and HTTP work', t => {
  const controller = new AbortController();
  controller.abort();
  const dns = installSyntheticDns(t);
  const http = installSyntheticHttps(t);
  assert.throws(() => sendJevOnce(validInput({ signal: controller.signal })), error => {
    assert.equal(error.code, 'ABORTED_BEFORE_DISPATCH');
    assert.equal(error.dispatchCertainty, 'not_dispatched');
    return true;
  });
  assert.equal(dns.resolve4, 0);
  assert.equal(http.calls, 0);
});

test('deadline_cancel: abort during DNS cancels the resolver and never creates HTTP work', async t => {
  const controller = new AbortController();
  let calls = 0;
  let cancellations = 0;
  const pending = new Promise(() => {});
  t.mock.method(Resolver.prototype, 'resolve4', () => { recordSyntheticMetric('controlled_dns_resolve4'); calls += 1; return pending; });
  t.mock.method(Resolver.prototype, 'resolve6', () => { recordSyntheticMetric('controlled_dns_resolve6'); calls += 1; return pending; });
  t.mock.method(Resolver.prototype, 'cancel', () => { cancellations += 1; });
  const http = installSyntheticHttps(t);
  const result = sendJevOnce(validInput({ signal: controller.signal }));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls, 2);
  controller.abort();
  await assert.rejects(result, error => {
    assert.equal(error.code, 'ABORTED_BEFORE_DISPATCH');
    assert.equal(error.dispatchCertainty, 'not_dispatched');
    return true;
  });
  assert.equal(cancellations, 1);
  assert.equal(http.calls, 0);
});

test('deadline_cancel: abort during TLS closes the request before its single HTTP write', async t => {
  const controller = new AbortController();
  installSyntheticDns(t);
  const observed = { calls: 0, sends: 0, request: null };
  t.mock.method(https, 'request', () => {
    recordSyntheticMetric('controlled_https_request');
    observed.calls += 1;
    const request = new EventEmitter();
    request.destroyed = false;
    request.destroy = () => { request.destroyed = true; };
    request.end = () => { recordSyntheticMetric('controlled_https_write'); observed.sends += 1; };
    observed.request = request;
    const socket = fakeSocket({ secure: false });
    setImmediate(() => {
      request.emit('socket', socket);
      setImmediate(() => controller.abort());
    });
    return request;
  });
  await assert.rejects(sendJevOnce(validInput({ signal: controller.signal })), error => {
    assert.equal(error.code, 'ABORTED_BEFORE_DISPATCH');
    assert.equal(error.dispatchCertainty, 'not_dispatched');
    return true;
  });
  assert.equal(observed.calls, 1);
  assert.equal(observed.sends, 0);
  assert.equal(observed.request.destroyed, true);
});

test('deadline_cancel: abort after possible body write leaves an unknown dispatch outcome', async t => {
  const controller = new AbortController();
  installSyntheticDns(t);
  let observed;
  observed = installSyntheticHttps(t, {
    onWrite: ({ request, callback, body }) => {
      assert.ok(body.length > 0);
      const response = fakeResponse(200, body);
      setImmediate(() => {
        callback(response);
        response.emit('data', Buffer.from('{"partial":'));
        controller.abort();
      });
    }
  });
  await assert.rejects(sendJevOnce(validInput({ signal: controller.signal })), error => {
    assert.equal(error.code, 'ABORTED_OUTCOME_UNKNOWN');
    assert.equal(error.dispatchCertainty, 'possible');
    assert.doesNotMatch(error.message, /offline-key|partial|SYNTHETIC/u);
    return true;
  });
  assert.equal(observed.calls, 1);
  assert.equal(observed.sends, 1);
});

test('deadline_cancel: total deadline after a possible write closes work and does not replay', async t => {
  installSyntheticDns(t);
  let deadlineCallback;
  t.mock.method(globalThis, 'setTimeout', callback => {
    deadlineCallback = callback;
    return { syntheticTimer: true };
  });
  t.mock.method(globalThis, 'clearTimeout', () => {});
  let markWritten;
  const written = new Promise(resolve => { markWritten = resolve; });
  const http = installSyntheticHttps(t, {
    onWrite: () => { markWritten(); }
  });
  const result = sendJevOnce(validInput());
  await written;
  assert.equal(http.sends, 1);
  assert.equal(typeof deadlineCallback, 'function');
  deadlineCallback();
  await assert.rejects(result, error => {
    assert.equal(error.code, 'DEADLINE_OUTCOME_UNKNOWN');
    assert.equal(error.dispatchCertainty, 'possible');
    return true;
  });
  assert.equal(http.calls, 1);
  assert.equal(http.sends, 1);
});

test('retry_faults: interrupted and oversized responses never trigger a second HTTP send', async t => {
  installSyntheticDns(t);
  const hugeHeaders = Array.from({ length: 130 }, (_, index) => [`x-header-${index}`, 'x']).flat();
  const headerHttp = installSyntheticHttps(t, { rawHeaders: hugeHeaders });
  await assert.rejects(sendJevOnce(validInput()), error => {
    assert.equal(error.code, 'RESPONSE_LIMIT_EXCEEDED');
    assert.equal(error.dispatchCertainty, 'possible');
    return true;
  });
  assert.equal(headerHttp.calls, 1);
  assert.equal(headerHttp.sends, 1);

  t.mock.restoreAll();
  installSyntheticDns(t);
  const bodyHttp = installSyntheticHttps(t, {
    body: Buffer.alloc(JEV_MAX_RESPONSE_BYTES + 1, 0x20),
    headers: { 'content-length': undefined, 'transfer-encoding': 'chunked' },
    rawHeaders: ['content-type', 'application/json', 'transfer-encoding', 'chunked']
  });
  await assert.rejects(sendJevOnce(validInput()), error => {
    assert.equal(error.code, 'RESPONSE_LIMIT_EXCEEDED');
    assert.equal(error.dispatchCertainty, 'possible');
    return true;
  });
  assert.equal(bodyHttp.calls, 1);
  assert.equal(bodyHttp.sends, 1);
});

test('privacy_identity: provider error body and credential bytes never enter returned errors or HTTP results', async t => {
  installSyntheticDns(t);
  const privateMarker = 'SYNTHETIC_PRIVATE_PROVIDER_BODY';
  const http = installSyntheticHttps(t, { status: 401, body: Buffer.from(privateMarker) });
  const result = await sendJevOnce(validInput());
  assert.equal(result.kind, 'http_error');
  assert.equal(result.status, 401);
  assert.equal(Object.hasOwn(result, 'body'), false);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(privateMarker));
  assert.doesNotMatch(JSON.stringify(result), new RegExp(API_KEY_FIXTURE));
  assert.equal(http.calls, 1);
});

test('retry_faults: 529 transport response provides bounded retry metadata without replaying the request', async t => {
  installSyntheticDns(t);
  const http = installSyntheticHttps(t, {
    status: 529,
    body: Buffer.from('SYNTHETIC_PROVIDER_ERROR_BODY'),
    headers: { 'retry-after': '5', 'x-typesafe-request-id': 'req-overloaded' }
  });
  const result = await sendJevOnce(validInput());
  assert.equal(result.kind, 'http_error');
  assert.equal(result.status, 529);
  assert.equal(result.retryAfter, '5');
  assert.equal(result.providerRequestId, 'req-overloaded');
  assert.equal(Object.hasOwn(result, 'body'), false);
  assert.equal(http.calls, 1);
  assert.equal(http.sends, 1);
  assert.doesNotMatch(JSON.stringify(result), /SYNTHETIC_PROVIDER_ERROR_BODY/u);
});

test('fixed_surface_ssrf: source URLs in state are data and never become network destinations', async t => {
  const dns = installSyntheticDns(t);
  const http = installSyntheticHttps(t);
  const request = fixture('positive-request.json');
  request.state.source_reference = 'https://127.0.0.1/private?token=synthetic';
  const result = await sendJevOnce({ request, apiKey: API_KEY_FIXTURE });
  assert.equal(result.kind, 'response');
  assert.equal(dns.resolve4, 1);
  assert.equal(dns.resolve6, 1);
  assert.equal(http.options.hostname, 'api.typesafe.ai');
});
