import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import './network-guard.js';
import {
  JEV_MAX_REQUEST_BYTES,
  serializeJevRequest,
  validateJevRequest
} from '../../../src/adapters/jev/request-validation.js';
import { validateJevResponseBytes } from '../../../src/adapters/jev/response-validation.js';
import {
  JEV_MAX_RESPONSE_BYTES,
  JevWireError,
  parseStrictJsonBytes
} from '../../../src/adapters/jev/safe-json.js';
import { JevTransportError, sendJevOnce } from '../../../src/adapters/jev/http-transport.js';

function fixture(name) {
  return JSON.parse(readFileSync(new URL(`../../fixtures/jev/${name}`, import.meta.url), 'utf8'));
}

function responseBytes(value) {
  return Buffer.from(JSON.stringify(value), 'utf8');
}

function validPair() {
  return {
    request: fixture('positive-request.json'),
    response: fixture('positive-response.json')
  };
}

function validateEditedResponse(edit) {
  const { request, response } = validPair();
  edit(response);
  return validateJevResponseBytes(responseBytes(response), request);
}

function assertInvalid(edit) {
  const result = validateEditedResponse(edit);
  assert.equal(result.kind, 'invalid');
  assert.equal(result.code, 'INVALID_RESPONSE');
  return result;
}

test('wire_positive: actual JEV envelope validates Choice ties, fractional Score, structured legends, and Noul 0/0.5/1', () => {
  const { request, response } = validPair();
  const wireRequest = validateJevRequest(request);
  const encoded = serializeJevRequest(wireRequest);
  const result = validateJevResponseBytes(responseBytes(response), wireRequest);

  assert.equal(encoded.request.model, 'jev-1.13.0');
  assert.ok(encoded.byteLength <= JEV_MAX_REQUEST_BYTES);
  assert.equal(result.kind, 'valid');
  assert.equal(result.requestedModel, 'jev-1.13.0');
  assert.equal(result.resolvedModel, 'jev-1.13.0');
  assert.deepEqual(result.usage, { kind: 'known', inputTokens: 83, outputTokens: 19 });
  assert.equal(result.answers.routing.choice, 'support');
  assert.equal(result.answers.impact.score, 1.75);
  assert.equal(result.answers.noul_no.noul, 0);
  assert.equal(result.answers.noul_uncertain.noul, 0.5);
  assert.equal(result.answers.noul_yes.noul, 1);
  assert.equal(Object.hasOwn(result.answers.noul_yes, 'confidence'), false);
  assert.ok(Object.isFrozen(wireRequest));
  assert.ok(Object.isFrozen(wireRequest.questions));
  assert.ok(Object.isFrozen(wireRequest.questions.impact.criteria[0]));
  assert.ok(Object.isFrozen(result.answers));
});

test('wire_adversarial: rejects schema, range, distribution, model, and structured legend mismatches', () => {
  const cases = [
    ['missing answer id', response => { delete response.answers.noul_yes; }],
    ['extra answer id', response => { response.answers.extra = { type: 'noul', noul: 0.5 }; }],
    ['wrong answer type', response => { response.answers.noul_no.type = 'choice'; }],
    ['unknown selected Choice option', response => { response.answers.routing.choice = 'other'; }],
    ['incomplete Choice distribution', response => { delete response.answers.routing.probabilities.billing; }],
    ['negative probability', response => { response.answers.routing.probabilities.support = -0.01; }],
    ['probability over one', response => { response.answers.routing.probabilities.support = 1.01; }],
    ['probability sum outside tolerance', response => { response.answers.routing.probabilities.support = 0.51; }],
    ['string probability', response => { response.answers.routing.probabilities.support = '0.5'; }],
    ['confidence below range', response => { response.answers.routing.confidence = -0.1; }],
    ['confidence above range', response => { response.answers.impact.confidence = 1.01; }],
    ['Noul below range', response => { response.answers.noul_no.noul = -0.1; }],
    ['Noul above range', response => { response.answers.noul_yes.noul = 1.1; }],
    ['Noul invents confidence', response => { response.answers.noul_yes.confidence = 0.9; }],
    ['Score below range', response => { response.answers.impact.score = -0.1; }],
    ['Score above range', response => { response.answers.impact.score = 2.1; }],
    ['Score not equal to weighted probabilities', response => { response.answers.impact.score = 1.8; }],
    ['missing Score legend level', response => { delete response.answers.impact.legend['2']; }],
    ['mismatched structured Score legend binding', response => { response.answers.impact.legend['1'].label = 'high'; }],
    ['mismatched resolved model', response => { response.model = 'jev-preview'; }],
    ['unknown response field', response => { response.extra = 'no authority'; }],
    ['malformed usage', response => { response.usage.input_tokens = '83'; }]
  ];
  for (const [name, edit] of cases) {
    const result = assertInvalid(edit);
    assert.equal(result.usage.kind, name === 'malformed usage' ? 'unknown' : 'known', name);
  }
});

test('wire_adversarial: valid usage survives a bad decision without promoting that decision', () => {
  const result = assertInvalid(response => { response.answers.routing.choice = 'not-an-option'; });
  assert.deepEqual(result.usage, { kind: 'known', inputTokens: 83, outputTokens: 19 });
  assert.equal(Object.hasOwn(result, 'answers'), false);
});

test('wire_adversarial: documented numerical tolerances accept rounded values and reject values outside policy', () => {
  const roundedProbabilities = validateEditedResponse(response => {
    response.answers.routing.probabilities.support += 0.009;
  });
  const roundedScore = validateEditedResponse(response => {
    response.answers.impact.score = 1.769;
  });
  const excessProbabilityRounding = assertInvalid(response => {
    response.answers.routing.probabilities.support += 0.011;
  });
  const excessScoreRounding = assertInvalid(response => {
    response.answers.impact.score = 1.771;
  });

  assert.equal(roundedProbabilities.kind, 'valid');
  assert.equal(roundedScore.kind, 'valid');
  assert.equal(excessProbabilityRounding.usage.kind, 'known');
  assert.equal(excessScoreRounding.usage.kind, 'known');
});

test('wire_adversarial: Score legend object key order is immaterial but level bindings must match', () => {
  const response = fixture('positive-response.json');
  const legend = response.answers.impact.legend;
  response.answers.impact.legend = { '2': legend['2'], '0': legend['0'], '1': legend['1'] };
  const result = validateJevResponseBytes(responseBytes(response), fixture('positive-request.json'));
  assert.equal(result.kind, 'valid');
});

test('wire_adversarial: older response envelope is invalid and its usage is not assumed current', () => {
  const result = validateJevResponseBytes(
    readFileSync(new URL('../../fixtures/jev/legacy-response.json', import.meta.url)),
    fixture('positive-request.json')
  );
  assert.equal(result.kind, 'invalid');
  assert.equal(result.resolvedModel, 'jev-1.12.0');
  assert.deepEqual(result.usage, { kind: 'known', inputTokens: 83, outputTokens: 19 });
});

test('wire_adversarial: missing, invalid, or unparseable usage stays unknown instead of zero', () => {
  const missing = assertInvalid(response => { delete response.usage; });
  const invalid = assertInvalid(response => { response.usage.output_tokens = -1; });
  assert.deepEqual(missing.usage, { kind: 'unknown', reason: 'missing_or_invalid' });
  assert.deepEqual(invalid.usage, { kind: 'unknown', reason: 'missing_or_invalid' });

  for (const bytes of [
    Buffer.alloc(0),
    Buffer.from('{"a":1,"a":2}', 'utf8'),
    Buffer.from([0xc3, 0x28]),
    Buffer.from('{"a":NaN}', 'utf8'),
    Buffer.from('{"a":1e999}', 'utf8')
  ]) {
    const result = validateJevResponseBytes(bytes, fixture('positive-request.json'));
    assert.equal(result.kind, 'invalid');
    assert.equal(result.usage.kind, 'unknown');
  }
});

test('wire_adversarial: strict parser rejects duplicate nested members, excess depth, and oversized bodies', () => {
  assert.throws(() => parseStrictJsonBytes(Buffer.from('{"a":{"x":1,"x":2}}')), JevWireError);
  assert.throws(() => parseStrictJsonBytes(Buffer.from('['.repeat(60) + '0' + ']'.repeat(60))), JevWireError);
  assert.throws(() => parseStrictJsonBytes(Buffer.alloc(512 * 1024 + 1, 0x20)), JevWireError);
});

test('wire_adversarial: parser uses intrinsic Buffer byte length and rejects shadowed metadata before decoding', () => {
  let getterCalls = 0;
  const accessorLength = Buffer.from('{}');
  Object.defineProperty(accessorLength, 'length', {
    configurable: true,
    get() { getterCalls += 1; throw new Error('SYNTHETIC_PRIVATE_BUFFER_LENGTH'); }
  });
  assert.throws(() => parseStrictJsonBytes(accessorLength), error => {
    assert.ok(error instanceof JevWireError);
    assert.equal(error.code, 'INVALID_RESPONSE');
    assert.doesNotMatch(error.message, /SYNTHETIC_PRIVATE_BUFFER_LENGTH/u);
    return true;
  });
  assert.equal(getterCalls, 0);

  const oversizedShadow = Buffer.concat([Buffer.alloc(JEV_MAX_RESPONSE_BYTES + 1, 0x20)]);
  Object.defineProperty(oversizedShadow, 'length', { configurable: true, value: 1 });
  assert.throws(() => parseStrictJsonBytes(oversizedShadow), error => {
    assert.ok(error instanceof JevWireError);
    assert.equal(error.code, 'INVALID_RESPONSE');
    return true;
  });

  const exactLimit = Buffer.concat([
    Buffer.from('{}'),
    Buffer.alloc(JEV_MAX_RESPONSE_BYTES - 2, 0x20)
  ]);
  assert.equal(JEV_MAX_RESPONSE_BYTES, exactLimit.length);
  assert.deepEqual(parseStrictJsonBytes(exactLimit), Object.freeze(Object.create(null)));
  assert.throws(() => parseStrictJsonBytes(Buffer.concat([exactLimit, Buffer.from(' ')])), JevWireError);

  let prototypeGetterCalls = 0;
  const customPrototypeBuffer = Buffer.from('{}');
  const customPrototype = Object.create(Buffer.prototype);
  Object.defineProperty(customPrototype, 'length', {
    get() { prototypeGetterCalls += 1; throw new Error('SYNTHETIC_PRIVATE_BUFFER_PROTOTYPE'); }
  });
  Object.setPrototypeOf(customPrototypeBuffer, customPrototype);
  assert.throws(() => parseStrictJsonBytes(customPrototypeBuffer), JevWireError);
  assert.equal(prototypeGetterCalls, 0);

  const revoked = Proxy.revocable(Buffer.from('{}'), {});
  revoked.revoke();
  assert.throws(() => parseStrictJsonBytes(revoked.proxy), error => {
    assert.ok(error instanceof JevWireError);
    assert.equal(error.code, 'INVALID_RESPONSE');
    return true;
  });
});

test('wire_adversarial: revoked request proxies fail as typed transport input errors', () => {
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  assert.throws(() => sendJevOnce(revoked.proxy), error => {
    assert.ok(error instanceof JevTransportError);
    assert.equal(error.code, 'INVALID_TRANSPORT_INPUT');
    assert.doesNotMatch(error.message, /Cannot perform|revoked|SYNTHETIC/u);
    return true;
  });
});

test('wire_adversarial: request snapshots reject accessors, proxies, toJSON hooks, and unsupported models without invoking them', () => {
  let calls = 0;
  const getterState = Object.defineProperty({}, 'private', {
    enumerable: true,
    get() { calls += 1; throw new Error('SYNTHETIC_PRIVATE_SENTINEL'); }
  });
  const getterRequest = { ...fixture('positive-request.json'), state: getterState };
  assert.throws(() => validateJevRequest(getterRequest), JevWireError);
  assert.equal(calls, 0);

  const proxied = new Proxy(fixture('positive-request.json'), {
    ownKeys() { calls += 1; throw new Error('SYNTHETIC_PRIVATE_SENTINEL'); }
  });
  assert.throws(() => validateJevRequest(proxied), JevWireError);
  assert.equal(calls, 0);

  const withToJson = fixture('positive-request.json');
  Object.defineProperty(withToJson.state, 'toJSON', {
    enumerable: true,
    value() { calls += 1; throw new Error('SYNTHETIC_PRIVATE_SENTINEL'); }
  });
  assert.throws(() => serializeJevRequest(withToJson), JevWireError);
  assert.equal(calls, 0);

  const unsupported = fixture('positive-request.json');
  unsupported.model = 'jev-latest';
  assert.throws(() => validateJevRequest(unsupported), JevWireError);

  const tooLarge = fixture('positive-request.json');
  tooLarge.state = 'x'.repeat(JEV_MAX_REQUEST_BYTES);
  assert.throws(() => serializeJevRequest(tooLarge), JevWireError);
});

test('wire_adversarial: own-key maps preserve prototype-like names as data without pollution', () => {
  const request = {
    model: 'jev-1.13.0',
    state: 'synthetic',
    questions: Object.create(null)
  };
  Object.defineProperty(request.questions, '__proto__', {
    value: { type: 'choice', instructions: 'Choose a fixture label.', criteria: Object.create(null) },
    enumerable: true
  });
  Object.defineProperty(request.questions.__proto__.criteria, 'constructor', { value: null, enumerable: true });
  Object.defineProperty(request.questions.__proto__.criteria, 'toString', { value: null, enumerable: true });
  const response = Object.create(null);
  const answers = Object.create(null);
  const probabilities = Object.create(null);
  Object.defineProperty(probabilities, 'constructor', { value: 1, enumerable: true });
  Object.defineProperty(probabilities, 'toString', { value: 0, enumerable: true });
  Object.defineProperty(answers, '__proto__', {
    value: { type: 'choice', choice: 'constructor', probabilities, confidence: 1 },
    enumerable: true
  });
  Object.assign(response, {
    model: 'jev-1.13.0',
    answers,
    usage: { input_tokens: 1, output_tokens: 1 }
  });
  const result = validateJevResponseBytes(responseBytes(response), request);
  assert.equal(result.kind, 'valid');
  assert.equal(Object.prototype.polluted, undefined);
  assert.ok(Object.hasOwn(result.answers, '__proto__'));
});

test('batch_semantics: each answer uses one immutable shared state; a dependent answer appears only in a later explicit request', () => {
  const first = validPair();
  const firstRequest = validateJevRequest(first.request);
  const firstResult = validateJevResponseBytes(responseBytes(first.response), firstRequest);
  assert.equal(firstResult.kind, 'valid');
  assert.equal(Object.hasOwn(firstRequest.state, 'prior_answer'), false);
  assert.equal(Object.keys(firstRequest.questions).length, 5);

  const laterRequest = validateJevRequest({
    model: 'jev-1.13.0',
    state: {
      ticket: firstRequest.state.ticket,
      prior_answer: firstResult.answers.noul_yes.noul
    },
    questions: {
      follow_up: { type: 'noul', instructions: 'Does the later synthetic state meet the follow-up condition?' }
    }
  });
  assert.equal(laterRequest.state.prior_answer, 1);
  assert.equal(Object.hasOwn(firstRequest.state, 'prior_answer'), false);
});
