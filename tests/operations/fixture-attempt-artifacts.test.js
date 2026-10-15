import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseFixtureAttemptArtifacts } from './fixture-attempt-artifacts.js';

function history(kind) {
  return { schema: 'agent-harness.ops-fixture-busy-retry.v1', kind, stopReason: 'success',
    maxAttempts: 8, maxRetryMs: 1_500, attempts: [
      { attempt: 1, outcome: kind === 'claim' ? 'error' : 'refused',
        code: kind === 'claim' ? 'STORE_BUSY' : 'OPS_BACKPRESSURE',
        reasonCode: kind === 'claim' ? 'none' : 'STORE_BUSY', eligible: true, restored: true,
        boundary: 'begin', nativePrimaryCode: 5, beforeInactive: true, afterInactive: true,
        retryScheduled: true, delayMs: 40 },
      { attempt: 2, outcome: kind === 'claim' ? 'claim' : 'accepted', code: 'none', reasonCode: 'none',
        eligible: false, restored: true, boundary: 'unproven', nativePrimaryCode: null,
        beforeInactive: null, afterInactive: null, retryScheduled: false, delayMs: 0 }
    ] };
}

function frames() {
  return [
    ...['expired', 'terminal'].flatMap(race => [0, 1].map(slot =>
      `    # OPS05_C12_RETRY_V1 ${JSON.stringify({ race, slot, diagnostic: history('claim') })}`)),
    ...[0, 1, 2].map(slot => `    # OPS05_C08_RETRY_V1 ${JSON.stringify({ slot, diagnostic: history('admission') })}`)
  ];
}

test('both recovery races and all supervisors retain their original refused attempts', () => {
  const input = `TAP version 13\n${frames().join('\n')}\n# pass 66\n`;
  const result = parseFixtureAttemptArtifacts(input);
  assert.equal(result.valid, true); assert.equal(result.complete, true);
  assert.equal(result.observedCount, 7); assert.deepEqual(result.missing, []);
  assert.ok(result.records.every(record => record.diagnostic.attempts.length === 2));
  assert.ok(result.records.every(record => record.diagnostic.attempts[0].retryScheduled));
  assert.equal(result.records[0].diagnostic.attempts[0].code, 'STORE_BUSY');
  assert.equal(result.records.at(-1).diagnostic.attempts[0].reasonCode, 'STORE_BUSY');
  assert.equal(result.records.at(-1).diagnostic.attempts[1].outcome, 'accepted');
});

test('missing and unavailable histories remain explicit and cannot become complete evidence', () => {
  const input = frames();
  input[0] = '    # OPS05_C12_RETRY_V1 {"race":"expired","slot":0,"diagnostic":null}';
  const result = parseFixtureAttemptArtifacts(input.slice(0, -1).join('\n') + '\n');
  assert.equal(result.valid, true); assert.equal(result.complete, false);
  assert.deepEqual(result.unavailable, ['OPS05.C12:expired:0']);
  assert.deepEqual(result.missing, ['OPS05.C08:2']);
  assert.equal(parseFixtureAttemptArtifacts('TAP version 13\n').reason, 'missing');
});

test('unknown versions or malformed reserved family markers cannot accompany complete evidence', () => {
  for (const extra of ['# OPS05_C08_RETRY_V2 {}', '# OPS05_C12_RETRY_V1',
    '# OPS05_C08_RETRY_V1{}', '# OPS05_C12_RETRY_FUTURE {}',
    '#OPS05_C08_RETRY_V1{}', '\t#\tOPS05_C12_RETRY_V1 {}']) {
    const result = parseFixtureAttemptArtifacts([...frames(), extra].join('\n') + '\n');
    assert.equal(result.valid, false); assert.equal(result.complete, false);
    assert.equal(result.records.length, 7);
    assert.equal(result.invalidFrames, 1);
  }
});

test('duplicate, malformed, private, mismatched and truncated input preserves only bounded closed records', () => {
  const lines = frames();
  const duplicate = parseFixtureAttemptArtifacts([lines[0], lines[0]].join('\n') + '\n');
  assert.equal(duplicate.valid, false); assert.equal(duplicate.reason, 'duplicate');
  assert.equal(duplicate.records.length, 1);
  const extra = JSON.stringify({ slot: 0, diagnostic: history('admission'), secret: 'PRIVATE_CANARY' });
  const badKind = JSON.stringify({ slot: 0, diagnostic: history('claim') });
  for (const line of [`# OPS05_C08_RETRY_V1 ${extra}`, `# OPS05_C08_RETRY_V1 ${badKind}`,
    '# OPS05_C08_RETRY_V1 {"slot":0,"diagnostic":"PRIVATE_CANARY"}',
    '# OPS05_C08_RETRY_V1 {"slot":999,"diagnostic":null}',
    '# OPS05_C12_RETRY_V1 PRIVATE_CANARY']) {
    const result = parseFixtureAttemptArtifacts(line + '\n');
    assert.equal(result.valid, false); assert.equal(result.complete, false);
    assert.doesNotMatch(JSON.stringify(result), /PRIVATE_CANARY|999/u);
  }
  assert.equal(parseFixtureAttemptArtifacts(lines.join('\n')).reason, 'truncated');
  assert.equal(parseFixtureAttemptArtifacts('x'.repeat(2 * 1024 * 1024 + 1)).reason, 'oversize');
  assert.equal(parseFixtureAttemptArtifacts(null).reason, 'invalid_input');
});
