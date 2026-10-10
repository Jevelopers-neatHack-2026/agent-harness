import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import {
  assertExecutionModeAvailable,
  invokeInjected,
  parseRuntimeConfig,
  RuntimeError
} from '@jevelopers/agent-harness-runtime';

const validSettings = Object.freeze({
  HARNESS_MODE: 'mock',
  HARNESS_MODEL_ID: 'gpt-6-luna',
  HARNESS_DEADLINE_MS: '12000',
  HARNESS_MAX_ATTEMPTS: '3',
  HARNESS_MAX_ELAPSED_MS: '90000',
  HARNESS_MAX_COST_MINOR_UNITS: '0'
});

test('configuration requires explicit bounds and permits additive settings', () => {
  const config = parseRuntimeConfig({
    ...validSettings,
    FUTURE_ADDITIVE_SETTING: 'ignored'
  });
  assert.equal(config.mode, 'mock');
  assert.equal(config.modelId, 'gpt-6-luna');
  assert.equal(config.maxCostMinorUnits, 0);

  for (const key of Object.keys(validSettings)) {
    const incomplete = { ...validSettings };
    delete incomplete[key];
    assert.throws(
      () => parseRuntimeConfig(incomplete),
      error => error instanceof RuntimeError && error.code === 'CONFIG_MISSING'
    );
  }

  for (const [key, value] of [
    ['HARNESS_DEADLINE_MS', '0'],
    ['HARNESS_MAX_ATTEMPTS', '-1'],
    ['HARNESS_MAX_ELAPSED_MS', '1.5'],
    ['HARNESS_MAX_COST_MINOR_UNITS', 'NaN'],
    ['HARNESS_MODEL_ID', 'latest']
  ]) {
    assert.throws(
      () => parseRuntimeConfig({ ...validSettings, [key]: value }),
      error => error instanceof RuntimeError && error.code === 'CONFIG_INVALID'
    );
  }
  assert.throws(
    () => parseRuntimeConfig({ ...validSettings, HARNESS_MODE: '' }),
    error => error instanceof RuntimeError && error.code === 'CONFIG_MISSING'
  );
});

test('live mode fails closed without canonical authorities', () => {
  const config = parseRuntimeConfig({ ...validSettings, HARNESS_MODE: 'live' });
  assert.throws(
    () => assertExecutionModeAvailable(config),
    error => error instanceof RuntimeError
      && error.code === 'LIVE_EXECUTION_UNAVAILABLE'
      && error.safe_message.includes('canonical ports')
  );
});

test('injected boundary errors are safe and malformed results fail closed', async () => {
  const privateMarker = 'private-fixture-value-do-not-disclose';
  await assert.rejects(
    invokeInjected('fixture.lookup', async () => {
      throw new Error(privateMarker);
    }, () => true),
    error => {
      assert.ok(error instanceof RuntimeError);
      assert.equal(error.code, 'PORT_UNAVAILABLE');
      assert.equal(error.retryable, true);
      assert.ok(!JSON.stringify(error).includes(privateMarker));
      assert.ok(!error.safe_message.includes(privateMarker));
      assert.ok(error.diagnostic_ref.startsWith('diag-'));
      return true;
    }
  );

  await assert.rejects(
    invokeInjected('fixture.validate', async () => ({ allowed: false }), value => value?.allowed === true),
    error => error instanceof RuntimeError && error.code === 'PORT_INVALID_RESULT'
  );
});

test('internal package import remains experimental primitives', async () => {
  const publicSurface = await import('@jevelopers/agent-harness-runtime');
  assert.equal(typeof publicSurface.DurableWorkflowStore.open, 'function');
  assert.equal(typeof publicSurface.parseRuntimeConfig, 'function');
  assert.equal('ResearchPort' in publicSurface, false);
  assert.equal('CreativePort' in publicSurface, false);
  assert.equal('DecisionPort' in publicSurface, false);
});

test('example environment keeps the image access preference distinct from an adapter', () => {
  const contents = readFileSync(new URL('../../.env.example', import.meta.url), 'utf8');
  assert.match(contents, /^TYPESAFE_API_KEY=$/m);
  assert.match(contents, /^GPT_IMAGE_ACCESS_MODE=codex_subscription$/m);
  assert.match(contents, /^GPT_IMAGE_MODEL=gpt-image-2$/m);
  assert.doesNotMatch(contents, /API_KEY=.+/);
  assert.match(contents, /do not configure a GPT Image adapter/i);
});
