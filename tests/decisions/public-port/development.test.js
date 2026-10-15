import assert from 'node:assert/strict';
import test from 'node:test';
import { invokeAgentCreativeDevelopment } from '@jevelopers/agent-harness-runtime/development';

const pins = { application: '1'.repeat(40), agent_harness: '2'.repeat(40), creative_engine: '3'.repeat(40) };

test('development driver delegates to the same host with exact loaded component pins', async () => {
  let calls = 0;
  const host = { environment: 'test', source_manifest: pins,
    async invoke(request) { calls++; assert.equal(request.environment, 'test'); return { status: 'unavailable' }; } };
  const output = await invokeAgentCreativeDevelopment({ host, request: { environment: 'test' }, source_manifest: pins });
  assert.equal(calls, 1); assert.equal(output.evidence_mode, 'development_only');
  assert.equal(output.response.status, 'unavailable');
});

test('production, moving pins and mismatched manifests cannot use the development driver', async () => {
  let calls = 0;
  const host = { environment: 'test', source_manifest: pins, async invoke() { calls++; } };
  for (const options of [
    { host: { ...host, environment: 'production' }, request: { environment: 'production' }, source_manifest: pins },
    { host, request: { environment: 'test' }, source_manifest: { ...pins, agent_harness: 'main' } },
    { host, request: { environment: 'test' }, source_manifest: { ...pins, agent_harness: 'a'.repeat(40) } },
    { host, request: { environment: 'production' }, source_manifest: pins },
  ]) await assert.rejects(invokeAgentCreativeDevelopment(options));
  assert.equal(calls, 0);
});

test('matching hidden required pins cannot omit moving refs from the recorded manifest', async () => {
  const hidden = {};
  for (const key of Object.keys(pins)) {
    Object.defineProperty(hidden, key, { value: 'main', enumerable: false });
  }
  let calls = 0;
  const host = { environment: 'test', source_manifest: hidden, async invoke() { calls++; } };
  await assert.rejects(
    invokeAgentCreativeDevelopment({ host, request: { environment: 'test' }, source_manifest: hidden }),
    { name: 'JevDecisionError', code: 'CONFIG_INVALID' },
  );
  assert.equal(calls, 0);
});

for (const location of ['caller', 'host']) {
  test(`${location} manifest rejects every hidden or symbolic property before invocation`, async () => {
    const cases = [
      ...Object.keys(pins).map(key => ({ key, enumerable: false, value: pins[key] })),
      { key: 'event_intelligence', enumerable: false, value: 'main' },
      { key: 'finance', enumerable: false, value: 'main' },
      { key: 'unknown_component', enumerable: false, value: '4'.repeat(40) },
      { key: Symbol('component'), enumerable: true, value: '4'.repeat(40) },
      { key: Symbol('hidden_component'), enumerable: false, value: '4'.repeat(40) },
    ];
    for (const { key, enumerable, value } of cases) {
      const malformed = { ...pins };
      Object.defineProperty(malformed, key, { value, enumerable });
      let calls = 0;
      const host = { environment: 'test', source_manifest: location === 'host' ? malformed : pins,
        async invoke() { calls++; } };
      await assert.rejects(
        invokeAgentCreativeDevelopment({ host, request: { environment: 'test' },
          source_manifest: location === 'caller' ? malformed : pins }),
        { name: 'JevDecisionError', code: 'CONFIG_INVALID' },
        `${location}: ${String(key)}`,
      );
      assert.equal(calls, 0);
    }
  });

  test(`${location} manifest refuses accessors without evaluating them`, async () => {
    for (const key of [...Object.keys(pins), 'event_intelligence', 'finance']) {
      for (const enumerable of [true, false]) {
        let reads = 0;
        let calls = 0;
        const malformed = { ...pins };
        Object.defineProperty(malformed, key, { enumerable, get() { reads++; return '4'.repeat(40); } });
        const host = { environment: 'test', source_manifest: location === 'host' ? malformed : pins,
          async invoke() { calls++; } };
        await assert.rejects(
          invokeAgentCreativeDevelopment({ host, request: { environment: 'test' },
            source_manifest: location === 'caller' ? malformed : pins }),
          { name: 'JevDecisionError', code: 'CONFIG_INVALID' },
          `${location}: ${key}, enumerable=${enumerable}`,
        );
        assert.equal(reads, 0);
        assert.equal(calls, 0);
      }
    }
  });
}

test('complete manifests accept optional pins and equivalent ordering without losing provenance', async () => {
  const supplied = Object.assign(Object.create(null), {
    finance: '5'.repeat(40), creative_engine: pins.creative_engine,
    event_intelligence: '4'.repeat(40), agent_harness: pins.agent_harness, application: pins.application,
  });
  const loaded = { ...pins, event_intelligence: supplied.event_intelligence, finance: supplied.finance };
  let calls = 0;
  const host = { environment: 'test', source_manifest: loaded, async invoke() { calls++; return 'synthetic'; } };
  const output = await invokeAgentCreativeDevelopment({ host, request: { environment: 'test' }, source_manifest: supplied });
  assert.equal(calls, 1);
  assert.deepEqual({ ...output.source_manifest }, loaded);
  assert.equal(Object.isFrozen(output.source_manifest), true);
  supplied.finance = '6'.repeat(40);
  assert.equal(output.source_manifest.finance, '5'.repeat(40));
});
