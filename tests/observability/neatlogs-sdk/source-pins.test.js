import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

function sha256(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

function mappedSource(relativePath) {
  const map = JSON.parse(readFileSync('node_modules/neatlogs/dist/index.mjs.map', 'utf8'));
  const entries = map.sources
    .map((source, index) => ({ source, content: map.sourcesContent[index] }))
    .filter(item => item.source.endsWith(`/${relativePath}`));
  assert.equal(entries.length, 1);
  return entries[0].content;
}

test('Neatlogs and OpenTelemetry source pins preserve mask-drop and partial-success findings', () => {
  const lock = JSON.parse(readFileSync('package-lock.json', 'utf8'));
  const packages = lock.packages;
  assert.equal(packages['node_modules/neatlogs'].version, '1.1.28');
  assert.equal(packages['node_modules/@opentelemetry/sdk-trace-base'].version, '1.30.1');
  assert.equal(packages['node_modules/@opentelemetry/otlp-exporter-base'].version, '0.57.2');
  assert.equal(
    packages['node_modules/neatlogs'].integrity,
    'sha512-KeOEzjufhhTaxF7pckw08JOaILQvjj8HpS+Xb+3owMzQfjEtjYG01Oo2WtOGurifuTNMMabjw3WSRDi8a0thmg=='
  );

  const maskSource = mappedSource('src/core/mask.ts');
  const filterSource = mappedSource('src/core/filtering-exporter.ts');
  assert.match(maskSource, /return result !== undefined \? result : spanData/);
  assert.match(maskSource, /logger\.error\(\s*`mask callable failed/);
  assert.match(filterSource, /if \(filtered\.length === 0\)/);
  assert.match(filterSource, /resultCallback\(\{ code: ExportResultCode\.SUCCESS \}\)/);
  assert.equal(
    createHash('sha256').update(maskSource).digest('hex'),
    '717fcef0b44c55d26f6c61dfdbac31569c0833e870ef48a6d9ac1b76f5ffef00'
  );
  assert.equal(
    createHash('sha256').update(filterSource).digest('hex'),
    'c8b19acd3f3086634c31f0e073abe0497baf82c6cb21809bdd39a007218ec0e9'
  );

  const delegatePath = 'node_modules/@opentelemetry/otlp-exporter-base/build/src/otlp-export-delegate.js';
  const handlerPath = 'node_modules/@opentelemetry/otlp-exporter-base/build/src/logging-response-handler.js';
  const delegate = readFileSync(delegatePath, 'utf8');
  const handler = readFileSync(handlerPath, 'utf8');
  assert.equal(sha256(delegatePath), '3390c48e80645133864632c005ac20806f6ecc735fab80b181ce8a0df6983051');
  assert.equal(sha256(handlerPath), 'a08b084b841b3f3d79d515047ee2988409c07232362f89512edc3e09590b37e3');
  assert.match(delegate, /response\.status === 'success'/);
  assert.match(delegate, /responseHandler\.handleResponse/);
  assert.match(delegate, /could not deserialize response[\s\S]*response\.data/);
  assert.match(delegate, /ExportResultCode\.SUCCESS/);
  assert.match(handler, /partialSuccess/);
  assert.match(handler, /diag\.warn\('Received Partial Success response:'/);
});
