import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { createNeatlogsSdkCapture } from '../../../../src/observability/private/neatlogs-sdk-capture.js';

const recordsPath = resolve(process.argv[2]);
const artifactPath = resolve(process.argv[3]);
const networkGuard = process.argv[4] ?? 'explicit-local-exporter-no-network-path';
const lock = JSON.parse(readFileSync('package-lock.json', 'utf8'));

function packageName(path, entry, manifest) {
  if (typeof entry.name === 'string') return entry.name;
  if (typeof manifest?.name === 'string') return manifest.name;
  const tail = path.slice('node_modules/'.length).split('/node_modules/').at(-1);
  if (tail?.startsWith('@')) return tail.split('/').slice(0, 2).join('/');
  return tail;
}

function packageGraph() {
  const entries = [];
  for (const [path, entry] of Object.entries(lock.packages ?? {})) {
    if (!path.startsWith('node_modules/')) continue;
    let manifest = null;
    try {
      manifest = JSON.parse(readFileSync(join(process.cwd(), path, 'package.json'), 'utf8'));
    } catch {
      // Optional packages that do not install on a host remain recorded from the lock.
    }
    const license = entry.license ?? manifest?.license ?? null;
    const privateDevelopmentPackage = entry.dev === true && manifest?.private === true;
    if (typeof entry.version !== 'string'
        || typeof entry.integrity !== 'string'
        || (typeof license !== 'string' && !(license === null && privateDevelopmentPackage))) {
      throw new Error('The locked dependency record is incomplete.');
    }
    entries.push(Object.freeze({
      path,
      name: packageName(path, entry, manifest),
      version: entry.version,
      integrity: entry.integrity,
      license,
      licenseStatus: license === null ? 'unknown_private_development' : 'declared',
      privatePackage: manifest?.private === true,
      optional: entry.optional === true,
      developmentOnly: entry.dev === true,
      dependencies: entry.dependencies ?? Object.freeze({}),
      installed: manifest !== null
    }));
  }
  return Object.freeze(entries.sort((left, right) => left.path.localeCompare(right.path)));
}

function sha256(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

const sourcePins = Object.freeze({
  'neatlogs/package.json': {
    path: 'node_modules/neatlogs/package.json',
    sha256: '1ea4baebc7b774b4174a3bcd10296fd2a56343384f8151eb99e6c982b219109c'
  },
  'neatlogs/dist/index.mjs': {
    path: 'node_modules/neatlogs/dist/index.mjs',
    sha256: '6604c04e83b7ea56ebebe535684d685d52c4008162f1619e6ceef0cce0efea7f'
  },
  'neatlogs/dist/index.d.ts': {
    path: 'node_modules/neatlogs/dist/index.d.ts',
    sha256: 'bc49503c2ec315547d8b05172c244c77b26404a742cc6c755f037d65a977c462'
  },
  'neatlogs/dist/index.mjs.map': {
    path: 'node_modules/neatlogs/dist/index.mjs.map',
    sha256: '290e91fc6daf7155b38bc1ff9b28fe3a986e50b62d4868fd5c879b87d295cf1b'
  },
  'opentelemetry/otlp-exporter-base/logging-response-handler.js': {
    path: 'node_modules/@opentelemetry/otlp-exporter-base/build/src/logging-response-handler.js',
    sha256: 'a08b084b841b3f3d79d515047ee2988409c07232362f89512edc3e09590b37e3'
  }
});

const mappedSourcePins = Object.freeze({
  'mapped-source/src/core/client.ts': '43502db93fd931e08ceda051ad16ec0e7d2f6aa96ae7934917f1b7b6ebaccd36',
  'mapped-source/src/core/context.ts': '4ef814f3c3f837bf025f2461173496e37ad1d9a02992f4103d6404c7d7a1d596',
  'mapped-source/src/core/filtering-exporter.ts': 'c8b19acd3f3086634c31f0e073abe0497baf82c6cb21809bdd39a007218ec0e9',
  'mapped-source/src/core/byte-limited-exporter.ts': '6e0a0b02831e68b43390978fdab01d16387d1457bd3b1aa435a0faf6229be1e5',
  'mapped-source/src/core/mask.ts': '717fcef0b44c55d26f6c61dfdbac31569c0833e870ef48a6d9ac1b76f5ffef00',
  'mapped-source/src/core/logger.ts': '5fcedde6a7b09db76d097a7a2e0f333035d2b5f2369261ff78b3581ca5e8ef72',
  'mapped-source/src/core/observable-batch-processors.ts': 'a655c921305a247fef7fccafe61a0610293f09db7d5612856e437d1daca838e8',
  'mapped-source/src/core/span-processor.ts': '99b3345f0eaa6ea90cfc92055e47341bf4f701b9b660e5e891afe5d3c46fdf94',
  'mapped-source/src/core/provider.ts': '1bdb04538a6a18c13da2ef2c01bcb0ca2179a71b56540c5e00f78aabba4b71ef',
  'mapped-source/src/core/active-client.ts': '9c01298bce5e2fdd2198865908473c795219b2d5b6132adf621ca95f0e66041b',
  'mapped-source/src/core/deadline.ts': '3777487fbb5b915dc79a00e68da60d70ecccd9800ec3b9c7962c621238fe88a7',
  'mapped-source/src/core/resource.ts': '967ad526d84ac374a0620124e6b1bf03d7dadddeae69bdafbf7a9d2f34be47e3',
  'mapped-source/src/core/delivery-diagnostics.ts': '01549d62fea8b0231f0060f259ea8a6643aedcff9956b907bf2e65d73c5c13a5',
  'mapped-source/src/constants.ts': 'b271d882284800400fd7392341e8993cfd8414cb6b083d7d59c64c02eca0845e',
  'mapped-source/src/init.ts': '8c9cf998e0515f163a5bd87f1e19a31dd194e9696ba32e3f354f0d564b5cb93a'
});

const dependencySourcePins = Object.freeze({
  'dependencies/@opentelemetry__sdk-trace-base/1.30.1/package/build/src/export/BatchSpanProcessorBase.js': {
    path: 'node_modules/@opentelemetry/sdk-trace-base/build/src/export/BatchSpanProcessorBase.js',
    sha256: '1ea1a61fccaf7652c38c981965f3247b149890e147b0ec55bcff519841c59bb9'
  },
  'dependencies/@opentelemetry__otlp-exporter-base/0.57.2/package/build/src/otlp-export-delegate.js': {
    path: 'node_modules/@opentelemetry/otlp-exporter-base/build/src/otlp-export-delegate.js',
    sha256: '3390c48e80645133864632c005ac20806f6ecc735fab80b181ce8a0df6983051'
  },
  'dependencies/@opentelemetry__otlp-exporter-base/0.57.2/package/build/src/retrying-transport.js': {
    path: 'node_modules/@opentelemetry/otlp-exporter-base/build/src/retrying-transport.js',
    sha256: '16eec8d1ecd2cfe4d1b58a9405208e91849468be3c0cf77df343ab258edb5073'
  },
  'dependencies/@opentelemetry__otlp-exporter-base/0.57.2/package/build/src/transport/http-transport-utils.js': {
    path: 'node_modules/@opentelemetry/otlp-exporter-base/build/src/transport/http-transport-utils.js',
    sha256: 'd44cd62f4eb92c4a0a0e4db7db5a332b5b5b18785bd09bd34eab6bfdc111f3da'
  }
});

function verifySourcePins() {
  const actual = Object.create(null);
  for (const [name, pin] of Object.entries({ ...sourcePins, ...dependencySourcePins })) {
    const digest = sha256(pin.path);
    if (digest !== pin.sha256) throw new Error('A reviewed SDK source pin did not match.');
    actual[name] = digest;
  }
  const sourceMap = JSON.parse(readFileSync('node_modules/neatlogs/dist/index.mjs.map', 'utf8'));
  for (const [name, expected] of Object.entries(mappedSourcePins)) {
    const relativePath = name.slice('mapped-source/'.length);
    const found = sourceMap.sources
      .map((source, index) => ({ source, content: sourceMap.sourcesContent[index] }))
      .filter(item => item.source.endsWith(`/${relativePath}`));
    if (found.length !== 1 || typeof found[0].content !== 'string') {
      throw new Error('A reviewed Neatlogs source entry was not available.');
    }
    const digest = createHash('sha256').update(found[0].content).digest('hex');
    if (digest !== expected) throw new Error('A reviewed Neatlogs mapped-source pin did not match.');
    actual[name] = digest;
  }
  return Object.freeze(actual);
}

const records = JSON.parse(readFileSync(recordsPath, 'utf8'));
const capture = createNeatlogsSdkCapture();
const result = await capture.prepare(records);
function recursivelyFrozen(value, seen = new Set()) {
  if (value === null || typeof value !== 'object' || seen.has(value)) return true;
  seen.add(value);
  if (!Object.isFrozen(value)) return false;
  return Reflect.ownKeys(value).every(key => {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && Object.hasOwn(descriptor, 'value')
      ? recursivelyFrozen(descriptor.value, seen)
      : false;
  });
}

let immutableCapture = false;
if (result.status === 'captured_local') {
  const beforeMutation = JSON.stringify(result.envelope);
  const firstSpan = result.envelope.spans[0];
  for (const collection of [firstSpan.events, firstSpan.links]) {
    assert.equal(Object.isFrozen(collection), true);
    assert.throws(() => collection.push({ name: 'SYNTHETIC_POST_VALIDATION_CANARY' }));
    assert.equal(Reflect.set(collection, '0', { name: 'SYNTHETIC_INDEX_CANARY' }), false);
    assert.throws(() => collection.splice(0, 0, { name: 'SYNTHETIC_SPLICE_CANARY' }));
    assert.equal(JSON.stringify(result.envelope), beforeMutation);
  }
  assert.equal(JSON.stringify(result.envelope).includes('SYNTHETIC_'), false);
  immutableCapture = recursivelyFrozen(result);
  assert.equal(immutableCapture, true);
}
const closeResult = await capture.close();
if (result.status !== 'captured_local' || closeResult.sdkShutdownCompleted !== true) {
  process.stdout.write(`${JSON.stringify({
    status: result.status,
    reasonCode: result.reasonCode ?? closeResult.status,
    sourceRecordCount: records.length,
    counters: result.counters ?? null
  })}\n`);
  process.exitCode = 2;
} else {
  const artifact = Object.freeze({
    evidenceSchema: 'agent-harness.neatlogs.local-evidence.v1',
    testMode: 'offline-synthetic-real-sdk',
    nodeVersion: process.version,
    networkGuard,
    sdk: result.envelope.sdk,
    capturedEnvelope: result.envelope,
    counters: result.counters,
    sourceHashes: verifySourcePins(),
    lockedDependencyGraph: packageGraph(),
    claims: Object.freeze({
      actualSdkLocalMapping: true,
      remoteIngestionVerified: false,
      sponsorAcceptance: false,
      entireEnabled: false,
      telemetryCursorAdvanced: false,
      financeAcknowledgement: false,
      liveServiceAccess: false
    }),
    artifactPath: relative(process.cwd(), artifactPath)
  });
  const bytes = `${JSON.stringify(artifact, null, 2)}\n`;
  if (Buffer.byteLength(bytes, 'utf8') > 256 * 1024) {
    throw new Error('The local evidence artifact exceeds its output cap.');
  }
  mkdirSync(dirname(artifactPath), { recursive: true });
  writeFileSync(artifactPath, bytes, { mode: 0o600 });
  const artifactSha256 = createHash('sha256').update(bytes).digest('hex');
  process.stdout.write(`${JSON.stringify({
    status: result.status,
    testMode: 'offline-synthetic-real-sdk',
    nodeVersion: process.version,
    sdkVersion: result.envelope.sdk.version,
    sourceRecordCount: result.envelope.sourceRecordCount,
    rootSpanCount: result.envelope.rootSpanCount,
    completionMarkerSpanCount: result.envelope.completionMarkerSpanCount,
    recursivelyFrozen: immutableCapture,
    nestedCollectionMutationsRejected: true,
    serializedEnvelopeUnchangedAfterMutationAttempts: true,
    networkGuard,
    artifactPath: relative(process.cwd(), artifactPath),
    artifactSha256
  })}\n`);
}
