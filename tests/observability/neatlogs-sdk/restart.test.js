import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { sourceRecordFromNeatlogsAttributes } from '../../../src/observability/private/neatlogs-sdk-mapper.js';
import {
  createTemporaryDirectory,
  projectStoreEvents,
  readJson,
  spawnNodeFixture,
  SYNTHETIC_CANARIES,
  writeJson
} from './helpers.js';

function snapshotAllTables(databasePath) {
  const database = new DatabaseSync(databasePath, { readOnly: true });
  try {
    const tableNames = database.prepare(
      "select name from sqlite_schema where type='table' order by name"
    ).all().map(row => row.name);
    const rows = Object.create(null);
    for (const name of tableNames) {
      const quotedName = `"${name.replaceAll('"', '""')}"`;
      rows[name] = database.prepare(`select * from ${quotedName}`).all();
    }
    return JSON.stringify({ tableNames, rows });
  } finally {
    database.close();
  }
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

async function waitForPath(path, child, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (!existsSync(path)) {
    if (child?.exitCode !== null && child?.exitCode !== undefined
        || child?.signalCode !== null && child?.signalCode !== undefined) {
      throw new Error('The isolated child exited before publishing its readiness marker.');
    }
    if (Date.now() >= deadline) throw new Error('The isolated child did not reach its SQLite kill barrier.');
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

async function waitForJson(path, child, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error('The isolated child exited before publishing a complete readiness marker.');
    }
    if (existsSync(path)) {
      try { return readJson(path); } catch (error) {
        if (!(error instanceof SyntaxError)) throw error;
      }
    }
    await new Promise(resolve => setTimeout(resolve, 5));
  }
  throw new Error('The isolated child did not publish a complete JSON readiness marker within its bound.');
}

async function killAndReapFixture(fixture, timeoutMs = 2_000) {
  const child = fixture.child;
  if (child.exitCode === null && child.signalCode === null) {
    try { child.kill('SIGKILL'); } catch {}
  }
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('The owned SDK fixture did not close within its cleanup bound.')), timeoutMs);
  });
  try {
    return await Promise.race([fixture.done, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

function assertNoCanaries(value) {
  const serialized = typeof value === 'string' ? value : JSON.stringify(value);
  for (const canary of SYNTHETIC_CANARIES) assert.equal(serialized.includes(canary), false);
}

test('SQLite close, process-kill boundaries, reopen, and real SDK reproject preserve durable source', async t => {
  const temporary = createTemporaryDirectory();
  t.after(() => temporary.cleanup());
  const databasePath = join(temporary.directory, 'workflow.sqlite');
  const store = DurableWorkflowStore.open({ databasePath, clock: () => 2_000 });
  const workflowId = `workflow:${SYNTHETIC_CANARIES[0]}`;
  store.createWorkflow({
    workflowId,
    eventId: 'synthetic-restart-create',
    checkpoint: { secret: SYNTHETIC_CANARIES[2], customer: SYNTHETIC_CANARIES[3] }
  });
  const lease = store.acquireLease({
    workflowId,
    ownerId: 'synthetic-restart-worker',
    ttlMs: 10_000,
    eventId: 'synthetic-restart-lease'
  });
  store.transitionWorkflow({
    ...lease,
    expectedRevision: 0,
    toState: 'running',
    eventId: 'synthetic-restart-running'
  });
  store.recordDispatchIntent({
    ...lease,
    expectedRevision: 1,
    attemptId: 'synthetic-restart-attempt',
    logicalOperationRef: 'synthetic-restart-operation',
    authorityHoldRef: 'synthetic-unresolved-hold',
    transportAttemptId: 'synthetic-restart-transport',
    eventId: 'synthetic-restart-intent'
  });
  const baselineProjection = projectStoreEvents(store);
  const baselineTokens = baselineProjection.records.map(record => record.eventId);
  const outboxBefore = store.readOutbox();
  assert.equal(store.getTelemetryCursor(), 0);
  assert.equal(store.getAttempt('synthetic-restart-attempt').state, 'intent');
  assert.equal(baselineProjection.records.length, 4);
  assert.equal(outboxBefore.length, 4);
  store.close();

  const originalSnapshot = snapshotAllTables(databasePath);
  const originalSnapshotSha256 = sha256(originalSnapshot);
  const phaseResults = [];

  const partialReadyPath = join(temporary.directory, 'partial.ready.json');
  const partialCapturePath = join(temporary.directory, 'partial.capture.json');
  const partialStagePath = join(temporary.directory, 'partial.stage.json');
  const partialGatePath = join(temporary.directory, 'partial.gate');
  const partialFixture = spawnNodeFixture('sqlite-restart-child.mjs', [
    databasePath, 'partial-publication-control', partialReadyPath, partialCapturePath,
    partialStagePath, partialGatePath
  ], { timeoutMs: 10_000 });
  try {
    await waitForPath(partialStagePath, partialFixture.child);
    const stage = readJson(partialStagePath);
    assert.equal(stage.finalReadyPathVisible, false);
    assert.ok(stage.partialBytes > 0 && stage.partialBytes < stage.totalBytes);
    const stagingBytes = readFileSync(stage.temporaryPath);
    assert.equal(stagingBytes.byteLength, stage.partialBytes);
    assert.throws(() => JSON.parse(stagingBytes.toString('utf8')), SyntaxError);
    assert.equal(existsSync(partialReadyPath), false);

    writeFileSync(partialGatePath, 'complete', { mode: 0o600 });
    const published = await waitForJson(partialReadyPath, partialFixture.child);
    assert.deepEqual(published, {
      phase: 'partial-publication-control', rawEventCount: 4, projectedRecordCount: 4
    });
      assert.equal(existsSync(stage.temporaryPath), false, 'the atomic rename removes the partial staging file');
    assert.equal(snapshotAllTables(databasePath), originalSnapshot, 'partial marker publication does not mutate SQLite');
  } finally {
    await killAndReapFixture(partialFixture);
  }
  assert.equal(snapshotAllTables(databasePath), originalSnapshot, 'partial marker cleanup preserves every SQLite table');

  for (const phase of ['before', 'during', 'after']) {
    const readyPath = join(temporary.directory, `${phase}.ready.json`);
    const capturePath = join(temporary.directory, `${phase}.capture.json`);
    const child = spawnNodeFixture('sqlite-restart-child.mjs', [
      databasePath, phase, readyPath, capturePath
    ], { timeoutMs: 20_000 });
    try {
      const ready = await waitForJson(readyPath, child.child);
      assert.equal(ready.phase, phase);
      assert.equal(ready.sourceRecordCount ?? ready.projectedRecordCount, 4);
      if (phase === 'during') assert.equal(ready.callbackPending, 1);
      if (phase === 'after') {
        assert.equal(existsSync(capturePath), true);
        const capturedBytes = readFileSync(capturePath);
        const captured = JSON.parse(capturedBytes.toString('utf8'));
        assert.equal(sha256(capturedBytes), ready.artifactSha256);
        assert.equal(captured.testMode, 'offline-synthetic-real-sdk');
        assert.equal(captured.capturedEnvelope.rootSpanCount, 4);
        assert.equal(captured.capturedEnvelope.completionMarkerSpanCount, 4);
        assert.deepEqual(captured.claims, {
          actualSdkLocalMapping: true,
          remoteIngestionVerified: false,
          sponsorAcceptance: false,
          entireEnabled: false,
          telemetryCursorAdvanced: false,
          financeAcknowledgement: false,
          liveServiceAccess: false
        });
        assertNoCanaries(captured);
      } else {
        assert.equal(existsSync(capturePath), false);
      }

      child.child.kill('SIGKILL');
      const execution = await child.done;
      assert.equal(execution.signal, 'SIGKILL');
      assertNoCanaries(execution.stdout);
      assertNoCanaries(execution.stderr);
      assert.equal(snapshotAllTables(databasePath), originalSnapshot, `${phase}: all SQLite tables changed`);

      const reopened = DurableWorkflowStore.open({ databasePath });
      assert.equal(reopened.getTelemetryCursor(), 0);
      assert.deepEqual(reopened.readOutbox(), outboxBefore);
      assert.equal(reopened.getAttempt('synthetic-restart-attempt').state, 'intent');
      assert.equal(reopened.getAttempt('synthetic-restart-attempt').authorityHoldRef,
        'synthetic-unresolved-hold');
      const reprojected = projectStoreEvents(reopened);
      assert.equal(reprojected.rawEventCount, baselineProjection.rawEventCount);
      assert.deepEqual(reprojected.records, baselineProjection.records);
      assert.deepEqual(reprojected.records.map(record => record.eventId), baselineTokens);
      reopened.close();

      const replayPath = join(temporary.directory, `${phase}.replay.json`);
      const replay = spawnNodeFixture('sqlite-restart-child.mjs', [
        databasePath, 'replay', join(temporary.directory, `${phase}.replay.ready.json`), replayPath
      ], { timeoutMs: 20_000 });
      let replayExecution;
      try {
        replayExecution = await replay.done;
        assert.equal(replayExecution.code, 0, replayExecution.stderr);
        assertNoCanaries(replayExecution.stdout);
        assertNoCanaries(replayExecution.stderr);
        const replaySummary = JSON.parse(replayExecution.stdout.trim());
        assert.equal(replaySummary.phase, 'replay');
        assert.equal(replaySummary.sourceRecordCount, 4);
        assert.equal(replaySummary.rootSpanCount, 4);
        assert.equal(replaySummary.completionMarkerSpanCount, 4);
        const replayBytes = readFileSync(replayPath);
        const replayArtifact = JSON.parse(replayBytes.toString('utf8'));
        assert.equal(sha256(replayBytes), replaySummary.artifactSha256);
        assert.deepEqual(replayArtifact.claims, {
          actualSdkLocalMapping: true,
          remoteIngestionVerified: false,
          sponsorAcceptance: false,
          entireEnabled: false,
          telemetryCursorAdvanced: false,
          financeAcknowledgement: false,
          liveServiceAccess: false
        });
        const roots = replayArtifact.capturedEnvelope.spans
          .filter(span => span.name === 'agent_harness.workflow_event');
        const reprojectedFromCapture = roots
          .map(span => sourceRecordFromNeatlogsAttributes(span.attributes))
          .sort((left, right) => left.sequence - right.sequence);
        assert.deepEqual(reprojectedFromCapture, baselineProjection.records);
        assertNoCanaries(replayArtifact);
        const afterReplay = snapshotAllTables(databasePath);
        assert.equal(afterReplay, originalSnapshot, `${phase}: replay changed SQLite tables`);
        phaseResults.push({
          phase,
          killedWhile: phase,
          replayedRecords: reprojectedFromCapture.length,
          roots: replaySummary.rootSpanCount,
          markers: replaySummary.completionMarkerSpanCount,
          sourceTokensStable: true,
          allTablesUnchanged: true,
          cursor: 0,
          unresolvedHoldRetained: true,
          capturedArtifactSha256: phase === 'after' ? ready.artifactSha256 : null,
          replayArtifactSha256: replaySummary.artifactSha256
        });
      } finally {
        await killAndReapFixture(replay);
      }
    } finally {
      await killAndReapFixture(child);
    }
  }

  const artifact = Object.freeze({
    evidenceSchema: 'agent-harness.neatlogs.sqlite-restart-test.v1',
    testMode: 'offline-synthetic-real-sdk',
    nodeVersion: process.version,
    originalSnapshotSha256,
    sourceRecordCount: 4,
    sourceTokensStable: true,
    phases: phaseResults,
    claims: Object.freeze({
      actualSdkLocalMapping: true,
      remoteIngestionVerified: false,
      sponsorAcceptance: false,
      entireEnabled: false,
      telemetryCursorAdvanced: false,
      financeAcknowledgement: false,
      liveServiceAccess: false
    })
  });
  assertNoCanaries(artifact);
  const artifactPath = join(process.cwd(), '.runtime-artifacts', 'neatlogs-sdk-sqlite-restart.json');
  mkdirSync(join(process.cwd(), '.runtime-artifacts'), { recursive: true });
  writeJson(artifactPath, artifact);
  const artifactBytes = readFileSync(artifactPath);
  assert.equal(JSON.parse(artifactBytes.toString('utf8')).phases.length, 3);
  assert.equal(typeof sha256(artifactBytes), 'string');
});
