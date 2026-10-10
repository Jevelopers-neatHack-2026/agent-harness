import assert from 'node:assert/strict';
import https from 'node:https';
import { Resolver } from 'node:dns/promises';
import { EventEmitter } from 'node:events';
import { test } from 'node:test';
import './network-guard.js';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { sendJevOnce } from '../../../src/adapters/jev/http-transport.js';
import { temporaryDatabase } from '../../runtime/helpers.js';
import { recordSyntheticMetric } from './synthetic-metrics.js';

const PRIVATE_MARKER = 'SYNTHETIC_PRIVATE_SOURCE_MARKER';

function sourceSnapshot(store) {
  return {
    workflow: store.getWorkflow('jev-source-integrity'),
    checkpoint: store.getCheckpoint('jev-source-integrity'),
    attempt: store.getAttempt('jev-source-attempt'),
    events: store.listEvents({ workflowId: 'jev-source-integrity' }),
    outbox: store.readOutbox()
  };
}

test('privacy_identity: synthetic transport failure leaves existing private checkpoint, outbox, workflow, and opaque hold reference unchanged', async t => {
  const temporary = temporaryDatabase('jev-phase-a-integrity-');
  t.after(() => temporary.cleanup());
  const store = DurableWorkflowStore.open({ databasePath: temporary.databasePath, clock: () => 1000 });
  t.after(() => store.close());

  store.createWorkflow({
    workflowId: 'jev-source-integrity',
    checkpoint: { private_source: PRIVATE_MARKER },
    eventId: 'jev-source-created'
  });
  const lease = store.acquireLease({
    workflowId: 'jev-source-integrity', ownerId: 'synthetic-owner', ttlMs: 10_000, eventId: 'jev-source-lease'
  });
  const running = store.transitionWorkflow({
    ...lease, expectedRevision: lease.revision, toState: 'running', eventId: 'jev-source-running'
  });
  store.recordDispatchIntent({
    ...lease,
    expectedRevision: running.revision,
    attemptId: 'jev-source-attempt',
    logicalOperationRef: 'opaque-logical-fixture',
    authorityHoldRef: 'opaque-hold-reference-fixture',
    transportAttemptId: 'jev-source-transport',
    eventId: 'jev-source-intent'
  });
  const before = sourceSnapshot(store);

  t.mock.method(Resolver.prototype, 'resolve4', () => { recordSyntheticMetric('controlled_dns_resolve4'); return Promise.resolve(['93.184.216.34']); });
  t.mock.method(Resolver.prototype, 'resolve6', () => { recordSyntheticMetric('controlled_dns_resolve6'); return Promise.resolve([]); });
  t.mock.method(https, 'request', (_options, callback) => {
    recordSyntheticMetric('controlled_https_request');
    const request = new EventEmitter();
    request.destroyed = false;
    request.destroy = () => { request.destroyed = true; };
    request.end = () => {
      recordSyntheticMetric('controlled_https_write');
      const response = new EventEmitter();
      const body = Buffer.from(PRIVATE_MARKER, 'utf8');
      response.statusCode = 503;
      response.headers = Object.assign(Object.create(null), {
        'content-type': 'application/json',
        'content-length': String(body.length)
      });
      response.rawHeaders = ['content-type', 'application/json', 'content-length', String(body.length)];
      setImmediate(() => {
        callback(response);
        response.emit('data', body);
        response.emit('end');
      });
      return request;
    };
    const socket = new EventEmitter();
    socket.encrypted = true;
    socket.authorized = true;
    socket.secureConnecting = true;
    socket.destroyed = false;
    socket.destroy = () => { socket.destroyed = true; };
    setImmediate(() => {
      request.emit('socket', socket);
      socket.secureConnecting = false;
      socket.emit('secureConnect');
    });
    return request;
  });

  const result = await sendJevOnce({
    request: {
      model: 'jev-1.13.0',
      state: { private_source: PRIVATE_MARKER },
      questions: { classify: { type: 'noul', instructions: 'Is this a synthetic request?' } }
    },
    apiKey: 'synthetic-offline-key'
  });

  assert.equal(result.kind, 'http_error');
  assert.equal(result.status, 503);
  assert.deepEqual(sourceSnapshot(store), before);
  assert.equal(store.getAttempt('jev-source-attempt').authorityHoldRef, 'opaque-hold-reference-fixture');
  assert.equal(store.getWorkflow('jev-source-integrity').state, 'awaiting_response');
  assert.equal(store.getCheckpoint('jev-source-integrity').data.private_source, PRIVATE_MARKER);
  assert.doesNotMatch(JSON.stringify(result), /SYNTHETIC_PRIVATE_SOURCE_MARKER|synthetic-offline-key/u);
});
