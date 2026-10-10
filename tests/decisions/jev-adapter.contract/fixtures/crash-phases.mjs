import https from 'node:https';
import { Resolver } from 'node:dns/promises';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import '../network-guard.js';
import { DurableWorkflowStore } from '@jevelopers/agent-harness-runtime';
import { sendJevOnce } from '../../../../src/adapters/jev/http-transport.js';
import { recordSyntheticMetric } from '../synthetic-metrics.js';

const [databasePath, stage] = process.argv.slice(2);
const store = DurableWorkflowStore.open({ databasePath, clock: () => 1000 });
store.createWorkflow({
  workflowId: 'crash-jev-workflow',
  checkpoint: { step: 'provider-awaiting-observation', privateMarker: 'SYNTHETIC_CRASH_SOURCE' },
  eventId: 'crash-jev-created'
});
const lease = store.acquireLease({
  workflowId: 'crash-jev-workflow', ownerId: 'crash-jev-worker', ttlMs: 10_000, eventId: 'crash-jev-lease'
});
const running = store.transitionWorkflow({
  ...lease, expectedRevision: lease.revision, toState: 'running', eventId: 'crash-jev-running'
});
const attempt = {
  workflowId: 'crash-jev-workflow',
  ownerId: 'crash-jev-worker',
  fence: lease.fence,
  expectedRevision: running.revision,
  attemptId: 'crash-jev-billable',
  logicalOperationRef: 'crash-jev-logical',
  authorityHoldRef: 'synthetic-local-bookkeeping:crash-hold',
  transportAttemptId: 'crash-jev-transport'
};
const intent = store.recordDispatchIntent({ ...attempt, eventId: 'crash-jev-intent' });

function crash() {
  process.kill(process.pid, 'SIGKILL');
  throw new Error('synthetic crash signal was not delivered');
}

if (stage === 'before-dispatch') {
  recordSyntheticMetric('crash_before_dispatch');
  crash();
}
const marker = store.recordJevDispatchMarker({
  workflowId: attempt.workflowId,
  ownerId: attempt.ownerId,
  fence: attempt.fence,
  expectedRevision: intent.revision,
  attemptId: attempt.attemptId,
  transportAttemptId: attempt.transportAttemptId
});
if (marker.duplicate) throw new Error('a new crash fixture unexpectedly found a duplicate marker');

Resolver.prototype.resolve4 = async function resolve4() {
  recordSyntheticMetric('controlled_dns_resolve4');
  return ['93.184.216.34'];
};
Resolver.prototype.resolve6 = async function resolve6() {
  recordSyntheticMetric('controlled_dns_resolve6');
  return [];
};
const responseBody = readFileSync(new URL('../../../fixtures/jev/positive-response.json', import.meta.url));
https.request = (_options, callback) => {
  recordSyntheticMetric('controlled_https_request');
  const request = new EventEmitter();
  request.destroyed = false;
  request.destroy = () => { request.destroyed = true; };
  request.end = () => {
    recordSyntheticMetric('controlled_https_write');
    if (stage === 'after-possible-write') {
      recordSyntheticMetric('crash_after_possible_write');
      crash();
    }
    const response = new EventEmitter();
    response.statusCode = 200;
    response.headers = Object.assign(Object.create(null), {
      'content-type': 'application/json',
      'content-length': String(responseBody.length)
    });
    response.rawHeaders = ['content-type', 'application/json', 'content-length', String(responseBody.length)];
    setImmediate(() => {
      callback(response);
      response.emit('data', Buffer.from(responseBody));
      response.emit('end');
    });
    return request;
  };
  const socket = new EventEmitter();
  socket.encrypted = true;
  socket.authorized = false;
  socket.secureConnecting = true;
  socket.destroyed = false;
  socket.destroy = () => { socket.destroyed = true; };
  setImmediate(() => {
    request.emit('socket', socket);
    socket.secureConnecting = false;
    socket.authorized = true;
    socket.emit('secureConnect');
  });
  return request;
};

const sent = await sendJevOnce({
  request: JSON.parse(readFileSync(new URL('../../../fixtures/jev/positive-request.json', import.meta.url), 'utf8')),
  apiKey: 'synthetic-offline-key'
});
if (stage === 'after-response-before-observation') {
  recordSyntheticMetric('crash_after_response_before_observation');
  crash();
}
if (stage === 'after-observation-commit') {
  store.recordJevProviderObservation({
    workflowId: attempt.workflowId,
    logicalOperationRef: attempt.logicalOperationRef,
    attemptId: attempt.attemptId,
    transportAttemptId: attempt.transportAttemptId,
    sourceObservationRef: 'crash-jev-observation',
    dispatchCertainty: sent.dispatchCertainty,
    requestedModel: sent.validation.requestedModel,
    resolvedModel: sent.validation.resolvedModel,
    providerRequestId: sent.providerRequestId,
    decisionConformance: sent.validation.kind,
    usage: sent.validation.usage
  });
  recordSyntheticMetric('crash_after_observation_commit');
  crash();
}
throw new Error('unknown crash fixture phase');
