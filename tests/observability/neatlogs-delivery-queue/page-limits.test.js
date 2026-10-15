import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import {
  NeatlogsDeliveryQueue,
  NeatlogsDeliveryQueueError
} from '../../../src/observability/private/neatlogs-delivery-queue.js';

const ROWS = 65;
const token = value => `tok1_${value.toString(16).padStart(64, '0')}`;
const TABLES = [
  'neatlogs_delivery_queue', 'neatlogs_delivery_queue_meta',
  'neatlogs_delivery_batches', 'neatlogs_delivery_attempts'
];

function fixture({ sent = false } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'neatlogs-page-limits-'));
  const databasePath = join(directory, 'queue.sqlite');
  const clock = { now: 1_700_000_000_000, reads: 0 };
  const queue = NeatlogsDeliveryQueue.open({
    databasePath, readbackDelayMs: 0,
    clock: () => { clock.reads += 1; return clock.now; }
  });
  let lease;
  try {
    const result = queue.enqueue(Array.from({ length: ROWS }, (_, index) => ({
      eventToken: token(index + 1), workflowName: 'agent-harness.ticketstub-events',
      sessionId: token(index + 1_000), eventType: 'art_ready',
      input: JSON.stringify({ fixture: index }), occurredAtMs: 1_000 + index,
      attributes: [['agent_harness.source', 'ticketstub'], ['ticketstub.event_type', 'art_ready']]
    })));
    assert.equal(result.accepted, ROWS);
    lease = queue.acquireDrainLease('synthetic-page-limit-owner');
    assert.ok(lease);
    if (sent) {
      // Prepare valid durable readback rows without invoking any transport.
      // The acknowledgement is a synthetic local method input, not a receipt.
      for (let index = 0; index < 3; index += 1) {
        for (const batch of queue.prepareSendBatches(queue.dueForSend(), ['evidence:synthetic'], lease)) {
          queue.beginPost(batch, lease);
          queue.recordSend(batch, {
            status: 'receiver_accepted', attempts: 1, httpStatus: 200, reasonCode: null
          }, lease);
        }
      }
      assert.equal(queue.diagnostics().counts.sent, ROWS);
    }
    clock.now += 100;
    return { queue, lease, clock, databasePath, close() {
      try { queue.releaseDrainLease(lease); } finally {
        try { queue.close(); } finally { rmSync(directory, { recursive: true, force: true }); }
      }
    } };
  } catch (error) {
    try { queue.close(); } finally { rmSync(directory, { recursive: true, force: true }); }
    throw error;
  }
}

function snapshot(databasePath) {
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try {
    return Object.fromEntries(TABLES.map(table => [table,
      db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all().map(row => ({ ...row }))
    ]));
  } finally {
    db.close();
  }
}

function leaseRow(value) {
  return JSON.parse(value.neatlogs_delivery_queue_meta.find(row => row.key === 'drain_lease').value);
}

function invalidLimits(onCoercion) {
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  return [
    ['negative', -1], ['negative safe integer', -Number.MAX_SAFE_INTEGER],
    ['fractional', 0.5], ['negative fractional', -0.5],
    ['NaN', NaN], ['positive infinity', Infinity], ['negative infinity', -Infinity],
    ['unsafe integer', Number.MAX_SAFE_INTEGER + 1],
    ['numeric string', '1'], ['negative string', '-1'], ['empty string', ''],
    ['null', null], ['boolean', true], ['bigint', 1n], ['symbol', Symbol('invalid-limit')],
    ['array', []], ['boxed number', new Number(1)],
    ['coercible object', { [Symbol.toPrimitive]() { onCoercion(); return 1; } }],
    ['proxy', new Proxy({}, { get() { onCoercion(); throw new Error('coercion must not run'); } })],
    ['revoked proxy', revoked.proxy]
  ];
}

function assertInvalidLimits(f, invoke) {
  const before = snapshot(f.databasePath);
  let coercions = 0;
  f.clock.reads = 0;
  for (const [label, value] of invalidLimits(() => { coercions += 1; })) {
    assert.throws(() => invoke(value), error => error instanceof NeatlogsDeliveryQueueError
      && error.code === 'INVALID_OPTION', label);
    assert.equal(coercions, 0, `${label}: no numeric coercion`);
    assert.equal(f.clock.reads, 0, `${label}: refuse before clock or lease work`);
    assert.deepEqual(snapshot(f.databasePath), before, `${label}: no durable queue or lease change`);
  }
}

test('diagnostic pages preserve default, zero and positive clamping', () => {
  const f = fixture();
  try {
    const before = snapshot(f.databasePath);
    assert.equal(f.queue.diagnostics().latest.length, 5);
    for (const [recent, expected] of [[undefined, 5], [0, 0], [-0, 0], [1, 1], [50, 50], [51, 50], [Number.MAX_SAFE_INTEGER, 50]]) {
      assert.equal(f.queue.diagnostics({ recent }).latest.length, expected);
    }
    assert.deepEqual(snapshot(f.databasePath), before);
  } finally { f.close(); }
});

test('invalid diagnostic limits cannot request an unbounded SQLite page or coerce objects', () => {
  const f = fixture();
  try { assertInvalidLimits(f, recent => f.queue.diagnostics({ recent })); } finally { f.close(); }
});

test('send pages preserve default, zero, ascending order and positive clamping', () => {
  const f = fixture();
  try {
    const before = snapshot(f.databasePath);
    assert.equal(f.queue.dueForSend().length, 32);
    for (const [limit, expected] of [[undefined, 32], [0, 0], [-0, 0], [1, 1], [32, 32], [33, 32], [Number.MAX_SAFE_INTEGER, 32]]) {
      const rows = f.queue.dueForSend(limit);
      assert.equal(rows.length, expected);
      assert.deepEqual(rows.map(row => row.event_token), Array.from({ length: expected }, (_, i) => token(i + 1)));
    }
    assert.deepEqual(snapshot(f.databasePath), before);
  } finally { f.close(); }
});

test('invalid send limits fail before clock access and preserve every queue table', () => {
  const f = fixture();
  try { assertInvalidLimits(f, limit => f.queue.dueForSend(limit)); } finally { f.close(); }
});

test('readback pages preserve default, zero and positive caps under a valid lease', () => {
  const f = fixture({ sent: true });
  try {
    const before = snapshot(f.databasePath);
    const originalLease = leaseRow(before);
    for (const [limit, expected] of [[undefined, 32], [0, 0], [-0, 0], [1, 1], [32, 32], [33, 32], [Number.MAX_SAFE_INTEGER, 32]]) {
      const rows = f.queue.dueForReadback(limit, f.lease);
      assert.equal(rows.length, expected);
      assert.deepEqual(rows.map(row => row.event_token), Array.from({ length: expected }, (_, i) => token(i + 1)));
    }
    const after = snapshot(f.databasePath);
    const renewedLease = leaseRow(after);
    assert.equal(renewedLease.owner, originalLease.owner);
    assert.equal(renewedLease.fence, originalLease.fence);
    assert.ok(renewedLease.expiresAtMs > originalLease.expiresAtMs);
    for (const table of TABLES.filter(name => name !== 'neatlogs_delivery_queue_meta')) {
      assert.deepEqual(after[table], before[table]);
    }
  } finally { f.close(); }
});

test('invalid readback limits fail before lease renewal and preserve every queue table', () => {
  const f = fixture({ sent: true });
  try { assertInvalidLimits(f, limit => f.queue.dueForReadback(limit, f.lease)); } finally { f.close(); }
});

test('invalid readback limits are refused before the missing-lease path', () => {
  const f = fixture({ sent: true });
  try { assertInvalidLimits(f, limit => f.queue.dueForReadback(limit)); } finally { f.close(); }
});

test('valid readback page limits still require a current lease', () => {
  const f = fixture({ sent: true });
  try {
    const before = snapshot(f.databasePath);
    assert.deepEqual(f.queue.dueForReadback(32), []);
    assert.deepEqual(f.queue.dueForReadback(32, { ...f.lease, fence: f.lease.fence + 1 }), []);
    assert.deepEqual(snapshot(f.databasePath), before);
  } finally { f.close(); }
});

test('a stale readback owner cannot renew or clear a newer lease', () => {
  const f = fixture({ sent: true });
  let newer;
  try {
    f.clock.now += 120_000;
    newer = f.queue.acquireDrainLease('newer-synthetic-page-limit-owner');
    assert.ok(newer);
    const before = snapshot(f.databasePath);
    assert.deepEqual(f.queue.dueForReadback(32, f.lease), []);
    f.queue.releaseDrainLease(f.lease);
    assert.deepEqual(snapshot(f.databasePath), before);
    assert.equal(f.queue.dueForReadback(32, newer).length, 32);
  } finally {
    if (newer) f.queue.releaseDrainLease(newer);
    f.close();
  }
});
