import assert from 'node:assert/strict';
import {
  acquireOperationsDiagnosticSlot,
  closeOperationsDiagnosticSessionFiles
} from '../../src/internal/private-ops-diagnostics.js';
import { writeJsonAtomically } from '../observability/neatlogs-sdk/atomic-json-publisher.js';

const cleanFlags = () => ({
  enabled: true, complete: false, missingChildCapture: false, malformed: false,
  overflow: false, writeFailed: false, abruptExit: false, cleanupIncomplete: false
});

function diagnosticState(slots = []) {
  return {
    directory: '/synthetic/ops07', slots, pendingDescriptor: null,
    cleanupUncertain: false, flags: cleanFlags()
  };
}

function diagnosticOperations(overrides = {}) {
  const calls = { open: 0, fstat: 0, close: 0, path: null, fd: null };
  const operations = {
    openSync(path, flags, mode) {
      calls.open += 1;
      calls.path = path;
      assert.equal(flags, 'wx');
      assert.equal(mode, 0o600);
      if (overrides.openSync) return overrides.openSync(path);
      calls.fd = 71;
      return calls.fd;
    },
    fstatSync(fd) {
      calls.fstat += 1;
      assert.equal(fd, calls.fd);
      if (overrides.fstatSync) return overrides.fstatSync(fd);
      return { isFile: () => true, mode: 0o100600, dev: 7, ino: 11 };
    },
    closeSync(fd) {
      calls.close += 1;
      assert.equal(fd, calls.fd);
      if (overrides.closeSync) return overrides.closeSync(fd);
      return undefined;
    }
  };
  return { calls, operations };
}

function testDiagnosticAcquisitionAndDisposal() {
  {
    const state = diagnosticState();
    const failure = new Error('synthetic fstat failure');
    const { calls, operations } = diagnosticOperations({ fstatSync: () => { throw failure; } });
    assert.equal(acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 3), null);
    assert.equal(calls.open, 1);
    assert.equal(calls.fstat, 1);
    assert.equal(calls.close, 1);
    assert.equal(state.pendingDescriptor, null);
    assert.equal(state.slots.length, 0);
    assert.equal(state.flags.writeFailed, true);
    assert.equal(state.flags.cleanupIncomplete, false);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), true);
    assert.equal(calls.close, 1);
  }

  {
    const state = diagnosticState();
    const { calls, operations } = diagnosticOperations({
      fstatSync: () => ({ isFile: () => true, mode: 0o100644, dev: 7, ino: 11 })
    });
    assert.equal(acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 4), null);
    assert.equal(calls.close, 1);
    assert.equal(state.pendingDescriptor, null);
    assert.equal(state.slots.length, 0);
    assert.equal(state.flags.writeFailed, true);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), true);
    assert.equal(calls.close, 1);
  }

  {
    const state = diagnosticState();
    const openFailure = Object.assign(new Error('synthetic exclusive-open collision'), { code: 'EEXIST' });
    const { calls, operations } = diagnosticOperations({ openSync: () => { throw openFailure; } });
    assert.equal(acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 5), null);
    assert.equal(calls.open, 1);
    assert.equal(calls.fstat, 0);
    assert.equal(calls.close, 0);
    assert.equal(state.pendingDescriptor, null);
    assert.equal(state.flags.writeFailed, true);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), true);
    assert.equal(calls.close, 0);
  }

  {
    const state = diagnosticState();
    const statFailure = new Error('synthetic metadata failure');
    const closeFailure = new Error('synthetic close failure');
    const { calls, operations } = diagnosticOperations({
      fstatSync: () => { throw statFailure; },
      closeSync: () => { throw closeFailure; }
    });
    assert.equal(acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 6), null);
    assert.equal(calls.close, 1);
    assert.equal(state.pendingDescriptor?.fd, 71);
    assert.equal(state.pendingDescriptor?.closeAttempted, true);
    assert.equal(state.pendingDescriptor?.closeFailed, true);
    assert.equal(state.cleanupUncertain, true);
    assert.equal(state.flags.writeFailed, true);
    assert.equal(state.flags.cleanupIncomplete, true);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), false);
    assert.equal(calls.close, 1, 'uncertain close is never blindly retried');
  }

  {
    const slots = [];
    Object.defineProperty(slots, 'push', { value() { throw new Error('synthetic slot registration failure'); } });
    const state = diagnosticState(slots);
    const { calls, operations } = diagnosticOperations();
    assert.equal(acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 7), null);
    assert.equal(calls.open, 1);
    assert.equal(calls.fstat, 1);
    assert.equal(calls.close, 1);
    assert.equal(state.pendingDescriptor, null);
    assert.equal(state.flags.writeFailed, true);
    assert.equal(state.flags.cleanupIncomplete, false);
  }

  {
    const state = diagnosticState();
    const { calls, operations } = diagnosticOperations();
    const slot = acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 8);
    assert.equal(slot, state.slots[0]);
    assert.equal(slot.fd, 71);
    assert.equal(slot.dev, 7);
    assert.equal(slot.ino, 11);
    assert.equal(state.pendingDescriptor, null);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), true);
    assert.equal(slot.closed, true);
    assert.equal(slot.fd, -1);
    assert.equal(calls.close, 1);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), true);
    assert.equal(calls.close, 1, 'successful disposal is idempotent');
  }

  {
    const state = diagnosticState();
    const closeFailure = new Error('synthetic disposal close failure');
    const { calls, operations } = diagnosticOperations({ closeSync: () => { throw closeFailure; } });
    const slot = acquireOperationsDiagnosticSlot(state, '/synthetic/ops07/child.jsonl', operations, () => 9);
    assert.ok(slot);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), false);
    assert.equal(state.flags.cleanupIncomplete, true);
    assert.equal(state.cleanupUncertain, true);
    assert.equal(slot.closeFailed, true);
    assert.equal(calls.close, 1);
    assert.equal(closeOperationsDiagnosticSessionFiles(state, operations.closeSync), false);
    assert.equal(calls.close, 1, 'failed disposal remains uncertain without retry');
  }
}

function fakePublisherFilesystem({ openFailure, writeFailure, closeFailure, renameFailure, removeFailure } = {}) {
  const calls = { open: 0, write: 0, close: 0, rename: 0, remove: 0 };
  let temporaryPath = null;
  let finalPath = null;
  let temporaryBytes = null;
  let finalBytes = null;
  let closed = false;
  let foreignBytes = Buffer.from('preexisting sentinel');
  let sentinelPresent = Boolean(openFailure);
  const filesystem = {
    async open(path, flags, mode) {
      calls.open += 1;
      temporaryPath = path;
      assert.equal(flags, 'wx');
      assert.equal(mode, 0o600);
      if (openFailure) throw openFailure;
      assert.equal(sentinelPresent, false, 'exclusive open did not collide with a foreign temp file');
      sentinelPresent = true;
      temporaryBytes = Buffer.alloc(0);
      return {
        async writeFile(bytes) {
          calls.write += 1;
          if (writeFailure) throw writeFailure;
          temporaryBytes = Buffer.concat([temporaryBytes, Buffer.from(bytes)]);
        },
        async close() {
          calls.close += 1;
          if (closeFailure) throw closeFailure;
          closed = true;
        }
      };
    },
    async rename(from, to) {
      calls.rename += 1;
      assert.equal(from, temporaryPath);
      assert.equal(closed, true);
      if (renameFailure) throw renameFailure;
      finalPath = to;
      finalBytes = Buffer.from(temporaryBytes);
      temporaryBytes = null;
      sentinelPresent = false;
    },
    async rm(path, options) {
      calls.remove += 1;
      assert.equal(path, temporaryPath);
      assert.deepEqual(options, { force: true });
      if (removeFailure) throw removeFailure;
      temporaryBytes = null;
      sentinelPresent = false;
    }
  };
  return {
    filesystem, calls,
    get temporaryPath() { return temporaryPath; },
    get finalPath() { return finalPath; },
    get temporaryBytes() { return temporaryBytes; },
    get finalBytes() { return finalBytes; },
    get sentinelPresent() { return sentinelPresent; },
    get foreignBytes() { return foreignBytes; }
  };
}

async function expectSameRejection(promise, primary) {
  let observed;
  try { await promise; } catch (error) { observed = error; }
  assert.equal(observed, primary, 'cleanup preserves the original failure object');
}

async function testAtomicPublisherOwnership() {
  {
    const collision = Object.assign(new Error('synthetic exclusive-open collision'), { code: 'EEXIST' });
    const fake = fakePublisherFilesystem({ openFailure: collision });
    const original = Buffer.from(fake.foreignBytes);
    await expectSameRejection(writeJsonAtomically('/synthetic/ready.json', { state: 'ready' }, {
      filesystem: fake.filesystem
    }), collision);
    assert.equal(fake.calls.open, 1);
    assert.equal(fake.calls.write, 0);
    assert.equal(fake.calls.close, 0);
    assert.equal(fake.calls.rename, 0);
    assert.equal(fake.calls.remove, 0, 'unacquired EEXIST path is never removed');
    assert.equal(fake.sentinelPresent, true);
    assert.deepEqual(fake.foreignBytes, original);
  }

  {
    const fake = fakePublisherFilesystem();
    const value = { phase: 'fake partial publication' };
    let partialVisible = false;
    await writeJsonAtomically('/synthetic/ready.json', value, {
      filesystem: fake.filesystem,
      afterPartialWrite: async ({ partialBytes, totalBytes }) => {
        partialVisible = true;
        assert.equal(fake.finalPath, null, 'the final marker stays absent during the partial write');
        assert.equal(fake.sentinelPresent, true);
        assert.equal(fake.temporaryBytes.length, partialBytes);
        assert.ok(partialBytes > 0 && partialBytes < totalBytes);
      }
    });
    assert.equal(partialVisible, true);
    assert.equal(fake.calls.write, 2);
    assert.equal(fake.calls.close, 1);
    assert.equal(fake.calls.rename, 1);
    assert.equal(fake.calls.remove, 0);
    assert.equal(fake.sentinelPresent, false);
    assert.equal(fake.finalPath, '/synthetic/ready.json');
    assert.deepEqual(JSON.parse(fake.finalBytes.toString('utf8')), value);
  }

  {
    const primary = new Error('synthetic partial callback failure');
    const fake = fakePublisherFilesystem();
    await expectSameRejection(writeJsonAtomically('/synthetic/ready.json', { state: 'partial' }, {
      filesystem: fake.filesystem,
      afterPartialWrite: async () => { throw primary; }
    }), primary);
    assert.equal(fake.calls.write, 1);
    assert.equal(fake.calls.close, 1);
    assert.equal(fake.calls.remove, 1);
    assert.equal(fake.calls.rename, 0);
    assert.equal(fake.sentinelPresent, false);
  }

  {
    const primary = new Error('synthetic write failure');
    const cleanup = new Error('synthetic remove failure');
    const fake = fakePublisherFilesystem({ writeFailure: primary, removeFailure: cleanup });
    await expectSameRejection(writeJsonAtomically('/synthetic/ready.json', { state: 'write' }, {
      filesystem: fake.filesystem
    }), primary);
    assert.equal(fake.calls.close, 1);
    assert.equal(fake.calls.remove, 1);
    assert.equal(fake.calls.rename, 0);
    assert.equal(fake.sentinelPresent, true, 'failed cleanup is not reported as successful removal');
  }

  {
    const primary = new Error('synthetic close failure');
    const fake = fakePublisherFilesystem({ closeFailure: primary });
    await expectSameRejection(writeJsonAtomically('/synthetic/ready.json', { state: 'close' }, {
      filesystem: fake.filesystem
    }), primary);
    assert.equal(fake.calls.close, 1, 'a throwing close is never retried');
    assert.equal(fake.calls.remove, 1);
    assert.equal(fake.calls.rename, 0);
    assert.equal(fake.sentinelPresent, false);
  }

  {
    const primary = new Error('synthetic rename failure');
    const fake = fakePublisherFilesystem({ renameFailure: primary });
    await expectSameRejection(writeJsonAtomically('/synthetic/ready.json', { state: 'rename' }, {
      filesystem: fake.filesystem
    }), primary);
    assert.equal(fake.calls.close, 1);
    assert.equal(fake.calls.rename, 1);
    assert.equal(fake.calls.remove, 1, 'rename failure removes only the acquired temp file');
    assert.equal(fake.sentinelPresent, false);
    assert.equal(fake.finalPath, null);
  }
}

testDiagnosticAcquisitionAndDisposal();
await testAtomicPublisherOwnership();
console.log('OPS actual ownership fake controls passed: diagnostic descriptor custody and SDK atomic publisher acquisition/error paths.');
