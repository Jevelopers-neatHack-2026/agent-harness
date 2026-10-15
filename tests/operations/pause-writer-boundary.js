import { DatabaseSync } from 'node:sqlite';

// Fixture-only: a paused runner must not retain the database's writer lock
// while the parent prepares and records a separate controlled JEV attempt.
// No retry: a failed BEGIN must refuse before sending the pause signal.
export function pauseAtWriterBoundary(databasePath, pause, confirmPaused) {
  const fence = new DatabaseSync(databasePath, { timeout: 20 });
  try {
    fence.exec('BEGIN IMMEDIATE');
    if (pause() !== true || confirmPaused() !== true) {
      throw new Error('The owned runner pause could not be confirmed.');
    }
  } finally {
    try { if (fence.isTransaction) fence.exec('ROLLBACK'); }
    finally { fence.close(); }
  }
}
