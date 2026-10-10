export type DependencyMode = 'mock' | 'recorded' | 'sandbox' | 'live';
export type SafeErrorCode =
  | 'CONFIG_MISSING'
  | 'CONFIG_INVALID'
  | 'UNSUPPORTED_RUNTIME'
  | 'SQLITE_UNSUPPORTED'
  | 'LIVE_EXECUTION_UNAVAILABLE'
  | 'PORT_UNAVAILABLE'
  | 'PORT_INVALID_RESULT'
  | 'STORE_OPEN_FAILED'
  | 'STORE_WRITE_FAILED'
  | 'STORE_BUSY'
  | 'STORE_CLOSED'
  | 'DURABILITY_REQUIRES_FILE'
  | 'UNSUPPORTED_SCHEMA_VERSION'
  | 'UNVERSIONED_DATA'
  | 'INVALID_INPUT'
  | 'WORKFLOW_NOT_FOUND'
  | 'WORKFLOW_ALREADY_EXISTS'
  | 'ILLEGAL_TRANSITION'
  | 'CONCURRENT_MODIFICATION'
  | 'LEASE_HELD'
  | 'LEASE_REQUIRED'
  | 'STALE_FENCE'
  | 'LEASE_EXPIRED'
  | 'TERMINAL_STATE'
  | 'OUTCOME_UNRESOLVED'
  | 'ATTEMPT_NOT_FOUND'
  | 'ATTEMPT_ALREADY_EXISTS'
  | 'ATTEMPT_STATE_INVALID'
  | 'LOGICAL_OPERATION_COMPLETED'
  | 'OBSERVATION_IDENTITY_CONFLICT'
  | 'ARTIFACT_IMMUTABLE'
  | 'ARTIFACT_NOT_FOUND'
  | 'IDEMPOTENCY_CONFLICT'
  | 'CHECKPOINT_CORRUPT';
export type WorkflowState =
  | 'queued'
  | 'running'
  | 'awaiting_response'
  | 'reconciliation_pending'
  | 'review_required'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

export interface SafeErrorRecord {
  module: string;
  operation: string;
  code: SafeErrorCode;
  retryable: boolean;
  safe_message: string;
  diagnostic_ref: string;
}

export class RuntimeError extends Error implements SafeErrorRecord {
  constructor(input: {
    module: string;
    operation: string;
    code: SafeErrorCode;
    retryable: boolean;
    safe_message: string;
    diagnostic_ref?: string;
  });
  module: string;
  operation: string;
  code: SafeErrorCode;
  retryable: boolean;
  safe_message: string;
  diagnostic_ref: string;
  toJSON(): SafeErrorRecord;
}

export function toSafeErrorRecord(error: unknown): SafeErrorRecord;
export function invokeInjected<T>(
  operation: string,
  handler: () => T | Promise<T>,
  validateResult: (value: unknown) => boolean
): Promise<T>;

export interface RuntimeConfig {
  readonly mode: DependencyMode;
  readonly modelId: string;
  readonly deadlineMs: number;
  readonly maxAttempts: number;
  readonly maxElapsedMs: number;
  readonly maxCostMinorUnits: number;
}

export function parseRuntimeConfig(env: Record<string, string | undefined>): RuntimeConfig;
export function assertExecutionModeAvailable(config: RuntimeConfig): true;

export const INTERNAL_SCHEMA_VERSION: 3;
export interface StoreOptions {
  databasePath: string;
  clock?: () => number;
  idFactory?: () => string;
  busyTimeoutMs?: number;
}

export interface Lease {
  workflowId: string;
  ownerId: string;
  fence: number;
  expiresAtMs: number;
  revision: number;
}

export class DurableWorkflowStore {
  static open(options: StoreOptions): DurableWorkflowStore;
  close(): void;
  createWorkflow(input: {
    workflowId: string;
    checkpoint?: Record<string, unknown>;
    eventId?: string;
  }): { workflowId: string; state: WorkflowState; revision: number; checkpointSequence: number };
  getWorkflow(workflowId: string): {
    workflowId: string;
    state: WorkflowState;
    revision: number;
    fence: number;
    leaseOwner: string | null;
    leaseExpiresAtMs: number | null;
    cancellationRequested: boolean;
    checkpointSequence: number;
  };
  getCheckpoint(workflowId: string): {
    workflowId: string;
    sequence: number;
    schemaVersion: number;
    data: Record<string, unknown>;
    sha256: string;
  };
  acquireLease(input: { workflowId: string; ownerId: string; ttlMs: number; eventId?: string }): Lease;
  renewLease(input: Lease & { ttlMs: number; eventId?: string }): Lease;
  releaseLease(input: Lease & { eventId?: string }): { released: boolean; state: WorkflowState };
  transitionWorkflow(input: Lease & {
    expectedRevision: number;
    toState: WorkflowState;
    eventId?: string;
  }): { state: WorkflowState; revision: number; eventSequence: number };
  requestCancellation(input: Lease & {
    expectedRevision: number;
    eventId?: string;
  }): { accepted: boolean; state: WorkflowState; revision: number; eventSequence?: number };
  recordDispatchIntent(input: Lease & {
    expectedRevision: number;
    attemptId: string;
    logicalOperationRef: string;
    authorityHoldRef: string;
    transportAttemptId: string;
    eventId?: string;
  }): { attemptId: string; state: WorkflowState; revision: number; eventSequence: number };
  recordTransportAttempt(input: Lease & {
    expectedRevision: number;
    attemptId: string;
    transportAttemptId: string;
    eventId?: string;
  }): { transportAttemptId: string; revision: number; eventSequence: number | null; duplicate: boolean };
  recordResponseReceipt(input: Lease & {
    expectedRevision: number;
    attemptId: string;
    receiptId: string;
    outcome: 'completed' | 'no_effect';
    nextState: 'running' | 'succeeded' | 'failed';
    responseBytes: Uint8Array;
    nextCheckpoint: Record<string, unknown>;
    artifacts?: Array<{ artifactId: string; bytes: Uint8Array }>;
    eventId?: string;
  }): { receiptId: string; responseSha256: string; revision: number; eventSequence: number };
  markOutcomeUnknown(input: Lease & {
    expectedRevision: number;
    attemptId: string;
    eventId?: string;
  }): { state: WorkflowState; revision: number; authorityHoldRef: string };
  reconcilePendingReceipts(input: Lease & {
    expectedRevision: number;
    eventIdPrefix?: string;
  }): { reconciled: number; state: WorkflowState; revision: number; checkpointSequence: number };
  recordAttemptObservation(input: {
    attemptId: string;
    sourceObservationRef: string;
    kind: 'known' | 'unknown';
  }): { duplicate: boolean; observationCount: number };
  getAttempt(attemptId: string): Record<string, unknown>;
  getReceipt(attemptId: string): Record<string, unknown>;
  getArtifact(workflowId: string, artifactId: string): {
    workflowId: string;
    artifactId: string;
    sha256: string;
    bytes: Uint8Array;
  };
  listEvents(input?: { workflowId?: string; afterSequence?: number }): Array<Record<string, unknown>>;
  readOutbox(input?: { afterSequence?: number; limit?: number }): Array<Record<string, unknown>>;
  acknowledgeOutbox(eventId: string): { acknowledged: boolean };
}
