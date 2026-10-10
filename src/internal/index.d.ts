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
  | 'CHECKPOINT_CORRUPT'
  | 'TELEMETRY_INVALID_INPUT'
  | 'TELEMETRY_REDACTION_FAILED'
  | 'TELEMETRY_STORE_UNAVAILABLE'
  | 'TELEMETRY_SINK_UNAVAILABLE'
  | 'TELEMETRY_SINK_TIMEOUT'
  | 'TELEMETRY_CURSOR_GAP'
  | 'TELEMETRY_CURSOR_CONFLICT';
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

export const INTERNAL_SCHEMA_VERSION: 5;
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

export interface DurableWorkflowEvent {
  sequence: number;
  eventId: string;
  workflowId: string;
  revision: number;
  type: string;
  fromState: WorkflowState | null;
  toState: WorkflowState | null;
  fence: number;
  occurredAtMs: number;
}

export type LocalJevUsage =
  | { readonly kind: 'known'; readonly inputTokens: number; readonly outputTokens: number }
  | {
      readonly kind: 'unknown';
      readonly reason: 'not_dispatched' | 'possible_dispatch' | 'missing_or_invalid_usage' | 'recovered_after_crash';
    };

export interface LocalJevObservationInput {
  workflowId: string;
  logicalOperationRef: string;
  attemptId: string;
  transportAttemptId: string;
  sourceObservationRef: string;
  dispatchCertainty: 'not_dispatched' | 'possible' | 'response_received';
  requestedModel: 'jev-1.13.0';
  resolvedModel: string | null;
  providerRequestId: string | null;
  decisionConformance: 'valid' | 'invalid' | 'unavailable';
  usage: LocalJevUsage;
}

export interface LocalJevObservation extends LocalJevObservationInput {
  readonly schema: 'agent-harness.jev.local-usage-observation.v1';
  readonly eventId: string;
  readonly eventSequence: number;
  readonly recordedAtMs: number;
}

export interface LocalJevObservationDeliveryPage {
  readonly items: ReadonlyArray<LocalJevObservation & {
    readonly deliveryIntent: 'local_only_not_finance_acceptance';
  }>;
  readonly hasMore: boolean;
  readonly nextAfterSequence: number;
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
  recordJevDispatchMarker(input: Pick<Lease, 'workflowId' | 'ownerId' | 'fence'> & {
    expectedRevision: number;
    attemptId: string;
    transportAttemptId: string;
  }): {
    duplicate: boolean;
    transportAttemptId: string;
    eventId: string;
    eventSequence: number;
    revision: number;
  };
  getJevDispatchMarker(transportAttemptId: string): {
    transportAttemptId: string;
    attemptId: string;
    workflowId: string;
    logicalOperationRef: string;
    eventId: string;
    eventSequence: number;
    markedAtMs: number;
  } | null;
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
  recordJevProviderObservation(input: LocalJevObservationInput): {
    duplicate: boolean;
    sourceObservationRef: string;
    eventId: string;
    eventSequence: number;
  };
  getJevProviderObservation(sourceObservationRef: string): LocalJevObservation | null;
  readJevObservationDeliveryPage(input?: {
    afterSequence?: number;
    limit?: number;
  }): LocalJevObservationDeliveryPage;
  acknowledgeJevObservationDelivery(eventId: string): {
    acknowledged: boolean;
    duplicate: boolean;
  };
  getAttempt(attemptId: string): Record<string, unknown>;
  getReceipt(attemptId: string): Record<string, unknown>;
  getArtifact(workflowId: string, artifactId: string): {
    workflowId: string;
    artifactId: string;
    sha256: string;
    bytes: Uint8Array;
  };
  listEvents(input?: { workflowId?: string; afterSequence?: number }): DurableWorkflowEvent[];
  readEventPage(input?: {
    afterSequence?: number;
    limit?: number;
    workflowId?: string;
  }): {
    events: DurableWorkflowEvent[];
    hasMore: boolean;
    nextAfterSequence: number;
  };
  countEventRowsAfter(afterSequence?: number, cap?: number): { count: number; capped: boolean };
  getTelemetryCursor(consumerId?: string): number;
  advanceTelemetryCursor(input: {
    consumerId?: string;
    expectedSequence: number;
    nextSequence: number;
  }): number;
  readOutbox(input?: { afterSequence?: number; limit?: number }): Array<Record<string, unknown>>;
  acknowledgeOutbox(eventId: string): { acknowledged: boolean };
}

export interface TelemetryRecord {
  readonly schema: 'agent-harness.telemetry.v1';
  readonly sequence: number;
  readonly eventId: string;
  readonly workflowId: string;
  readonly revision: number;
  readonly eventType: string;
  readonly fromState: WorkflowState | null;
  readonly toState: WorkflowState | null;
  readonly fence: number;
  readonly occurredAtMs: number;
}

export interface TelemetryTokenizer {
  tokenize(kind: 'event' | 'workflow', value: string): string;
  destroy?(): void;
}

export interface TelemetryLimits {
  queueRecords?: number;
  queueBytes?: number;
  batchSize?: number;
  maxAttempts?: number;
  retryDelayMs?: number;
  sinkTimeoutMs?: number;
  maxBatchesPerFlush?: number;
  maxPolicyDropsPerFlush?: number;
}

export interface TelemetryDiagnostics {
  readonly state: 'idle' | 'flushing' | 'sink-pending' | 'unavailable' | 'closed';
  readonly cursor: number;
  readonly pendingRecords: number;
  readonly pendingCapped: boolean;
  readonly deliveredRecords: number;
  readonly policyDroppedRecords: number;
  readonly sinkAttempts: number;
  readonly lastErrorCode:
    | 'TELEMETRY_STORE_UNAVAILABLE'
    | 'TELEMETRY_CURSOR_GAP'
    | 'TELEMETRY_REDACTION_FAILED'
    | 'TELEMETRY_SINK_TIMEOUT'
    | 'TELEMETRY_SINK_UNAVAILABLE'
    | 'TELEMETRY_CURSOR_CONFLICT'
    | null;
}

export interface TelemetrySink {
  (
    records: readonly TelemetryRecord[],
    context: { readonly signal: AbortSignal }
  ): Promise<{ accepted: true; acceptedCount: number }> | { accepted: true; acceptedCount: number };
}

export function createHmacTokenizer(key: Uint8Array): TelemetryTokenizer;
export function projectTelemetryEvent(
  event: DurableWorkflowEvent,
  tokenize: (kind: 'event' | 'workflow', value: string) => string
): TelemetryRecord;
export type TelemetryExporterOptions = {
  store: DurableWorkflowStore;
  sink: TelemetrySink;
  limits?: TelemetryLimits;
} & (
  | { tokenizer: TelemetryTokenizer; tokenize?: never }
  | { tokenize: (kind: 'event' | 'workflow', value: string) => string; tokenizer?: never }
);

export function createTelemetryExporter(options: TelemetryExporterOptions): TelemetryExporter;
export class TelemetryExporter {
  constructor(options: TelemetryExporterOptions);
  diagnostics(): TelemetryDiagnostics;
  flush(): Promise<TelemetryDiagnostics>;
  close(): Promise<TelemetryDiagnostics>;
}
export const TELEMETRY_LIMITS: {
  readonly defaults: Readonly<Required<TelemetryLimits>>;
  readonly maximums: Readonly<Record<string, number>>;
};
