export interface JevEligibility {
  provider: 'typesafe';
  model: 'jev-1.13.0';
  environment: 'development' | 'test' | 'production';
  accountRef: string;
  projectRef: string;
  evidenceRef: string;
  expiresAt: string;
  eligible: true;
}

export interface JevRoute {
  readonly retry_mapping_ref: string;
  readonly provider: Readonly<{
    name: 'typesafe'; model: 'jev-1.13.0'; endpoint: 'https://api.typesafe.ai/v1/systemone';
    sdk: Readonly<{ name: 'agent-harness-http'; version: '0.1.0' }>;
  }>;
}

export interface JevDispatchInput<Request, Attempt, Scope> {
  scope: Scope;
  actor_id: string;
  request: Request;
  attempt: Attempt;
  reservation_ref: string;
  transport_attempt_id: string;
  dispatch_intent_ref: string;
  target_binding_sha256: `sha256:${string}`;
  deadline_at: string;
  signal?: AbortSignal;
  recordTransportAttempt(transportAttemptId: string): Promise<string>;
  assertDispatchCurrent(): string;
  revalidateDispatch(binding: Readonly<{
    reservation_ref: string; billable_attempt_id: string; transport_attempt_id: string;
    dispatch_intent_ref: string; target_binding_sha256: `sha256:${string}`; signal?: AbortSignal;
  }>): Promise<string>;
}

export interface JevDecisionPort<Request, Result, Attempt, Scope> {
  resolveRoute(input: { scope: Scope; actor_id: string; decision: unknown }): JevRoute;
  execute(input: JevDispatchInput<Request, Attempt, Scope>): Promise<Result>;
}

/** Canonical validators and types remain supplied by their owner. */
export declare function createJevDecisionPort<Request, Result, Attempt, Scope>(options: {
  apiKey: string;
  environment: JevEligibility['environment'];
  eligibility: JevEligibility;
  contracts: {
    validateDecisionRequest(value: any, options: { trustedScope: Scope }): Request;
    validateDecisionResult(request: Request, value: any, options: { trustedScope: Scope }): Result;
    validateMeteredAttempt(value: unknown, trustedScope: any): Attempt;
    decisionExecutionTargetHash(value: any): string;
  };
  clock?: () => number;
}): JevDecisionPort<Request, Result, Attempt, Scope>;

export declare class JevDecisionError extends Error {
  constructor(code: string);
  readonly code: string;
}
