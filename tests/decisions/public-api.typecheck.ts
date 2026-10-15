import { createJevDecisionPort } from '@jevelopers/agent-harness-runtime/decisions';
import { invokeAgentCreativeDevelopment } from '@jevelopers/agent-harness-runtime/development';
import { validateDecisionRequest, validateDecisionResult, decisionExecutionTargetHash } from 'creative-engine/contracts/decision-port/v1';
import { validateMeteredAttempt } from 'creative-engine/contracts/metering';
import type { CreativeRuntimePorts } from 'creative-engine/api/v1';

const port = createJevDecisionPort({ apiKey: 'typecheck-not-a-key', environment: 'test', eligibility: {
  provider: 'typesafe', model: 'jev-1.13.0', environment: 'test', accountRef: 'account', projectRef: 'project',
  evidenceRef: 'evidence', expiresAt: '2026-10-13T00:00:00Z', eligible: true,
}, contracts: { validateDecisionRequest, validateDecisionResult, validateMeteredAttempt,
  decisionExecutionTargetHash } });
// Check both methods, complete final dispatch context and canonical result.
const compatiblePort: NonNullable<CreativeRuntimePorts['decisionPort']> = port;
void compatiblePort;
void invokeAgentCreativeDevelopment({ host: { environment: 'test', source_manifest: {
  application: '1'.repeat(40), agent_harness: '2'.repeat(40), creative_engine: '3'.repeat(40),
}, async invoke(request: { environment: string }) { return request; } }, request: { environment: 'test' },
source_manifest: { application: '1'.repeat(40), agent_harness: '2'.repeat(40), creative_engine: '3'.repeat(40) } });
