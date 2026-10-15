import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync, statSync } from 'node:fs';

const SHA_PATTERN = /^[0-9a-f]{40}$/u;
const ZERO_SHA = '0'.repeat(40);
const MAX_EVENT_BYTES = 1024 * 1024;

function fail() {
  throw new Error('Unable to establish revision-bound JEV Git provenance.');
}

function git(root, args) {
  try {
    return execFileSync('git', args, {
      cwd: root,
      encoding: 'utf8',
      env: { PATH: process.env.PATH ?? '' },
      stdio: ['ignore', 'pipe', 'ignore']
    }).trim();
  } catch {
    fail();
  }
}

function isPlainRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function safeSha(value) {
  return typeof value === 'string' && SHA_PATTERN.test(value);
}

function checkedCommit(root, value) {
  if (!safeSha(value)) fail();
  const resolved = git(root, ['rev-parse', '--verify', `${value}^{commit}`]);
  if (resolved !== value) fail();
  return value;
}

function checkedRef(root, ref) {
  if (typeof ref !== 'string' || ref.length > 256 || /[\u0000-\u0020\u007f]/u.test(ref)) fail();
  const resolved = git(root, ['rev-parse', '--verify', `${ref}^{commit}`]);
  if (!safeSha(resolved)) fail();
  return { ref, commit: resolved };
}

function checkedBranch(root, value) {
  if (typeof value !== 'string' || value.length < 1 || value.length > 240
      || value.startsWith('-')
      || /[\u0000-\u0020\u007f]/u.test(value)) fail();
  if (git(root, ['check-ref-format', '--branch', value]) !== value) fail();
  return value;
}

function assertAncestor(root, ancestor, descendant) {
  // Inputs are already verified immutable commit IDs, never revision options.
  if (git(root, ['merge-base', '--', ancestor, descendant]) !== ancestor) fail();
}

function localHeadBranch(root) {
  const result = spawnSync('git', ['symbolic-ref', '--quiet', '--short', 'HEAD'], {
    cwd: root,
    encoding: 'utf8',
    env: { PATH: process.env.PATH ?? '' },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  if (result.error || result.signal !== null) fail();
  if (result.status === 1) {
    // With HEAD already verified as a commit, symbolic-ref's quiet status 1
    // means HEAD is detached. Reject diagnostics or output as an actual error.
    if (result.stdout !== '' || result.stderr !== '') fail();
    return null;
  }
  if (result.status !== 0 || result.stderr !== '') fail();
  return checkedBranch(root, result.stdout.trim());
}

function eventPayload(env) {
  const path = env.GITHUB_EVENT_PATH;
  if (typeof path !== 'string' || path.length === 0) fail();
  try {
    const metadata = statSync(path);
    if (!metadata.isFile() || metadata.size > MAX_EVENT_BYTES) fail();
    const payload = JSON.parse(readFileSync(path, 'utf8'));
    if (!isPlainRecord(payload)) fail();
    return payload;
  } catch {
    fail();
  }
}

function resolveGithubProvenance(root, env, candidateCommit) {
  if (env.GITHUB_SHA !== candidateCommit) fail();
  const payload = eventPayload(env);

  if (env.GITHUB_EVENT_NAME === 'pull_request') {
    const pull = payload.pull_request;
    if (!isPlainRecord(pull) || !Number.isSafeInteger(pull.number) || pull.number < 1
        || !isPlainRecord(pull.base) || !isPlainRecord(pull.head)) fail();
    const baseBranch = checkedBranch(root, pull.base.ref);
    const sourceBranch = checkedBranch(root, pull.head.ref);
    const webhookBaseCommit = checkedCommit(root, pull.base.sha);
    const sourceCommit = checkedCommit(root, pull.head.sha);
    const candidateRef = `refs/pull/${pull.number}/merge`;
    if (env.GITHUB_REF !== candidateRef) fail();

    const row = git(root, ['rev-list', '--parents', '-n', '1', candidateCommit]).split(/\s+/u);
    if (row[0] !== candidateCommit || row.length !== 3
        || row[1] === row[2] || row[2] !== sourceCommit) fail();
    const baseCommit = checkedCommit(root, row[1]);
    const fetchedBase = checkedRef(root, `refs/remotes/origin/${baseBranch}`);
    // A webhook can name an earlier base than the evaluated merge candidate.
    // Accept that drift only on the same fetched base-branch history. A made-up
    // descendant of the webhook base that was never on that branch must fail.
    assertAncestor(root, webhookBaseCommit, baseCommit);
    assertAncestor(root, baseCommit, fetchedBase.commit);

    return {
      eventName: 'pull_request',
      candidateKind: 'pull_request_merge_commit',
      sourceRef: `refs/heads/${sourceBranch}`,
      sourceCommit,
      baseRef: `refs/heads/${baseBranch}`,
      baseCommit,
      webhookBaseRef: `refs/heads/${baseBranch}`,
      webhookBaseCommit,
      fetchedBaseRef: fetchedBase.ref,
      fetchedBaseCommit: fetchedBase.commit,
      candidateParentCommits: Object.freeze([baseCommit, sourceCommit]),
      baseAncestryPolicy: 'webhook_base_to_candidate_base_to_fetched_base',
      candidateRef,
      candidateCommit,
      baseSelectionPolicy: 'verified_merge_candidate_first_parent'
    };
  }

  if (env.GITHUB_EVENT_NAME === 'push') {
    const eventRef = payload.ref;
    if (typeof eventRef !== 'string' || !eventRef.startsWith('refs/heads/')) fail();
    const branch = checkedBranch(root, eventRef.slice('refs/heads/'.length));
    if (env.GITHUB_REF !== eventRef || payload.after !== candidateCommit
        || !safeSha(payload.before)) fail();

    let baseRef;
    let baseCommit;
    let baseSelectionPolicy;
    if (branch === 'main') {
      baseRef = 'push.before';
      baseCommit = checkedCommit(root, payload.before);
      const common = git(root, ['merge-base', baseCommit, candidateCommit]);
      if (common !== baseCommit) fail();
      baseSelectionPolicy = 'verified_push_before_commit';
    } else {
      const main = checkedRef(root, 'refs/remotes/origin/main');
      baseRef = main.ref;
      baseCommit = git(root, ['merge-base', candidateCommit, main.ref]);
      if (!safeSha(baseCommit)) fail();
      checkedCommit(root, baseCommit);
      baseSelectionPolicy = 'merge_base_with_fetched_origin_main';
    }

    return {
      eventName: 'push',
      candidateKind: 'push_commit',
      sourceRef: eventRef,
      sourceCommit: candidateCommit,
      baseRef,
      baseCommit,
      candidateRef: eventRef,
      candidateCommit,
      baseSelectionPolicy
    };
  }

  fail();
}

export function resolveGitEvidenceProvenance(root, env = process.env) {
  const candidateCommit = git(root, ['rev-parse', '--verify', 'HEAD^{commit}']);
  if (!safeSha(candidateCommit)) fail();
  const candidateTree = git(root, ['rev-parse', '--verify', 'HEAD^{tree}']);
  if (!safeSha(candidateTree)) fail();

  let provenance;
  if (env.GITHUB_ACTIONS === 'true') {
    provenance = resolveGithubProvenance(root, env, candidateCommit);
  } else {
    const branch = localHeadBranch(root);
    const sourceRef = branch === null ? 'refs/detached/HEAD' : `refs/heads/${branch}`;
    const main = checkedRef(root, 'refs/heads/main');
    const baseCommit = git(root, ['merge-base', candidateCommit, main.ref]);
    if (!safeSha(baseCommit)) fail();
    checkedCommit(root, baseCommit);
    provenance = {
      eventName: 'local',
      candidateKind: branch === null ? 'local_detached_commit' : 'local_commit',
      sourceRef,
      sourceCommit: candidateCommit,
      baseRef: main.ref,
      baseCommit,
      candidateRef: sourceRef,
      candidateCommit,
      baseSelectionPolicy: 'merge_base_with_local_main'
    };
  }

  return Object.freeze({ ...provenance, candidateTree });
}
