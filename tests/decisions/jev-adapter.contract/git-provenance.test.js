import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { resolveGitEvidenceProvenance } from './git-provenance.js';

const ZERO_SHA = '0'.repeat(40);

function git(root, args) {
  return execFileSync('git', args, {
    cwd: root,
    encoding: 'utf8',
    env: {
      PATH: process.env.PATH ?? '',
      GIT_CONFIG_GLOBAL: '/dev/null',
      GIT_CONFIG_NOSYSTEM: '1'
    },
    stdio: ['ignore', 'pipe', 'ignore']
  }).trim();
}

function commit(root, message) {
  git(root, [
    '-c', 'user.name=JEV Synthetic Fixture',
    '-c', 'user.email=jev-fixture@example.invalid',
    '-c', 'commit.gpgsign=false',
    'commit', '--quiet', '--no-gpg-sign', '-m', message
  ]);
  return git(root, ['rev-parse', 'HEAD']);
}

function makeRepository() {
  const root = mkdtempSync(join(tmpdir(), 'jev-git-provenance-'));
  git(root, ['init', '--quiet', '--initial-branch=main']);
  writeFileSync(join(root, 'evidence.txt'), 'base\n');
  git(root, ['add', 'evidence.txt']);
  const baseCommit = commit(root, 'synthetic base');
  return { root, baseCommit };
}

function githubEnvironment(root, event, { eventName, ref, sha }) {
  const eventPath = join(root, 'github-event.json');
  writeFileSync(eventPath, JSON.stringify(event));
  return {
    GITHUB_ACTIONS: 'true',
    GITHUB_EVENT_NAME: eventName,
    GITHUB_EVENT_PATH: eventPath,
    GITHUB_REF: ref,
    GITHUB_SHA: sha
  };
}

function featurePushFixture() {
  const repo = makeRepository();
  const { root, baseCommit } = repo;
  git(root, ['checkout', '--quiet', '-b', 'launch/jev-ci']);
  writeFileSync(join(root, 'evidence.txt'), 'feature\n');
  git(root, ['add', 'evidence.txt']);
  const candidateCommit = commit(root, 'synthetic feature push');
  git(root, ['update-ref', 'refs/remotes/origin/main', baseCommit]);
  const ref = 'refs/heads/launch/jev-ci';
  const env = githubEnvironment(root, {
    ref,
    before: ZERO_SHA,
    after: candidateCommit
  }, { eventName: 'push', ref, sha: candidateCommit });
  return { ...repo, candidateCommit, env };
}

function mergeObject(root, tree, firstParent, secondParent, message) {
  return git(root, ['-c', 'user.name=JEV Synthetic Fixture', '-c', 'user.email=jev-fixture@example.invalid',
    '-c', 'commit.gpgsign=false', 'commit-tree', tree, '-p', firstParent, '-p', secondParent, '-m', message]);
}

function pullRequestFixture({ advancedBase = false } = {}) {
  const repo = makeRepository();
  const { root, baseCommit: webhookBaseCommit } = repo;
  git(root, ['checkout', '--quiet', '-b', 'launch/jev-ci']);
  writeFileSync(join(root, 'source.txt'), 'pull request source\n');
  git(root, ['add', 'source.txt']);
  const sourceCommit = commit(root, 'synthetic pull request source');
  git(root, ['checkout', '--quiet', 'main']);
  let evaluatedBaseCommit = webhookBaseCommit;
  if (advancedBase) {
    writeFileSync(join(root, 'advanced-base.txt'), 'independent merged base change\n');
    git(root, ['add', 'advanced-base.txt']);
    evaluatedBaseCommit = commit(root, 'synthetic base advanced after webhook');
  }
  git(root, ['update-ref', 'refs/remotes/origin/main', evaluatedBaseCommit]);
  git(root, ['-c', 'user.name=JEV Synthetic Fixture', '-c', 'user.email=jev-fixture@example.invalid',
    '-c', 'commit.gpgsign=false', 'merge', '--quiet', '--no-ff', '--no-gpg-sign', '--no-edit', 'launch/jev-ci']);
  const candidateCommit = git(root, ['rev-parse', 'HEAD']);
  const candidateTree = git(root, ['rev-parse', 'HEAD^{tree}']);
  const ref = 'refs/pull/53/merge';
  git(root, ['update-ref', ref, candidateCommit]);
  git(root, ['checkout', '--quiet', '--detach', candidateCommit]);
  const payload = { pull_request: { number: 53, base: { ref: 'main', sha: webhookBaseCommit },
    head: { ref: 'launch/jev-ci', sha: sourceCommit } } };
  const env = githubEnvironment(root, payload, { eventName: 'pull_request', ref, sha: candidateCommit });
  return { ...repo, webhookBaseCommit, evaluatedBaseCommit, sourceCommit, candidateCommit, candidateTree, ref, payload, env };
}

test('evidence_provenance: local fixture binds source, base, and candidate to local Git history', () => {
  const repo = makeRepository();
  try {
    const { root, baseCommit } = repo;
    git(root, ['checkout', '--quiet', '-b', 'launch/jev-ci']);
    writeFileSync(join(root, 'evidence.txt'), 'local candidate\n');
    git(root, ['add', 'evidence.txt']);
    const candidateCommit = commit(root, 'synthetic local candidate');
    const provenance = resolveGitEvidenceProvenance(root, {});

    assert.equal(provenance.eventName, 'local');
    assert.equal(provenance.sourceRef, 'refs/heads/launch/jev-ci');
    assert.equal(provenance.sourceCommit, candidateCommit);
    assert.equal(provenance.baseRef, 'refs/heads/main');
    assert.equal(provenance.baseCommit, baseCommit);
    assert.equal(provenance.candidateCommit, candidateCommit);
    assert.equal(provenance.baseSelectionPolicy, 'merge_base_with_local_main');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test('evidence_provenance: detached local fixture binds its explicit ref, HEAD, tree, and base', () => {
  const repo = makeRepository();
  try {
    const { root, baseCommit } = repo;
    git(root, ['checkout', '--quiet', '-b', 'launch/jev-ci']);
    writeFileSync(join(root, 'evidence.txt'), 'detached local candidate\n');
    git(root, ['add', 'evidence.txt']);
    const candidateCommit = commit(root, 'synthetic detached local candidate');
    git(root, ['checkout', '--quiet', '--detach', candidateCommit]);

    const provenance = resolveGitEvidenceProvenance(root, {});
    assert.equal(provenance.eventName, 'local');
    assert.equal(provenance.candidateKind, 'local_detached_commit');
    assert.equal(provenance.sourceRef, 'refs/detached/HEAD');
    assert.equal(provenance.sourceCommit, candidateCommit);
    assert.equal(provenance.baseRef, 'refs/heads/main');
    assert.equal(provenance.baseCommit, baseCommit);
    assert.equal(provenance.candidateRef, 'refs/detached/HEAD');
    assert.equal(provenance.candidateCommit, candidateCommit);
    assert.equal(provenance.candidateTree, git(root, ['rev-parse', 'HEAD^{tree}']));
    assert.equal(provenance.baseSelectionPolicy, 'merge_base_with_local_main');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test('evidence_provenance: first feature push uses fetched main ancestry when push.before is zero', () => {
  const repo = featurePushFixture();
  try {
    const provenance = resolveGitEvidenceProvenance(repo.root, repo.env);
    assert.equal(provenance.eventName, 'push');
    assert.equal(provenance.sourceRef, 'refs/heads/launch/jev-ci');
    assert.equal(provenance.sourceCommit, repo.candidateCommit);
    assert.equal(provenance.baseRef, 'refs/remotes/origin/main');
    assert.equal(provenance.baseCommit, repo.baseCommit);
    assert.equal(provenance.candidateRef, 'refs/heads/launch/jev-ci');
    assert.equal(provenance.candidateCommit, repo.candidateCommit);
    assert.equal(provenance.baseSelectionPolicy, 'merge_base_with_fetched_origin_main');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test('evidence_provenance: main push binds its explicit before commit as the base', () => {
  const repo = makeRepository();
  try {
    const { root, baseCommit } = repo;
    writeFileSync(join(root, 'evidence.txt'), 'main candidate\n');
    git(root, ['add', 'evidence.txt']);
    const candidateCommit = commit(root, 'synthetic main push');
    const ref = 'refs/heads/main';
    const env = githubEnvironment(root, {
      ref,
      before: baseCommit,
      after: candidateCommit
    }, { eventName: 'push', ref, sha: candidateCommit });

    const provenance = resolveGitEvidenceProvenance(root, env);
    assert.equal(provenance.baseRef, 'push.before');
    assert.equal(provenance.baseCommit, baseCommit);
    assert.equal(provenance.candidateCommit, candidateCommit);
    assert.equal(provenance.baseSelectionPolicy, 'verified_push_before_commit');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test('evidence_provenance: pull request manifest binds the actual two-parent merge candidate', () => {
  const repo = makeRepository();
  try {
    const { root, baseCommit } = repo;
    git(root, ['checkout', '--quiet', '-b', 'launch/jev-ci']);
    writeFileSync(join(root, 'evidence.txt'), 'pull request source\n');
    git(root, ['add', 'evidence.txt']);
    const sourceCommit = commit(root, 'synthetic pull request source');
    const sourceTree = git(root, ['rev-parse', `${sourceCommit}^{tree}`]);
    const candidateCommit = git(root, [
      '-c', 'user.name=JEV Synthetic Fixture',
      '-c', 'user.email=jev-fixture@example.invalid',
      '-c', 'commit.gpgsign=false',
      'commit-tree', sourceTree,
      '-p', baseCommit,
      '-p', sourceCommit,
      '-m', 'synthetic pull request merge candidate'
    ]);
    git(root, ['update-ref', 'refs/pull/29/merge', candidateCommit]);
    git(root, ['update-ref', 'refs/remotes/origin/main', baseCommit]);
    git(root, ['checkout', '--quiet', '--detach', candidateCommit]);
    const ref = 'refs/pull/29/merge';
    const env = githubEnvironment(root, {
      pull_request: {
        number: 29,
        base: { ref: 'main', sha: baseCommit },
        head: { ref: 'launch/jev-ci', sha: sourceCommit }
      }
    }, { eventName: 'pull_request', ref, sha: candidateCommit });

    const provenance = resolveGitEvidenceProvenance(root, env);
    assert.equal(provenance.eventName, 'pull_request');
    assert.equal(provenance.sourceRef, 'refs/heads/launch/jev-ci');
    assert.equal(provenance.sourceCommit, sourceCommit);
    assert.equal(provenance.baseRef, 'refs/heads/main');
    assert.equal(provenance.baseCommit, baseCommit);
    assert.equal(provenance.webhookBaseCommit, baseCommit);
    assert.equal(provenance.fetchedBaseCommit, baseCommit);
    assert.equal(provenance.candidateRef, ref);
    assert.equal(provenance.candidateCommit, candidateCommit);
    assert.equal(provenance.baseSelectionPolicy, 'verified_merge_candidate_first_parent');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});

test('evidence_provenance: an advanced evaluated base is recorded separately from the webhook base', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    const provenance = resolveGitEvidenceProvenance(repo.root, repo.env);
    assert.equal(provenance.baseCommit, repo.evaluatedBaseCommit);
    assert.equal(provenance.webhookBaseCommit, repo.webhookBaseCommit);
    assert.notEqual(provenance.baseCommit, provenance.webhookBaseCommit);
    assert.equal(provenance.fetchedBaseRef, 'refs/remotes/origin/main');
    assert.equal(provenance.fetchedBaseCommit, repo.evaluatedBaseCommit);
    assert.equal(provenance.sourceCommit, repo.sourceCommit);
    assert.equal(provenance.candidateCommit, repo.candidateCommit);
    assert.equal(provenance.candidateTree, repo.candidateTree);
    assert.deepEqual(provenance.candidateParentCommits, [repo.evaluatedBaseCommit, repo.sourceCommit]);
    assert.equal(provenance.baseAncestryPolicy, 'webhook_base_to_candidate_base_to_fetched_base');
    assert.notEqual(repo.candidateTree, git(repo.root, ['rev-parse', `${repo.sourceCommit}^{tree}`]));
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: a moving PR merge ref cannot substitute for the immutable event candidate', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    git(repo.root, ['update-ref', repo.ref, repo.sourceCommit]);
    const provenance = resolveGitEvidenceProvenance(repo.root, repo.env);
    assert.equal(provenance.candidateCommit, repo.candidateCommit);
    assert.equal(provenance.candidateTree, repo.candidateTree);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: the second parent must remain the exact event PR head', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    const wrong = mergeObject(repo.root, repo.candidateTree, repo.evaluatedBaseCommit,
      repo.webhookBaseCommit, 'synthetic candidate with wrong source parent');
    git(repo.root, ['checkout', '--quiet', '--detach', wrong]);
    assert.throws(() => resolveGitEvidenceProvenance(repo.root, { ...repo.env, GITHUB_SHA: wrong }),
      /Unable to establish revision-bound JEV Git provenance/u);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: an invented descendant not on the fetched base branch is rejected', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    const invented = git(repo.root, ['-c', 'user.name=JEV Synthetic Fixture', '-c', 'user.email=jev-fixture@example.invalid',
      'commit-tree', repo.candidateTree, '-p', repo.webhookBaseCommit, '-m', 'synthetic unmerged base']);
    const wrong = mergeObject(repo.root, repo.candidateTree, invented, repo.sourceCommit, 'synthetic invented candidate');
    git(repo.root, ['checkout', '--quiet', '--detach', wrong]);
    assert.throws(() => resolveGitEvidenceProvenance(repo.root, { ...repo.env, GITHUB_SHA: wrong }),
      /Unable to establish revision-bound JEV Git provenance/u);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: an unrelated candidate base is rejected even when it matches a fetched ref', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    const unrelated = git(repo.root, ['-c', 'user.name=JEV Synthetic Fixture', '-c', 'user.email=jev-fixture@example.invalid',
      'commit-tree', repo.candidateTree, '-m', 'synthetic unrelated base']);
    git(repo.root, ['update-ref', 'refs/remotes/origin/main', unrelated]);
    const wrong = mergeObject(repo.root, repo.candidateTree, unrelated, repo.sourceCommit, 'synthetic unrelated candidate');
    git(repo.root, ['checkout', '--quiet', '--detach', wrong]);
    assert.throws(() => resolveGitEvidenceProvenance(repo.root, { ...repo.env, GITHUB_SHA: wrong }),
      /Unable to establish revision-bound JEV Git provenance/u);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: a forged checkout tree with the same parents cannot replace the event SHA', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    const wrong = mergeObject(repo.root, git(repo.root, ['rev-parse', `${repo.webhookBaseCommit}^{tree}`]),
      repo.evaluatedBaseCommit, repo.sourceCommit, 'synthetic forged checkout tree');
    git(repo.root, ['update-ref', repo.ref, wrong]);
    git(repo.root, ['checkout', '--quiet', '--detach', wrong]);
    assert.throws(() => resolveGitEvidenceProvenance(repo.root, repo.env),
      /Unable to establish revision-bound JEV Git provenance/u);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: missing or unsafe event provenance and missing fetched base fail closed', () => {
  const repo = pullRequestFixture({ advancedBase: true });
  try {
    for (const field of ['GITHUB_SHA', 'GITHUB_REF', 'GITHUB_EVENT_PATH', 'GITHUB_EVENT_NAME']) {
      const env = { ...repo.env }; delete env[field];
      assert.throws(() => resolveGitEvidenceProvenance(repo.root, env), /Unable to establish revision-bound JEV Git provenance/u);
    }
    for (const branch of ['--help', 'main^{commit}', 'main/../other']) {
      const payload = structuredClone(repo.payload); payload.pull_request.base.ref = branch;
      const env = githubEnvironment(repo.root, payload, { eventName: 'pull_request', ref: repo.ref, sha: repo.candidateCommit });
      assert.throws(() => resolveGitEvidenceProvenance(repo.root, env), /Unable to establish revision-bound JEV Git provenance/u);
    }
    githubEnvironment(repo.root, repo.payload, { eventName: 'pull_request', ref: repo.ref, sha: repo.candidateCommit });
    git(repo.root, ['update-ref', '-d', 'refs/remotes/origin/main']);
    assert.throws(() => resolveGitEvidenceProvenance(repo.root, repo.env), /Unable to establish revision-bound JEV Git provenance/u);
  } finally { rmSync(repo.root, { recursive: true, force: true }); }
});

test('evidence_provenance: GitHub source SHA mismatch fails instead of choosing another base', () => {
  const repo = featurePushFixture();
  try {
    assert.throws(
      () => resolveGitEvidenceProvenance(repo.root, { ...repo.env, GITHUB_SHA: repo.baseCommit }),
      /Unable to establish revision-bound JEV Git provenance/u
    );
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
});
