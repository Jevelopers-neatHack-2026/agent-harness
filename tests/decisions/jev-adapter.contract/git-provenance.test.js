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
    assert.equal(provenance.candidateRef, ref);
    assert.equal(provenance.candidateCommit, candidateCommit);
    assert.equal(provenance.baseSelectionPolicy, 'verified_merge_candidate_first_parent');
  } finally {
    rmSync(repo.root, { recursive: true, force: true });
  }
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
