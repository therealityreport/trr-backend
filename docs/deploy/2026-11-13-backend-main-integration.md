# Backend main integration — 2026-11-13

## Reviewed source

Integrates `chore/repo-hygiene-2026-09-25` at `23ec221787dd33451734c362b992ca980ee0977c`
with `main` at `7fca52d82a3c4f98fadfe01aa3b12f4770b7e819`. The branch has 21 unique
patches and three storage patches already represented on main. Main's dependency
locks and CI updates are retained. No migrations or stored records are changed.

Independent source reviews covered the unique commits and the uncommitted batch.
The integration repairs these findings:

- Keep legacy comments-only filters and search semantics with new Instagram count sorts.
- Use the same owner and collaborator membership rules for count pages, summaries,
  and normal profile hydration.
- Check a bounded detail response's frozen native ID, shortcode, and owner before
  retaining it. Use the actual persisted `post_details` stage.
- Load the API minimum/maximum container settings from the selected Modal source
  environment while preserving explicit process overrides.
- Update tests for Modal 1.6.3's supported local invocation and make the deploy
  dry-run test independent of a developer's environment file.

Nine independent pre-existing edits are included: `CONTRIBUTING.md`, `README.md`,
`docs/Repository/README.md`, `docs/SECURITY.md`, `docs/README_local.md`,
`docs/api/run.md`, and `scripts/db/{README.md,guard_core_schema.sql,run_sql.sh}`.
They correct source-backed guidance and make the SQL runner stop on SQL errors.
The SQL options do not verify database identity; operators must still verify it.

## Preserved unfinished work

The user explicitly directed this integration to keep active work uncommitted.
The primary checkout retains all other edits and untracked files. They are not
accepted by this merge. In particular:

- Getty workflow/session renewal: administrator ownership, dispatch failure,
  shared discovery/detail budgets, error classification, and larger resumable jobs
  need further work. Production transport success is not full workflow acceptance.
- Decodo policy and Modal overlays: test every actual client boundary, retained
  credentials, and recovery scheduling before acceptance.
- Media streaming: the new activity session needs its production caller and
  direct lifecycle checks. Cast-photo pruning must report partial delete failures.
- SocialBlade/TikTok/Threads: retain source-specific retry, diagnostic, session,
  and completeness work with its existing owner.
- Remaining documentation: correct stale Scrapling/hosting claims and the restore
  example's environment precedence before inclusion.
- Test cleanup: retain all proposed deletions here. Credits and TMDb removals need
  a behavior-coverage map; placeholder removal alone does not establish coverage.
- `.hygiene/` is retained audit evidence, not a completed repair or acceptance record.

## Validation

- Focused merged-candidate run: 401 passed, six failures. The failures identified
  the stage fixture mismatch, environment-dependent dry run, and obsolete SDK API.
- Affected repair run: 92 passed, covering every failed check and affected behavior.
- Existing summary-contract regression: one passed.
- Real isolated PostgreSQL: owner/raw collaborator membership, count ordering,
  pagination totals, and empty zero-count selection passed. No configured TRR DB used.
- SQL runner: shell syntax and four stub cases passed (command/file modes, successful
  and failed psql exits); no database connection was made.
- All 49 changed Python files parse and pass Ruff. GitHub exposed formatting
  differences under its pinned Ruff 0.17.4; all changed files now pass that exact
  check and formatter. AST equality verifies the formatting follow-up changes no behavior.
  Repository hygiene and environment example checks pass. App direct-SQL inventory check passes; SQL ownership unchanged.
- Independent review of the complete committed batch and affected repairs found no
  remaining merge blocker. Deferred sealed-manifest corruption hardening has no
  identified normal execution path and is not treated as a passing live check.

GitHub's initial merge run passed the API tests, Python 3.12 canary, secret scan,
and Modal lock freshness. It exposed 53 Pyright errors in the integrated code and
fixtures. The follow-up makes optional values explicit, preserves callable hook
signatures, and uses valid typed test doubles. It also rejects non-text timestamps
through the existing controlled error path. Focused follow-up tests passed:
66 Twitter/policy tests, 62 Instagram tests, and 61 ingestion tests. Independent
review of the follow-up found no actionable defects. Ruff 0.17.4 passes.
Pyright 1.1.390 checks all 1,749 files with zero errors (88 existing warnings).
Its final run used a 3 GB Node heap after the initial 2 GB limit was exhausted.

Private verification logs and the preservation manifest are retained in
`/Users/thomashulihan/Documents/Codex/2026-11-13/survey-profile-release/backend-integration/`.
Tests used an isolated environment with main's requirements lock and Python 3.11.17;
main declares Python 3.11.18, which was not installed locally. GitHub CI owns that
exact declared-interpreter check.

## Production boundary

Provider readback found `admin-56995 / main / trr-backend-jobs` on **v208**, tag
`plan057-getty-trr-cookies-20261113`. This includes newer, unfinished Getty and policy
work outside the committed branch. Deploying this merge by itself would remove
those changes. This Git integration therefore preserves v208 and does not claim
that the new integration repairs are deployed. A coordinated release must preserve
that runtime overlay, complete its owner's acceptance, and verify the actual API
maintenance owner and all expected schedules before deployment.

No Vercel or Firestore activation is part of this backend Git integration. App
runtime and shared API payload shapes are unchanged by the integration repairs;
no app production build is required for them.
