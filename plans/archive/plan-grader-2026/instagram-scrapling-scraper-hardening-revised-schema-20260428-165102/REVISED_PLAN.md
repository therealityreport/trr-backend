# Instagram Scrapling Scraper Hardening Revised Plan

schema_version: `write-plan-v1`
source_plan: `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md`
source_suggestions: `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/SUGGESTIONS.md`
recommended_next_execution_skill: `orchestrate-subagents`
ready_for_execution: `conditional_on_branch_preflight`

## summary

Harden the Instagram Scrapling scraper lanes so dispatcher fallback, fetch retries, warmup failures, job metadata, cancellation, session adapters, docs, and verification are consistent and safe. This plan intentionally keeps the pluggable `ScraplingRuntime` unsupported until a separate implementation plan wires it to current Scrapling APIs.

Execution should use `orchestrate-subagents` from the main session. The orchestrator must keep sequencing, ownership boundaries, validation, and final integration in the main session. No branch or worktree may be created by the orchestrator or any subagent.

## project_context

- Repo: `/Users/thomashulihan/Projects/TRR/TRR-Backend`.
- Current branch observed during revision: `chore/backend-batch-2026-04-28`.
- `orchestrate-subagents` expects branch `main`. If implementation begins while the branch is not `main`, stop before mutation and ask for explicit user direction.
- Current uncommitted state observed during revision: untracked `.plan-grader/` artifacts and `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md`.
- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py` currently reports healthy when `scrapling` imports but raises `NotImplementedError` from runtime endpoint methods.
- `trr_backend/socials/instagram/runtimes/dispatcher.py` skips unhealthy runtimes and only falls through endpoint calls on `RuntimeUnsupported`.
- `InstagramAuthSession` requires more fields than the old shared-session test snippet supplied, so tests must use `SimpleNamespace` or a fully valid session object.
- `pg.fetch_one` accepts a `conn` keyword argument, so comments-lane cancellation checks can reuse `persist_conn`.

## assumptions

- The backend virtualenv remains at `.venv/`.
- Scrapling is available locally as `scrapling==0.4.7`, but the plan still verifies package surface before implementation.
- Unit tests should use local fakes and mocks; live Instagram scraping is not part of default validation.
- Existing user-owned changes must not be reverted or overwritten.
- Each subagent edits only its assigned ownership scope and reports changed files.

## goals

- Make `ScraplingRuntime` a safe unsupported scaffold that cannot intercept dispatcher traffic and crash with `NotImplementedError`.
- Classify `httpx.TransportError` and timeout failures as retryable transport failures in both posts and comments lanes.
- Add posts warmup parity with comments, including no-cookie warmup errors, homepage redirect recovery, and preserved runtime metadata.
- Add cooperative cancellation that stops between work units without increasing comments-lane DB pool pressure.
- Share posts/comments Scrapling session cookie adapter logic.
- Incorporate all ten prior Plan Grader suggestions as required tasks under `ADDITIONAL SUGGESTIONS`.
- Make implementation executable through `orchestrate-subagents` with clear ownership, validation, and handoff requirements.

## non_goals

- Do not implement the full pluggable `ScraplingRuntime` fetch methods in this pass.
- Do not rewrite `trr_backend/socials/instagram/scraper.py`.
- Do not change app routes, public API contracts, Supabase schema, or admin UI behavior.
- Do not run a live Instagram scrape unless the operator explicitly approves a bounded smoke.
- Do not create branches or worktrees during orchestration.

## phased_implementation

### Phase 0 - Orchestrator preflight

Owner: main session only.

Dependencies: none.

Concrete steps:

- [ ] Confirm approved plan source is this `REVISED_PLAN.md`.
- [ ] Read repo instructions before mutation: `../AGENTS.md`, `TRR Backend Brain/BRAIN.md`, and any directly relevant handoff files.
- [ ] Run `git branch --show-current`. Expected for `orchestrate-subagents`: `main`.
- [ ] If the branch is not `main`, stop before mutation and ask for explicit user direction. Do not create a branch or worktree.
- [ ] Run `git status --short` and classify dirty files as user-owned, plan-owned, or unknown.
- [ ] Verify Scrapling package surface:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python - <<'PY'
from scrapling.fetchers import StealthyFetcher
import inspect
import scrapling
print("scrapling", scrapling.__version__)
print("StealthyFetcher.async_fetch", inspect.signature(StealthyFetcher.async_fetch))
PY
```

- [ ] Run the current dispatcher baseline:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest tests/socials/instagram/runtimes/test_dispatcher.py -q
```

Validation: branch/status recorded, package surface verified, dispatcher baseline result captured.

Acceptance criteria: the orchestrator has a clear proceed/stop decision and a clean ownership map before dispatching subagents.

Commit boundary: no commit; preflight evidence only.

### Phase 1 - Runtime and dispatcher scaffold

Owner: runtime/dispatcher subagent.

Allowed files:

- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- `trr_backend/socials/instagram/runtimes/__init__.py`
- `tests/socials/instagram/runtimes/test_scrapling_runtime.py`
- `tests/socials/instagram/runtimes/test_dispatcher.py`

Out of scope: posts/comments fetchers, job runners, session adapters, docs.

Concrete steps:

- [ ] Add tests proving `ScraplingRuntime.healthcheck()` is unhealthy with reason `scrapling_runtime_not_wired`.
- [ ] Add tests proving all pluggable `ScraplingRuntime` endpoint methods raise `RuntimeUnsupported`, not `NotImplementedError`.
- [ ] Add a dispatcher test proving an unhealthy Scrapling scaffold is skipped and a healthy fallback runtime still serves the request.
- [ ] Change `ScraplingRuntime.healthcheck()` to return `RuntimeHealth(healthy=False, reason="scrapling_runtime_not_wired")` when Scrapling imports.
- [ ] Change endpoint methods and `_fetch_json()` to raise `RuntimeUnsupported` with method-specific messages.
- [ ] Update runtime package docs to clarify external-package runtimes must stay unhealthy or unsupported until current-version APIs are verified.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  -q
```

Acceptance criteria: dispatcher cannot route to a healthy-but-unimplemented Scrapling runtime.

Commit boundary: `fix: make instagram scrapling runtime scaffold unsupported`.

### Phase 2 - Shared transport retry classification

Owner: fetcher/retry subagent.

Allowed files:

- `trr_backend/socials/_scrapling_http_utils.py`
- `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- `tests/socials/test_scrapling_http_utils.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`

Out of scope: runtime dispatcher, job runner cancellation, session adapter wrappers.

Concrete steps:

- [ ] Add helper tests for transport reason mapping and bounded retry-after-aware backoff.
- [ ] Add `_scrapling_http_utils.transport_failure_reason(exc)` with stable reasons `transport_timeout` and `transport_error`.
- [ ] Add `_scrapling_http_utils.transient_backoff_seconds(attempt, base_seconds, retry_after=None)`.
- [ ] Update posts and comments fetchers to catch `(TimeoutError, httpx.TimeoutException, httpx.TransportError)`.
- [ ] Return retryable failure payloads with `reason` set to `transport_timeout` or `transport_error` after retry exhaustion.
- [ ] Use the shared backoff helper for transient statuses and transport exceptions.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/test_scrapling_http_utils.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Acceptance criteria: posts/comments lanes classify broad httpx transport failures as retryable without duplicating backoff logic.

Commit boundary: `fix: classify instagram scrapling transport errors as retryable`.

### Phase 3 - Posts warmup parity and runtime metadata

Owner: posts fetcher/job-runner subagent.

Allowed files:

- `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- `tests/socials/instagram/posts_scrapling/test_job_runner.py`

Out of scope: comments runner cancellation and shared session wrappers unless required by failing tests already owned by another scope.

Concrete steps:

- [ ] Add `InstagramPostsWarmupError` with `error_code` and `retryable`.
- [ ] Make posts warmup raise `instagram_posts_warmup_auth_failed` for auth/challenge pages.
- [ ] Make posts warmup raise `instagram_posts_warmup_no_cookies` when warmup bridges no cookies and no prior `sessionid` exists.
- [ ] Add one-shot homepage/profile recovery for redirects to the Instagram home surface.
- [ ] Catch `InstagramPostsWarmupError` in `posts_scrapling/job_runner.py`.
- [ ] Raise `PostsScraplingRuntimeError` from warmup failures while preserving `dict(fetcher.runtime_metadata)`.
- [ ] Refresh `fetcher_metadata` after each posts page and immediately before return.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  -q
```

Acceptance criteria: posts warmup failures are diagnosable and final job metadata reflects the latest fetcher runtime state.

Commit boundary: `fix: harden instagram posts scrapling warmup metadata`.

### Phase 4 - Cooperative cancellation and degraded summaries

Owner: job-runner/cancellation subagent.

Allowed files:

- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`

Out of scope: fetcher retry classification, session adapter wrappers, docs.

Concrete steps:

- [ ] Add `ScraplingJobCancelled` to both job runners, or extract only if the existing codebase style makes a shared helper cleaner.
- [ ] Add `_raise_if_cancelled(job_id, run_id, runtime_metadata=None, conn=None)` to both job runners or a shared module.
- [ ] Query `social.scrape_jobs.status` and linked `social.scrape_runs.status`; raise cancellation for job or run cancellation.
- [ ] In comments runner, check once after warmup and before opening `pg.db_connection`.
- [ ] In the comments target-post loop, call cancellation with `conn=persist_conn`.
- [ ] In posts runner, check before each page fetch.
- [ ] Handle cancellation before retry handling and finish jobs with status `cancelled`.
- [ ] Use explicit counts: comments `items_found = processed_posts + comments_fetched`; posts `items_found = posts_fetched`.
- [ ] Add posts final summary degraded fallback for `pg.DatabaseServiceUnavailableError`, matching comments behavior.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Acceptance criteria: cancellation stops between work units, comments cancellation does not add an extra pool checkout while `persist_conn` is held, and posts can return a degraded completed summary if the final DB summary read is saturated.

Commit boundary: `fix: make instagram scrapling jobs cancellable and observable`.

### Phase 5 - Shared Scrapling session adapter

Owner: session adapter subagent.

Allowed files:

- `trr_backend/socials/instagram/scrapling_session.py`
- `trr_backend/socials/instagram/posts_scrapling/session.py`
- `trr_backend/socials/instagram/comments_scrapling/session.py`
- `tests/socials/instagram/test_scrapling_session.py`
- Existing posts/comments session tests if needed.

Out of scope: auth resolver behavior beyond calling `resolve_instagram_auth_session`.

Concrete steps:

- [ ] Create `InstagramScraplingSession` and `cookies_to_scrapling()` in `scrapling_session.py`.
- [ ] Filter blank cookie names and values, trim values, and emit Scrapling cookie dictionaries with `.instagram.com` domain and `/` path.
- [ ] Resolve auth through `resolve_instagram_auth_session` with `browser_account_id` and `caller_context`.
- [ ] Replace posts/comments session modules with compatibility wrappers that preserve `resolve_posts_scrapling_session` and `resolve_comments_scrapling_session`.
- [ ] Test shared behavior using `SimpleNamespace`, not a partial `InstagramAuthSession` constructor.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/test_instagram_comments_scrapling.py \
  -q
```

Acceptance criteria: posts and comments resolve through the same cookie adapter without breaking existing public helper names.

Commit boundary: `refactor: share instagram scrapling session adapter`.

## ADDITIONAL SUGGESTIONS

These tasks incorporate every numbered suggestion from the source `SUGGESTIONS.md`. They are required for this revised plan.

### Suggestion 1 - Add a one-page Scrapling lane architecture diagram

Concrete changes:

- [ ] Add a compact Mermaid diagram to `docs/workspace/instagram-posts-scrapling.md`.
- [ ] Cross-link or duplicate the same diagram in `docs/workspace/instagram-comments-scrapling.md`.
- [ ] Show legacy scraper, posts Scrapling lane, comments Scrapling lane, and unsupported pluggable `ScraplingRuntime` as separate boxes.

Dependencies: after Phase 1 terminology is finalized.

Affected surfaces: posts and comments Scrapling runbooks.

Validation: inspect docs with `rg -n "Scrapling lane architecture|mermaid|ScraplingRuntime" docs/workspace/instagram-*scrapling.md`.

Acceptance criteria: a reader can distinguish runtime scaffold from posts/comments lanes without reading code.

Commit boundary: include with docs commit.

### Suggestion 2 - Add a static no-cookie-values metadata scanner

Concrete changes:

- [ ] Add a focused test or script that scans scraper metadata samples for cookie-like keys and raw cookie values.
- [ ] Include posts and comments metadata examples, including warmup metadata and final job metadata.
- [ ] Ensure the scanner allows counts, booleans, fingerprints, and source labels while rejecting `sessionid`, `csrftoken`, `ds_user_id`, and cookie value patterns.

Dependencies: after Phases 3 and 4 define final metadata keys.

Affected surfaces: `tests/socials/` or `scripts/socials/instagram/`.

Validation: run the new scanner test plus existing cookie metadata tests.

Acceptance criteria: future metadata regressions fail locally before reaching operator surfaces.

Commit boundary: `test: guard instagram scrapling metadata against cookie leaks`.

### Suggestion 3 - Add a local fake Instagram response fixture pack

Concrete changes:

- [ ] Create `tests/fixtures/instagram/scrapling/`.
- [ ] Add JSON or HTML fixtures for successful GraphQL, transient error, home redirect, auth failure, no-cookie warmup, and comments response shapes.
- [ ] Refactor at least the newly added retry/warmup tests to use fixture helpers instead of only ad hoc `MagicMock` payloads.

Dependencies: after Phase 2 and Phase 3 test expectations are stable.

Affected surfaces: fixture folder and posts/comments fetcher tests.

Validation: run posts/comments fetcher test suites.

Acceptance criteria: fixture names make response drift reviewable without reverse-engineering mocks.

Commit boundary: `test: add instagram scrapling response fixtures`.

### Suggestion 4 - Track retry reason counts in job metadata

Concrete changes:

- [ ] Add fetcher runtime metadata counters for retry reasons such as `transport_error`, `transport_timeout`, retryable status codes, and homepage redirect recovery attempts.
- [ ] Increment counters in posts and comments fetchers without storing URLs containing sensitive query data or cookie values.
- [ ] Surface the counters through final job metadata under existing runtime metadata structures.

Dependencies: after Phase 2 shared reason names land.

Affected surfaces: posts/comments fetchers, job metadata tests.

Validation: add tests that trigger mixed retry reasons and assert counts appear in runtime metadata.

Acceptance criteria: operators can distinguish repeated proxy/network failures from mixed transient HTTP statuses.

Commit boundary: `feat: track instagram scrapling retry reason counts`.

### Suggestion 5 - Add worker restart note to local runbook

Concrete changes:

- [ ] Add a runbook note explaining that stale workers may keep old cancellation/retry behavior until restarted.
- [ ] Include the local command or existing project runbook pointer for restarting backend worker processes.
- [ ] Place the note near cancellation or validation sections in the comments and posts runbooks.

Dependencies: after Phase 4 cancellation behavior is documented.

Affected surfaces: `docs/workspace/instagram-comments-scrapling.md`, `docs/workspace/instagram-posts-scrapling.md`.

Validation: `rg -n "restart|stale worker|cancellation" docs/workspace/instagram-*scrapling.md`.

Acceptance criteria: local validation instructions explicitly prevent stale-worker confusion.

Commit boundary: include with docs commit.

### Suggestion 6 - Split shared retry helpers into unit tests

Concrete changes:

- [ ] Add `tests/socials/test_scrapling_http_utils.py`.
- [ ] Test timeout vs transport error reason mapping.
- [ ] Test exponential backoff and `Retry-After` override behavior.

Dependencies: Phase 2 helper module.

Affected surfaces: `_scrapling_http_utils.py` tests.

Validation: `.venv/bin/python -m pytest tests/socials/test_scrapling_http_utils.py -q`.

Acceptance criteria: helper behavior is locked independently from posts/comments fetcher tests.

Commit boundary: include with Phase 2 retry commit.

### Suggestion 7 - Add a final smoke command for one page only

Concrete changes:

- [ ] Add an optional operator-approved one-page smoke command to runbooks and final verification.
- [ ] Mark it as manual only and not part of default CI.
- [ ] Include expected metadata fields to inspect after the smoke.

Dependencies: after Phases 2 through 5 land.

Affected surfaces: runbooks and final validation section.

Validation: no live command is run by default; docs contain a clear manual-only smoke block.

Acceptance criteria: operators have a bounded live check without making live scraping part of automated validation.

Commit boundary: include with docs commit.

### Suggestion 8 - Add cancellation latency to worker logs

Concrete changes:

- [ ] Log when cancellation is observed, including `job_id`, `run_id`, `cancel_scope`, current stage, and last progress timestamp if already available.
- [ ] Avoid logging raw cookies, request payloads, or target URLs with sensitive query data.
- [ ] Add tests or log assertions where the job runner test pattern supports them.

Dependencies: Phase 4 cancellation helper.

Affected surfaces: posts/comments job runners.

Validation: cancellation tests assert final metadata; log assertions are added if they stay stable.

Acceptance criteria: operators can distinguish expected in-flight delay from missing cancellation checks.

Commit boundary: include with Phase 4 cancellation commit.

### Suggestion 9 - Add a short glossary for runtime vs lane

Concrete changes:

- [ ] Add a runbook glossary defining `ScraplingRuntime`, posts Scrapling lane, comments Scrapling lane, legacy scraper, warmup, and cooperative cancellation.
- [ ] Keep glossary wording consistent with Phase 1 runtime scaffold behavior.

Dependencies: after Phase 1.

Affected surfaces: posts/comments runbooks.

Validation: `rg -n "Glossary|ScraplingRuntime|lane" docs/workspace/instagram-*scrapling.md`.

Acceptance criteria: support and implementation readers do not conflate the unsupported pluggable runtime with the production posts/comments lanes.

Commit boundary: include with docs commit.

### Suggestion 10 - Create a future plan for implementing `ScraplingRuntime`

Concrete changes:

- [ ] Create a short future-plan stub under `docs/codex/plans/` or `docs/superpowers/plans/` for implementing the pluggable `ScraplingRuntime`.
- [ ] State that this hardening plan deliberately marks the runtime unsupported.
- [ ] List required future evidence: current Scrapling docs/API verification, recorded response fixtures, contract tests, dispatcher rollout strategy, and operator rollback.

Dependencies: after Phase 1 clarifies unsupported scaffold.

Affected surfaces: plan docs only.

Validation: `test -f` the future-plan stub and inspect it for the required evidence list.

Acceptance criteria: the larger runtime implementation is separated from this operational hardening work.

Commit boundary: `docs: outline future instagram scrapling runtime implementation`.

### Phase 6 - Runbook updates and documentation integration

Owner: session/docs subagent after core code phases.

Allowed files:

- `docs/workspace/instagram-posts-scrapling.md`
- `docs/workspace/instagram-comments-scrapling.md`
- Future plan file from Suggestion 10

Concrete steps:

- [ ] Document new posts warmup error codes.
- [ ] Document retryable `transport_error` and `transport_timeout`.
- [ ] Document cooperative cancellation semantics and stale worker restart.
- [ ] Use four-backtick outer fences when documenting commands that contain triple-backtick or heredoc snippets.
- [ ] Add architecture diagram, glossary, manual smoke command, and future runtime implementation plan pointer.

Validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "instagram_posts_warmup_no_cookies|transport_error|Cooperative cancellation|Scrapling lane architecture|Glossary|manual" docs/workspace/instagram-*scrapling.md
```

Acceptance criteria: docs match implemented behavior and do not imply live smoke is automatic.

Commit boundary: `docs: update instagram scrapling scraper runbooks`.

### Phase 7 - Integration validation and handoff

Owner: main session orchestrator.

Dependencies: all implementation subagents have returned, changes have been reviewed, and no reported blockers remain.

Concrete steps:

- [ ] Review each subagent report and inspect changed files.
- [ ] Resolve integration conflicts in the main session.
- [ ] Run focused tests:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/test_scrapling_http_utils.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

- [ ] Run compile check:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m py_compile \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  trr_backend/socials/instagram/scrapling_session.py
```

- [ ] Run Ruff:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/ruff check \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/runtimes/__init__.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  trr_backend/socials/instagram/scrapling_session.py \
  trr_backend/socials/instagram/posts_scrapling/session.py \
  trr_backend/socials/instagram/comments_scrapling/session.py \
  tests/socials/test_scrapling_http_utils.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  tests/socials/test_instagram_comments_scrapling.py
```

- [ ] Run Ruff format check on the same Python paths.
- [ ] Run cookie metadata leak tests and the new static scanner.
- [ ] Run `git diff --check`.

Acceptance criteria: all focused automated checks pass or any blocked checks are explicitly documented with reason and residual risk.

Commit boundary: final integration commit only if the user asked for commits.

## architecture_impact

- The dispatcher contract becomes safer because `ScraplingRuntime` cannot advertise health until it is wired.
- Posts and comments Scrapling lanes share retry classification and session adapter behavior while preserving current public helper names.
- Job runners gain cooperative cancellation between pages or target posts; cancellation does not interrupt in-flight Instagram requests.
- Operator docs become clearer about the difference between the unsupported pluggable runtime and the production posts/comments lanes.

## data_or_api_impact

- No database schema changes.
- No public API route changes.
- Job metadata gains safer diagnostic fields, including runtime metadata, retry reason counts, cancellation scope, and degraded summary markers.
- Metadata must not include raw cookie values or sensitive request payloads.

## ux_admin_ops_considerations

- Admin/operator surfaces should see clearer terminal status and metadata when jobs are cancelled, retrying, degraded, or blocked by warmup.
- Runbooks must instruct operators to restart stale workers before local/live validation.
- Manual one-page smoke is optional and operator-approved only.
- Future implementation of the pluggable `ScraplingRuntime` is explicitly deferred to a separate plan.

## validation_plan

Default automated validation:

- Focused pytest for runtime, dispatcher, session adapter, posts fetcher, comments fetcher, retry helpers, and job runners.
- `py_compile` for touched Python modules.
- `ruff check` and `ruff format --check` for touched Python files.
- Static no-cookie metadata scanner.
- `git diff --check`.

Manual validation:

- Optional one-page live smoke only after operator approval.
- Verify worker restart before manual smoke so old code is not still running.

Blocked checks to report:

- Branch not `main` for `orchestrate-subagents` execution.
- Missing Scrapling/Patchright dependencies.
- Any live Instagram challenge, auth failure, or proxy outage during optional smoke.

## acceptance_criteria

- `ScraplingRuntime.healthcheck()` returns unhealthy with `scrapling_runtime_not_wired` until real runtime implementation exists.
- Runtime endpoint methods raise `RuntimeUnsupported`.
- Posts/comments retry logic handles `TimeoutError`, `httpx.TimeoutException`, and `httpx.TransportError` as retryable transport failures.
- Posts warmup exposes stable error codes and job-runner metadata preserves warmup runtime metadata.
- Comments cancellation reuses `persist_conn` inside the persistence loop.
- Posts and comments have explicit cancellation item counts without dynamic branch logic based on local variable inspection.
- Shared session adapter tests use valid fakes and keep posts/comments compatibility helpers.
- All ten prior suggestions are integrated as required plan tasks.
- Implementation handoff uses `orchestrate-subagents` and returns the skill completion contract fields.

## risks_edge_cases_open_questions

- Current revision observed branch `chore/backend-batch-2026-04-28`, not `main`; implementation must stop before mutation unless the user approves the branch or switches to `main`.
- Cancellation remains cooperative and cannot interrupt an active network request.
- Broad transport retry handling can hide unhealthy proxies unless retry reason counts and runbook guidance are implemented.
- Live Instagram behavior may drift; default validation intentionally avoids live scraping.
- `ScraplingRuntime` remains unsupported by design, which may surprise operators unless docs are clear.

## follow_up_improvements

All prior numbered suggestions were accepted and integrated under `ADDITIONAL SUGGESTIONS`. Remaining follow-up beyond this plan is the separate full implementation of the pluggable `ScraplingRuntime` after current Scrapling APIs and recorded response fixtures are verified.

## recommended_next_step_after_approval

Use `orchestrate-subagents`.

Main-session execution rules:

- Confirm branch is `main` before mutation. If not `main`, stop and ask for explicit user direction.
- Do not create branches or worktrees.
- Dispatch only concrete, bounded work with disjoint ownership scopes.
- Tell every subagent it is not alone in the codebase, must not revert edits made by others, and must accommodate concurrent changes.
- Keep accepted phase sequencing intact. Do not let later workstreams bypass Phase 0 or prerequisite phases.
- Run integration and validation in the main session after subagents return.

Recommended ownership scopes:

- Runtime/dispatcher worker: Phase 1.
- Fetcher/retry worker: Phase 2 plus Suggestion 6.
- Posts warmup worker: Phase 3.
- Job-runner/cancellation worker: Phase 4 plus Suggestion 8.
- Session adapter worker: Phase 5.
- Docs/ops worker: Phase 6 plus Suggestions 1, 5, 7, 9, and 10.
- Test fixtures/metadata worker: Suggestions 2, 3, and 4 after related core phases land.

Required final `orchestrate-subagents` completion report fields:

- `approved_plan_reference`
- `scope_statement`
- `current_branch`
- `main_branch_verified`
- `worktree_or_branch_created` with value `false`
- `execution_mode`
- `subagents_used`
- `ownership_scopes`
- `phase_status`
- `files_changed`
- `validations_run`
- `blocked_checks`
- `remaining_risks`
- `acceptance_check_tracking`
- `ready_for_handoff`

## ready_for_execution

Status: conditional.

This plan is ready to execute with `orchestrate-subagents` after the branch preflight passes. If the active branch is still `chore/backend-batch-2026-04-28`, the orchestrator must stop before mutation and ask whether to continue on that branch or switch to `main`.

## Cleanup Note

After this plan is completely implemented and verified, delete any temporary planning artifacts that are no longer needed, including generated audit, scorecard, suggestions, comparison, patch, benchmark, and validation files. Do not delete them before implementation is complete because they are part of the execution evidence trail.
