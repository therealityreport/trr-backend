# Instagram public isolation and Scrapling 0.5 release record

Status: user approved the explicit 35-path commit and deployment to admin-56995 / main / trr-backend-jobs on 2026-10-19. Deployment acceptance remains pending.

## Scope and source

- Branch: `chore/repo-hygiene-2026-09-25`.
- Base revision: `3c85e0d536dc361eebdd3d21c10d1a1bf486c7e8`.
- Fix strict public credentials/proxy isolation and actual transport diagnostics.
- Repair the disabled optional Scrapling runtime's timeout units, pagination and detail parser. Keep its feature flag disabled.
- Upgrade all three Scrapling dependency inputs/locks to 0.5; migrate SocialBlade and TikTok recording APIs together.
- Evaluate FetcherSession only. No production adoption is part of this release.
- Preserve unrelated hygiene changes, without reviewing or committing them. The authorized import-contract rewrite absorbs the two already-removed import tests; the commit message must name that absorbed hunk.

## Target and rollback

Live preflight on 2026-10-19 confirmed profile/workspace `admin-56995`, environment `main`, app `trr-backend-jobs`, app ID `ap-DkLTRoSvqhbkGO7fHyHxrD`.

Production remains v78, deployed 2026-10-16 with tag `ig-public-profile-20261017-690e08c1` and source label `690e08c*`. The asterisk denotes uncommitted source: that Git commit alone cannot reproduce the deployed artifact. Preserve the provider's v78 deployment as rollback evidence. The previous 0.4.12 locks remain recoverable from the base revision.

The installed Modal CLI supports explicit artifact rollback:

```sh
MODAL_PROFILE=admin-56995 MODAL_ENVIRONMENT=main .venv/bin/python -m modal app rollback ap-DkLTRoSvqhbkGO7fHyHxrD v78 --env main
```

This command is recorded only; it was not run. Deploy the candidate through `scripts/modal/deploy_backend.py` with explicit `--env main` after approval and commit. Do not activate either Modal profile globally.

## Dependency changes

Runtime/browser locks: Scrapling 0.4.12→0.5, curl-cffi 0.15.0→0.16.1, Patchright 1.61.2→1.62.1, Playwright 1.61.0→1.62.0, apify-fingerprint-datapoints 0.13.0→0.15.0, cssselect 1.4.0→1.5.0. Operator lock changes the first four; its other two versions were already compatible. Unaffected lock versions were retained.

The local environment was updated to Scrapling 0.5 constrained by the runtime lock. This is separate from deployed-version evidence. The browser runtime probe now reports curl-cffi and apify versions as well as Scrapling, Playwright and Patchright.

## Ephemeral FetcherSession evaluation

Command:

```sh
MODAL_PROFILE=admin-56995 MODAL_ENVIRONMENT=main .venv/bin/python scripts/modal/evaluate_fetcher_session.py --run
```

The script pins and verifies workspace ownership, uses a dedicated `trr-fetcher-eval` app and the shared 0.5 browser-image builder, and attaches no secrets. It compares requests with synchronous FetcherSession for listing/detail, and HTTPX with asynchronous FetcherSession for public Relay comments. Application headers/parsers/pacing are shared; Chrome impersonation is recorded as a property of the FetcherSession arm. Limits: 60 requests across both arms, two listing pages, three shared detail shortcodes, and at most two comment POSTs per sample. Redirects, transport retries, environment proxies and browser/auth recovery are disabled; throttle/challenge/credential rejection stops the whole run. Sessions close on exit.

Observed result: **inconclusive**. The baseline's first listing request returned HTTP 302 (0-byte body; 0.255 seconds). The public-response guard stopped after one request. No listing pages, details or comments were collected; the FetcherSession arm was not attempted. This provides no comparative speed, success-rate or adoption evidence. Do not retry around the stop guard.

- Completed run: `ap-X0Gtj71MrvGr8vJkIAMw1P`, 2026-10-19 15:57 EDT.
- Initial startup attempt: `ap-I8s7sH6Z8cn9Q5Ln4RD5p0`; failed before source requests because Modal mounts the entry script under `/root`. The script now handles that path. The failed run was explicitly stopped.
- Both ephemeral apps were verified `stopped`, with zero tasks. Production remained deployed.
- The shared image installed its locked packages and completed both `playwright install chromium` and `scrapling install --force` successfully.
- Local evaluation policy fixtures: five passed, including hostile proxy environment, credential rejection, budget/throttle stop and async session closure.

## Candidate validation

- Shared transport, SocialBlade, TikTok XHR-enabled and public import contract: 64 passed. After removing an obsolete SocialBlade fixture attribute, its 19 affected tests passed.
- Optional runtime: 15 passed, including real HTML parsing, second-page REST-shaped posts, deduplication and repeated-cursor rejection.
- Threads scraper/fetcher smoke fixtures: 30 passed under Scrapling 0.5.
- Browser image version-probe fixtures: two passed.
- Scoped formatting/lint and whitespace checks passed for completed work.
- Public-isolation focused checks: 211 passed. Expanded checks: 400 passed, 35 skipped, 32 failed and nine setup errors. Of the failures, 29 and all nine errors require unavailable isolated database infrastructure; the remaining three auth-block cases also fail against unchanged HEAD due to the existing job-claim-scope mock. No test guard was bypassed.
- Six new credential-isolation regressions fail against unchanged HEAD and pass against this candidate. Final affected transport metadata checks passed (30 cases, then two cases after the last attribution adjustment); both proxy-rotation regression cases passed.
- Final optional-runtime/evaluation policy selection: 20 passed, including a real curl option hook proving environment proxies are overridden explicitly. This local strengthening does not change the live evaluation conclusion.
- Independent review completed with no findings across all 35 scoped paths. The reviewer checked installed 0.5 recorder/session contracts and ran a request-free real-Response parsing probe; all source hashes remained unchanged during review. Detailed commands and local logs are recorded in the task evidence directory.

## Deployment acceptance pending

After approval, commit only the final task manifest, then deploy through the wrapper with `main` explicitly selected. Reuse passing source evidence. Verify deployed browser package versions and browser launch, the SocialBlade runtime probe, Threads remote-auth smoke, core workers, and a bounded public Instagram probe. Use `verify_modal_readiness.py --env main --probe-core-workers --strict-probes`; add `--probe-remote-auth threads` for the Threads smoke. Keep all commands pinned to `MODAL_PROFILE=admin-56995` and `MODAL_ENVIRONMENT=main`.

A source redirect or challenge is a blocked public acceptance result, not proof of successful scraping. Do not label fixture checks as deployed acceptance. Record the final revision, deployment version, readiness/smoke outcomes, dispatch target, and remaining uncommitted paths with image-boundary classification here before closeout.

TRR-APP build: not applicable (backend scraper/runtime change without an app contract change). SQL ownership: unchanged.

## Remaining dirty paths outside this release

The following 53 existing paths are outside the proposed task commit and outside the Modal image upload/dependency paths. Classification uses path names only; their changes were not reviewed. The image builders upload `api`, `trr_backend`, the listed social script folders/files and `scripts/sync`; dependency layers consume their explicit lock files. This table must be refreshed after the approved commit.

| Git status | Path | Image classification |
| --- | --- | --- |
| `M` | `.claude/commands/font-sync.md` | Outside |
| `M` | `.claude/commands/trr-impl.md` | Outside |
| `M` | `.claude/commands/trr-wt-new.md` | Outside |
| `M` | `.claude/hooks/before-bash.md` | Outside |
| `M` | `.claude/hooks/on-stop.md` | Outside |
| `M` | `CONTRIBUTING.md` | Outside |
| `M` | `README.md` | Outside |
| `M` | `docs/README_local.md` | Outside |
| `M` | `docs/Repository/README.md` | Outside |
| `M` | `docs/SECURITY.md` | Outside |
| `M` | `docs/ai/MODEL_GOVERNANCE.md` | Outside |
| `M` | `docs/ai/tiktok-posts-scrapling-signature-gap.md` | Outside |
| `M` | `docs/api/run.md` | Outside |
| `M` | `docs/architecture.md` | Outside |
| `M` | `docs/architecture/integrations.md` | Outside |
| `M` | `docs/architecture/pipeline.md` | Outside |
| `M` | `docs/architecture/social-compatibility-wrapper-ledger.md` | Outside |
| `M` | `docs/architecture/social-platform-module-checklist.md` | Outside |
| `M` | `docs/architecture/social_ingest_n8n_setup.md` | Outside |
| `M` | `docs/cloud/quick_cloud_setup.md` | Outside |
| `M` | `docs/db/schema.md` | Outside |
| `M` | `docs/db/verification.md` | Outside |
| `M` | `docs/deploy/R2-setup.md` | Outside |
| `M` | `docs/deploy/cloud_run.md` | Outside |
| `M` | `docs/deploy/render.md` | Outside |
| `M` | `docs/media-scraping/source-coverage-matrix.md` | Outside |
| `M` | `docs/runbooks/credits_v2_rollout.md` | Outside |
| `M` | `docs/runbooks/postgrest_schema_cache.md` | Outside |
| `M` | `docs/runbooks/show_import_job.md` | Outside |
| `M` | `docs/runbooks/social_worker_queue_ops.md` | Outside |
| `M` | `docs/runbooks/supabase_backup_restore.md` | Outside |
| `M` | `docs/runbooks/supabase_migration_history_repair.md` | Outside |
| `M` | `docs/runbooks/tiktok-ytdlp-degradation.md` | Outside |
| `M` | `docs/social/instagram-api-contract-appendix.md` | Outside |
| `M` | `docs/social/instagram-data-contract.md` | Outside |
| `M` | `docs/social/instagram-data-field-map.json` | Outside |
| `M` | `docs/workspace/instagram-comments-scrapling.md` | Outside |
| `M` | `docs/workspace/instagram-posts-scrapling.md` | Outside |
| `M` | `scripts/db/README.md` | Outside |
| `M` | `scripts/db/guard_core_schema.sql` | Outside |
| `M` | `scripts/db/run_sql.sh` | Outside |
| `M` | `scripts/media/README.md` | Outside |
| `M` | `scripts/media/mirror_cast_photos_to_s3.py` | Outside |
| `M` | `supabase/schema_docs/INDEX.md` | Outside |
| `M` | `tests/api/test_survey_submit.py` | Outside |
| `M` | `tests/integrations/tmdb/test_tmdb_tv_details_persistence.py` | Outside |
| `M` | `tests/media/test_s3_mirror.py` | Outside |
| `M` | `tests/repositories/test_credits.py` | Outside |
| `M` | `tests/test_api_smoke.py` | Outside |
| `M` | `tests/test_discussions_smoke.py` | Outside |
| `M` | `tests/test_dms_smoke.py` | Outside |
| `??` | `.hygiene/` | Outside |
| `??` | `docs/deploy/archive/` | Outside |

## Proposed explicit commit paths

Only these 35 paths are proposed for the approval-gated commit:

```text
requirements.in
requirements.modal.browser.in
requirements.operator-cookies.in
requirements.lock.txt
requirements.modal.browser.lock.txt
requirements.operator-cookies.lock.txt
trr_backend/socials/scrapling_transport.py
trr_backend/socials/socialblade/fetcher.py
trr_backend/socials/tiktok/posts_scrapling/fetcher.py
trr_backend/modal_jobs.py
tests/socials/test_instagram_comments_scrapling_contract.py
tests/socials/test_scrapling_transport.py
tests/socials/test_socialblade_fetcher.py
tests/socials/tiktok/posts_scrapling/test_fetcher.py
tests/test_modal_jobs.py
docs/workspace/scrapling-social-jobs.md
api/routers/socials/instagram_scrape.py
tests/ingestion/test_public_transport.py
tests/socials/instagram/posts_scrapling/test_fetcher.py
tests/socials/instagram/runtimes/test_scrapling_runtime.py
tests/socials/instagram/test_instagram_public_mode.py
trr_backend/socials/instagram/catalog_ingest.py
trr_backend/socials/instagram/comments_scrapling/fetcher.py
trr_backend/socials/instagram/posts_scrapling/fetcher.py
trr_backend/socials/instagram/posts_scrapling/job_runner.py
trr_backend/socials/instagram/public_probe.py
trr_backend/socials/instagram/runtimes/scrapling_runtime.py
trr_backend/socials/instagram/scraper.py
trr_backend/socials/social_season_analytics_impl.py
scripts/modal/evaluate_fetcher_session.py
tests/scripts/test_evaluate_fetcher_session.py
docs/deploy/2026-10-19-instagram-public-scrapling05.md
tests/socials/instagram/test_instagram_public_probe_script.py
tests/socials/instagram/comments_scrapling/test_fetcher_status_only.py
tests/socials/instagram/posts_scrapling/test_job_runner.py
```
