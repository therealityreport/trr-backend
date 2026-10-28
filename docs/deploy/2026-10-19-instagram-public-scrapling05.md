# Instagram public isolation and Scrapling 0.5 release record

Status: deployed v80 from 60cc311716881a7516f831d1ca763d55a9880733. Source and deployment verified; full acceptance remains blocked by Threads credentials and Instagram detail/pagination access. Hygiene changes remain uncommitted.

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

Pre-deploy production was v78, deployed 2026-10-16 with tag `ig-public-profile-20261017-690e08c1` and source label `690e08c*`. The asterisk denotes uncommitted source: that Git commit alone cannot reproduce the deployed artifact. Preserve the provider's v78 deployment as rollback evidence. The previous 0.4.12 locks remain recoverable from the base revision.

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

## Deployment acceptance procedure and limits

After approval, commit only the final task manifest, then deploy through the wrapper with `main` explicitly selected. Reuse passing source evidence. Verify deployed browser package versions and browser launch, the SocialBlade runtime probe, Threads remote-auth smoke, core workers, and a bounded public Instagram probe. Use `verify_modal_readiness.py --env main --probe-core-workers --strict-probes`; add `--probe-remote-auth threads` for the Threads smoke. Keep all commands pinned to `MODAL_PROFILE=admin-56995` and `MODAL_ENVIRONMENT=main`.

A source redirect or challenge is a blocked public acceptance result, not proof of successful scraping. Do not label fixture checks as deployed acceptance. Record the final revision, deployment version, readiness/smoke outcomes, dispatch target, and remaining uncommitted paths with image-boundary classification here before closeout.

TRR-APP build: not applicable (backend scraper/runtime change without an app contract change). SQL ownership: unchanged.

## Remaining dirty paths outside this release

The following 53 existing paths are outside the proposed task commit and outside the Modal image upload/dependency paths. Classification uses path names only; their changes were not reviewed. The image builders upload `api`, `trr_backend`, the listed social script folders/files and `scripts/sync`; dependency layers consume their explicit lock files. After the approved source commit, these same 53 paths remain uncommitted and outside the image; their contents remain unreviewed.

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

<!-- modal-deploy-history:start -->
## Deploy History Stamp

- Last stamped: `2026-10-19T16:30:05-04:00`
- Workspace: `admin-56995`
- Profile: `admin-56995`
- Canary: `https://admin-56995--trr-backend-api.modal.run/health` HTTP `200` on attempt `1`

| Version | Deployed At | Deployed By | Commit | Client |
| --- | --- | --- | --- | --- |
| v80 | 2026-10-19 16:29:39-04:00 | admin-56995 | 60cc311* | 1.5.5 |
| v79 | 2026-10-19 16:26:06-04:00 | admin-56995 | a662c2e* | 1.5.5 |
| v78 | 2026-10-16 20:53:20-04:00 | admin-56995 | 690e08c* | 1.5.5 |
| v77 | 2026-10-16 20:26:38-04:00 | admin-56995 | 7f35ce5* | 1.5.5 |
| v76 | 2026-10-16 20:26:32-04:00 | admin-56995 | 7f35ce5* | 1.5.5 |

<!-- modal-deploy-history:end -->

## Deployed checks and bounded-probe repair

Initial deployment: v79, 2026-10-19 16:26 EDT, tag `ig-public-scrapling05-a662c2e0`. Modal labels source `a662c2e*` because the 53 excluded hygiene paths remain dirty; image inputs match committed source. No global profile was activated.

- Wrapper readiness/API cold-start canary passed: HTTP 200 on first attempt.
- Admin operations, Google News, Reddit refresh, admin vision, SocialBlade and browser image runtime probes all healthy. Browser launch succeeded. Deployed versions: Scrapling 0.5, curl-cffi 0.16.1, Playwright 1.62.0, Patchright 1.62.1, apify-fingerprint-datapoints 0.15.0; Chromium 151.0.7922.34.
- Control-plane provider probe ready; all required functions/endpoints present. Dispatch helper pins admin-56995/main/trr-backend-jobs and rejects conflicting targets.
- Threads remote-auth smoke failed: missing csrftoken and sessionid. Strict readiness exits 1. No cookie repair, login or proxy change was attempted.
- Public Instagram recovered 33 unique @bravotv listing records and five sampled comments for `DeHyoOPNOiX`. Listing fields include shortcode, media ID/type, publication time, and like/comment counts. No media download or complete-comment-thread claim. Proof reports auth=public, proxy=none, Decodo=none.
- Zero detail extractions succeeded. Page two returned HTTP 401: `public_graphql_401_backoff_required`, recommending 3600 seconds. No follow-up Instagram requests were made.

Representative live listing records:

| Shortcode | Published UTC | Media type | Likes | Comments advertised |
| --- | --- | --- | --- | --- |
| DesEWRWl0Ix | 2026-10-19 20:01:12 | carousel | 537 | 7 |
| Der2j7hlvnd | 2026-10-19 18:01:25 | video | 1601 | 70 |
| DeHyoOPNOiX | 2026-10-05 17:59:32 | video | 74033 | 2140 |

The probe requested max-pages=2, one sampled detail and one sampled comment post per page. Deployed evidence exposed pre-existing code that ignored max-pages and selected every detail on the first three pages; it attempted 33 details. The run stopped on page two's 401, but did not prove its configured bounds. The repair enforces positive page limits per invocation, preserves the resume cursor and applies detail sampling on every page. Zero remains uncapped; explicit all mode remains available. All 15 owner tests passed, including cap=1/2 and resume regressions. Independent review of only the two changed files found no findings; subsequent test formatting was nonsemantic.

Repair edits and this record remain within the approved 35 paths. Browser/dependency/worker evidence is reused because the repair does not change those inputs. Instagram cooldown and Threads credentials remain acceptance limits.

## Final deployment evidence

- Current provider deployment: **v80**, 2026-10-19 16:29 EDT, app `ap-DkLTRoSvqhbkGO7fHyHxrD`, workspace `admin-56995`, environment `main`, tag `ig-public-probe-bounds-60cc3117`.
- Runtime source revision: `60cc311716881a7516f831d1ca763d55a9880733`. Provider label `60cc311*` reflects only the excluded 53 hygiene paths at deployment time. The original 35-path implementation is commit `a662c2e02dfcac891cfc08c8b49d3b4a424be27b`; the repair touches two of those paths plus this record.
- Repair wrapper readiness and API health passed (HTTP 200, first attempt). Earlier unchanged browser/dependency/core-worker/SocialBlade checks are retained. Threads strict readiness remains blocked by missing cookies; no success is claimed for that gate.
- In deployed container `ta-01M5AXP7ZDFH4HH73Z1TNZ9T2R`, an offline fixture smoke verified source SHA-256 `80b734b5d404348d0a13bdcb0a05476ee038d6f669b2a6c1fdb2c9081952c72f`. Page caps 1 and 2 produced exactly 1 and 2 listing, detail, and comment fixture calls respectively, preserving the cursor and returning `page_cap_reached`. It made zero external source requests and changed no app configuration or shared state.
- No Instagram requests were repeated after the reported 401/backoff. Live evidence remains the v79 retrieval above; v80 changes only probe limits. This does not prove permalink-detail extraction or complete pagination.
- Rollback v78 remains in provider history. No rollback was needed for the repair; deployment recovery can target the preserved provider artifact.
- The final documentation-only evidence commit does not change deployed runtime inputs. No push, PR or branch change was performed.

Remaining acceptance work: supply authorized valid Threads cookies before rerunning its auth smoke; investigate public permalink metadata availability and repeat a bounded Instagram probe only after the reported backoff. FetcherSession comparison remains inconclusive and production adoption remains out of scope.

## Local detail and pagination investigation (after v80)

User narrowed the follow-up to local work first. No live Instagram request, deployment, cookie/auth repair or proxy change occurred in this phase. The 53 hygiene paths remain excluded.

Detail means the individual post's public metadata: caption/hashtags/mentions, media URLs and carousel items, thumbnail, time/type, counts where exposed, owner and tagged/collaborating accounts. The bounded probe reports only success, media type/URL count, caption/thumbnail presence and timestamp; comments are a separate extractor. It does not download media.

Evidence and fixes:

- All 33 deployed detail attempts returned generic metadata_not_found. Since that path only returns this result after a successful HTTP 200 body, these were parser misses or public page shells, not established HTTP 401 detail failures. The earlier proof retained neither status/body-size fields nor response bodies, so the exact live cause cannot be recovered retrospectively.
- The legacy detail helper only recognizes specific data-sjs roots. A local fixture with xdt_shortcode_media in an application/json script is missed there but parsed by the existing public-post extractor. The probe now reuses that extractor on the same already-fetched HTML when the legacy parser finds nothing; it performs no extra request and requires the requested shortcode.
- Detail diagnostics now retain HTTP status and body byte count, classify empty/login/challenge pages when no metadata is found, and avoid alternate-route retries on HTTP rejection. Raw HTML, cookies and tokens are not retained in proof output. Caption presence now checks caption text rather than a nonempty caption object.
- Strict-public cursor requests previously forced a fresh profile context before each page. They now reuse existing public context, reducing extra requests and nonce churn. This is a local reliability fix, not proof that Instagram will allow pagination.
- Strict public GraphQL stops on the first 401/403/429 instead of cycling document IDs or retrying the public rejection. Successful GraphQL results clear stale errors from earlier warmups/attempts; the prior first-page proof showed 403 alongside recovered posts, illustrating ambiguous old diagnostics.
- Focused local checks: 39 passed. Additional existing GraphQL/parser checks: 44 passed, one failed. The same failure was reproduced against unchanged HEAD in a disposable pytest process: an existing test expects a redirect URL scheme that the maintained request-client sanitizer removes. No test guard was bypassed and that unrelated expectation was not edited.

Live result remains 33 listing records and five sampled comments from v79. Successful live detail extraction and pagination are still unproven. A new bounded source check can follow after the documented cooldown when the local-first phase is accepted; it must preserve no-auth/no-proxy and stop conditions.

Independent review of this four-file local repair completed with no findings; all reviewed source/test hashes stayed unchanged. The scoped local commit does not update production v80.
