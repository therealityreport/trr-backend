# TikTok path reprioritization

Last updated: 2026-04-11

## Correction — 2026-04-11

The 2026-04-10 closeout status was premature. The `yt-dlp`-first pivot work described here existed in local working-tree changes but was not committed to `origin/main` on 2026-04-10.

Actual backend branch commit SHAs are:

- `571e4bc` — `feat(tiktok): make ytdlp the primary scraper path`
- `7d2bb90` — `fix(tiktok): force shared-account fallback onto ytdlp`
- `28bcca1` — `feat(tiktok): surface scraper diagnostics in cli and api`

Canonical on-main behavior through 2026-04-10 remained `TikTokScrapeConfig.scrape_mode="api"` in `trr_backend/socials/tiktok/scraper.py`.

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-11
  current_phase: "runtime drift diagnosed, ytdlp-first pivot committed on branch, bravowwhl validated past four digits"
  next_action: "build and deploy a fresh Modal social image from main after PR merge, then drain mixed-runtime workers"
  detail: self
```

## What changed

- `TikTokScrapeConfig.scrape_mode` now defaults to `ytdlp`.
- `scrape_mode=auto` is treated as a deprecated alias to `ytdlp` instead of a hidden direct-path cascade.
- The direct TikTok HTTP client is lazy; default `yt-dlp` runs no longer build transport objects or touch proxy config.
- First-class `yt-dlp` diagnostics now report `retrieval_mode=ytdlp`, `http_client=yt_dlp`, `fallback_chain=["yt_dlp"]`, cookie presence and usage, and `profile_enrichment_status=skipped`.
- Shared-account TikTok posts now force `scrape_mode="ytdlp"` and bypass the partitioned direct API route at `_scrape_shared_tiktok_posts(...)`.
- TikTok scrape diagnostics are now surfaced safely through both `/tiktok/scrape` and `scripts/socials/tiktok/scrape.py`.

## Parked work

- Direct TikTok post API scraping remains opt-in only.
- Bright Data proxy investigation is documented as known operational debt and is off the critical path while `yt-dlp` remains healthy.

## Runtime Drift Diagnosis

- Modal social images are built from repo source at image-build time via `trr_backend/modal_jobs.py`:
  - `pip_install_from_requirements(...)`
  - `pip_install("yt-dlp")`
  - `add_local_python_source("api", "trr_backend")`
  - `add_local_dir(..., remote_path="/root/scripts/socials/tiktok")`
- The live `@bravowwhl` run `b38c33b5-fffe-43a6-b8ab-5926770bcd43` observed two runtime labels in the same run:
  - `modal`
  - `modal:main · im-sMysH7ppTegIK6j75iXci9`
- Discovery ran on the unlabeled `modal` worker; the single-runner fallback posts job ran on the labeled `main` image. That mixed labeling is the concrete cause of the `Runtime Version Drift` warning.
- Production is not pulling a pinned wheel. It is executing code baked into the Modal image built from local repo source at deploy time.
- A PR to `main` is necessary but not sufficient for cutover. A fresh Modal image must be built from the merged branch and old mixed-runtime workers should be drained.

## Procurement guidance

- Do not buy more residential or ISP proxy credits on current evidence.
- Let the Bright Data trial expire.
- Revisit only if `yt-dlp` stops working and a new concrete hypothesis emerges.

## Validation snapshot

- Targeted scraper tests passed:
  - `pytest -q tests/socials/tiktok/test_scraper.py` -> `9 passed`
- Targeted CLI + route diagnostics tests passed:
  - `pytest -q tests/scripts/test_tiktok_scrape_cli.py tests/api/routers/test_socials_tiktok_scrape.py` -> `2 passed`
- Targeted repository tests passed:
  - `pytest -q tests/repositories/test_social_season_analytics.py -k "single_runner_fallback or runtime_version or tiktok_empty_body_transport_failure"` -> `5 passed`
- Touched-file lint passed:
  - `ruff check` on the modified TikTok files -> clean
  - `ruff format --check` on the modified TikTok files -> clean
- Repo-wide baseline remains dirty:
  - `ruff check .` fails on unrelated pre-existing admin / Instagram / script files outside this TikTok change
  - `ruff format --check .` would reformat many unrelated files
  - `pytest -q` did not reach test execution within 171.59s and was interrupted during collection
- Local `@bravowwhl` dry-run, `ytdlp` mode:
  - clean-branch code enumerated `1800` posts in `104.7s`
  - `retrieval_mode="ytdlp"`, `stop_reason="max_posts_reached"`, `risk_state="healthy"`
- Live Modal `@bravowwhl` run, `b38c33b5-fffe-43a6-b8ab-5926770bcd43`:
  - posts stage advanced from the earlier `400` snapshot to `3277 / 3277`
  - latest recent log line: `Tiktok @bravowwhl shared account scrape running · scraped 3277 · persist catalog posts · 0pg · 3277chk · 3277match`
