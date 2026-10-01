# TRR Backend Architecture Improvement Plan

Date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Ground Rules

This plan is based on the live backend tree, `AGENTS.md`, `CLAUDE.md`, `README.md`, and current backend architecture docs. I did not use saved brain notes or old handoffs as authority.

The goal is not to make the code smaller for its own sake. The goal is to deepen the modules that operators and developers keep touching: put more behavior behind smaller interfaces, improve locality for fixes, and make tests cross the same seams production callers use.

Do not start by introducing abstract interfaces. Use the deletion test first. If deleting a module only moves its complexity into every caller, it is earning its keep. If deleting it removes complexity, it is probably shallow.

## Current Shape

The backend is a Supabase-first FastAPI app plus shared ingestion and worker code. The documented rule is that reusable code should live under `trr_backend/`, while `api/` should expose request routes.

The live tree already has a social-control-plane migration in progress:

- `trr_backend/socials/social_season_analytics_impl.py` is now the canonical implementation file and is about 55k lines.
- `trr_backend/repositories/social_season_analytics.py` is a compatibility alias to that implementation.
- New social packages exist under `trr_backend/socials/control_plane/`, `trr_backend/socials/pipelines/`, `trr_backend/socials/read_models/`, `trr_backend/socials/account_catalog/`, and `api/routers/socials/`.
- Many new modules still import `_core` or `legacy` from `social_season_analytics_impl`, so the extraction has created import surfaces but not yet a fully deep module shape.

There are also very large admin route modules:

- `api/routers/admin_person_images.py`: about 17k lines.
- `api/routers/socials/__init__.py`: about 8.7k lines.
- `api/routers/admin_show_links.py`: about 8.2k lines.
- `api/routers/admin_show_sync.py`: about 5.8k lines.

Those modules mix schemas, route handlers, persistence, scraping policy, queue behavior, progress/SSE formatting, and source-specific normalization.

## Candidate 1: Deepen the Social Control Plane

### Files

- `trr_backend/socials/social_season_analytics_impl.py`
- `trr_backend/repositories/social_season_analytics.py`
- `trr_backend/socials/control_plane/*.py`
- `trr_backend/socials/pipelines/**/*.py`
- `trr_backend/socials/read_models/**/*.py`
- `trr_backend/socials/account_catalog/*.py`
- `api/routers/socials/__init__.py`
- `tests/repositories/test_social_control_plane_imports.py`

### Problem

The control-plane extraction has improved names, but most modules still depend on the monolith as the private dependency provider. The current seam is therefore shallow: callers can import nicer paths, but the interface still includes too much of the old implementation's private vocabulary, constants, DB helpers, and error behavior.

The deletion test fails for several new modules today. Deleting wrappers like `trr_backend/socials/account_catalog/catalog_launch.py` or `trr_backend/socials/read_models/account_profile/instagram.py` would often just expose the old `_core` call again rather than move real behavior behind a smaller interface.

### Solution

Turn the control plane into a small set of deep modules organized by backend domain language:

- Run lifecycle: start, cancel, retry, progress, summarize, reconcile.
- Worker health: queue state, worker leases, stale recovery, health diagnostics.
- Account catalog: launch, progress, review queue, freshness, profile reads.
- Season ingest: target resolution, schedule preview, orchestration.
- Analytics read models: week detail, coverage, profile dashboard, exports.

For each module, move the behavior it owns out of `social_season_analytics_impl.py` until callers no longer need private helpers from the monolith. Keep the compatibility alias temporarily, but make it import from the new owner modules instead of being the behavior owner.

### Benefits

Locality improves because queue failures, account catalog drift, analytics response shape, and worker recovery stop sharing one implementation file. A future Instagram catalog fix should not require understanding season analytics export logic.

Leverage improves because route handlers and worker jobs can call a small domain module instead of coordinating internal `_core` helpers. Tests can assert behavior at the run lifecycle, worker health, and account catalog seams rather than pinning private helper identity back to the monolith.

## Candidate 2: Turn `api/routers/socials` Into Route Facades

### Files

- `api/routers/socials/__init__.py`
- `api/routers/socials/catalog.py`
- `api/routers/socials/profiles.py`
- `api/routers/socials/reddit.py`
- `api/routers/socials/season_ingest.py`
- `api/routers/socials/analytics.py`
- `api/routers/socials/legacy_scrape.py`
- `api/routers/socials/worker_health.py`
- `trr_backend/socials/api/handlers/*.py`
- `trr_backend/socials/api/schemas/*.py`

### Problem

The route package has been split into small files, but the package root still owns almost all of the behavior. The small route files are shallow because their interface is mostly "re-export or include the old giant router." The package root also mixes route schemas, cache state, background-thread behavior, route execution decisions, Reddit dispatch, account profile dashboard reads, and legacy platform scraping.

This makes route changes risky: a profile-dashboard request budget fix, an inline worker fallback change, and a Reddit refresh route can all touch the same module-level caches and helper functions.

### Solution

Make the route modules real facades:

- Move request and response models into narrow `trr_backend/socials/api/schemas/` modules.
- Move route handler orchestration into `trr_backend/socials/api/handlers/` modules by surface.
- Keep `api/routers/socials/*.py` responsible for FastAPI path declarations, dependency injection, request parsing, and converting known domain errors to HTTP responses.
- Keep route-cache behavior next to the read model or handler that owns the freshness contract.

### Benefits

Locality improves because a profile dashboard budget bug lives near profile dashboard code, not beside season sync and legacy scrape endpoints. Route tests become thinner and more stable: handler tests can cross the same handler seam without spinning up route module state, while route tests only verify path shape and HTTP translation.

Leverage improves because app-facing contracts can reuse schemas and handlers without copying route-level helper logic.

## Candidate 3: Deepen the Person Images Refresh Module

### Files

- `api/routers/admin_person_images.py`
- `trr_backend/services/person_images/*.py`
- `trr_backend/media/face_crops.py`
- `trr_backend/media/image_variants.py`
- `trr_backend/media/getty_replacement.py`
- `trr_backend/repositories/person_images.py`
- `trr_backend/repositories/media_assets.py`
- `tests/api/routers/test_admin_person_images*.py`

### Problem

`admin_person_images.py` contains the whole person-gallery refresh world: request schemas, source policy, Getty/NBCUMV import, BravoTV gallery import, IMDb metadata repair, TMDb/Fandom profile refresh, face detection, face crop assignment, text overlay detection, gallery mirroring, S3 pruning, SSE streaming, and operation producers.

This module is not just large; its interface is broad. A caller or test often needs to know source names, batch sizing env vars, progress payload shape, show lookup behavior, retry behavior, and streaming details. That is a shallow module with poor locality.

### Solution

Create a deep person images refresh module in `trr_backend/services/person_images/` that owns the refresh plan and source-stage execution. Keep the admin route as a facade for:

- validating request payloads,
- resolving the authenticated admin context,
- starting sync or stream execution,
- serializing the final response.

Move source-specific implementations behind source-stage modules:

- Getty/NBCUMV source ingestion.
- BravoTV gallery ingestion.
- IMDb media repair and episode/show context repair.
- TMDb/Fandom profile refresh.
- Face detection, crop, and assignment.
- Media mirroring and variant resizing.

### Benefits

Locality improves because source-specific fixes stop requiring edits in a 17k-line route file. Leverage improves because sync and stream refresh paths can share one refresh-plan module, rather than duplicating progress and stage behavior. Tests can move from route-helper tests to source-stage tests and a small number of route contract tests.

## Candidate 4: Deepen Show Link Discovery

### Files

- `api/routers/admin_show_links.py`
- `trr_backend/ingestion/fandom_*`
- `trr_backend/integrations/*`
- `trr_backend/repositories/media_links.py`
- `tests/api/routers/test_admin_show_links.py`

### Problem

Show link discovery currently owns URL normalization, Fandom candidate search, Wikidata/Wikipedia resolution, person source validation, social handle expansion, cleanup scans, SSE progress, and CRUD routes in one router module. This makes the interface too complex: callers need to understand discovery-stage budgets, Fandom allowlist policy, validation modes, and persistence side effects.

The module has real behavior worth preserving, but the seam is in the wrong place. Route-level helpers are carrying source-discovery policy that should be shared by stream, sync, cleanup, and route paths.

### Solution

Move the discovery behavior into a deep show link discovery module under `trr_backend/services/show_links/` or `trr_backend/ingestion/show_links/`. Keep the route module as the HTTP facade and split the domain behavior into:

- candidate discovery,
- source validation,
- link classification,
- cleanup and promotion,
- progress event production,
- persistence.

### Benefits

Locality improves because Fandom scoring changes no longer sit beside CRUD routes. Leverage improves because stream and non-stream discovery can share the same discovery run implementation. Tests can exercise a discovery run through one interface instead of separately pinning dozens of route-private helpers.

## Candidate 5: Deepen Show Refresh and Networks/Streaming Sync

### Files

- `api/routers/admin_show_sync.py`
- `trr_backend/pipeline/show_refresh_orchestrator.py`
- `trr_backend/pipeline/orchestrator.py`
- `trr_backend/pipeline/stages/*.py`
- `trr_backend/repositories/admin_operations.py`
- `trr_backend/repositories/admin_show_reads.py`
- `tests/api/routers/test_admin_show_sync*.py`
- `tests/pipeline/*.py`

### Problem

`admin_show_sync.py` owns multiple domains: list sync, networks/streaming sync, logo imports, show refresh, refresh streaming, cast person refresh phases, operation producers, and retry endpoints. Some of this overlaps with the documented `trr_backend/pipeline/` orchestrator, but the route file still contains a lot of orchestration implementation.

That creates low locality for show-refresh behavior. A backend change to media ingestion, social setup, or cast profile refresh may require edits across the route file and pipeline modules.

### Solution

Promote show refresh into a deep module that owns refresh targets, phase execution, cancellation, progress events, and retry planning. Let `admin_show_sync.py` delegate to that module and keep only HTTP translation and request/response schemas. Align the refresh module with the existing pipeline orchestrator instead of creating a second orchestration vocabulary.

### Benefits

Locality improves because refresh target logic and retry behavior sit with the refresh module. Leverage improves because sync, stream, retry, and operation producer paths all use one refresh implementation. Tests can cover the refresh target graph directly, while route tests verify HTTP shape.

## Suggested Order

1. Finish the social control plane deepening first.

This is already in motion and has the highest blast radius. The current alias/import tests show the intended ownership map, but the implementation still leaks through `_core` imports. Completing this first reduces friction for social route and scraper work.

2. Convert `api/routers/socials` into route facades.

This depends on the social control plane having real owner modules. Once those seams are deeper, the route package can become thin without inventing new behavior.

3. Deepen person images refresh.

This is the largest non-social router and has a clear route-vs-refresh-plan split. It also has source-stage names already visible in the code, which gives good module names.

4. Deepen show link discovery.

This is a strong candidate after person images because the discovery stages are source-policy heavy and currently route-private.

5. Deepen show refresh.

This should be done after social and media refresh because it likely crosses existing pipeline contracts and admin operation producers.

## Stop Rules

- Do not introduce a seam with only one adapter unless a second adapter is already required by tests, local vs remote execution, or runtime mode.
- Do not move code only to shrink file size. Move behavior only when callers can learn less and tests can cross a better interface.
- Do not remove compatibility imports until route tests, worker tests, and current app contracts are updated.
- Do not make app-facing contract changes without updating the workspace contract docs under `/Users/thomashulihan/Projects/TRR/docs/`.
- Do not treat current dirty worktree state as disposable; this plan assumes active in-progress refactors exist.

## First Grilling Target

The best first candidate to explore is Candidate 1: deepen the social control plane. It is already partially extracted, it affects the most active scraper/control-plane work, and it will make Candidate 2 much easier.

Which candidate should we grill first?
