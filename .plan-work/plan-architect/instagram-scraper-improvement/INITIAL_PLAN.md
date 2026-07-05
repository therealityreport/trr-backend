# INITIAL_PLAN.md

## Objective

Build the Instagram scraper improvement program described in `docs/codex/prds/instagram-scraper-improvement.md`: complete TRR-native Instagram post snapshots, account-level benchmark evidence, explicit retry/completeness tracking, adaptive Supabase/Instagram-safe speed control, and Modal-ready rollout validation.

## Current Evidence

- The PRD exists at `docs/codex/prds/instagram-scraper-improvement.md` and is published as GitHub issue `therealityreport/trr-backend#149` with `ready-for-agent`.
- The backend glossary defines Complete Instagram Post Snapshot, Instagram Backfill Runtime Target, Adaptive Instagram Scrape Speed, Lane-Specific Scrape Backoff, Partial Success with Retry Queue, Source-Unavailable Evidence, Adaptive Scrape Control Plane, Lane Budget, Hosted Snapshot Completion, and Mega-Post Shard.
- ADR `0001-adaptive-instagram-scrape-control-plane.md` accepts a shared control plane with lane-specific enforcement.
- Existing seams include posts and comments Scrapling lanes, backfill health, queue status, media queue guard, benchmark helpers, Modal readiness checks, and Instagram comment persistence.

## Initial Build Slices

1. Baseline benchmark and gap report for `bravotv`.
2. Snapshot completeness and retry-target model.
3. Adaptive scrape control-plane budgets.
4. Lane-specific enforcement for comments, posts, media mirror, and DB writes.
5. Mega-post sharding and cursor/checkpoint recovery.
6. Hosted media, comment media, and avatar completion.
7. Operator/API reporting and Modal validation.

## Initial Validation Strategy

- Prefer fixture-backed tests for deterministic parser, budget, completion, and retry behavior.
- Keep live Instagram/Supabase tests bounded, explicit, and operator-triggered.
- Validate runtime changes locally first and send Modal-affecting scraper/job/runtime changes to Modal unless a task is explicitly local-only.
