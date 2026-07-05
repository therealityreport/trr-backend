# TOOLS.md

Tool Finder was run for: `TRR Instagram scraper complete snapshot adaptive speed Supabase-safe backfill implementation plan`.

Invocation:

```bash
python3 /Users/thomashulihan/.codex/plugins/cache/local-plugins/plan-grader/1.0.0/scripts/run_tool_finder_for_plan.py \
  "TRR Instagram scraper complete snapshot adaptive speed Supabase-safe backfill implementation plan" \
  --run-dir .plan-work/plan-architect/instagram-scraper-improvement \
  --root /Users/thomashulihan/Projects/TRR/TRR-Backend
```

Result JSON: `TOOL_FINDER_RESULTS.json`.

## Accepted Capabilities

| ID | Capability | Role In Plan | Decision |
| --- | --- | --- | --- |
| local:AGENTS.md | Backend repo instructions | Preserve repo startup, validation, and nested-policy rules. | Accepted |
| local:docs/social/instagram-data-contract.md | Instagram data contract | Source-family and privacy contract for snapshot fields. | Accepted |
| local:docs/workspace/instagram-posts-scrapling.md | Posts Scrapling runbook | Existing posts lane behavior, env, auth, routing, and failure modes. | Accepted |
| local:docs/workspace/instagram-comments-scrapling.md | Comments Scrapling runbook | Existing comments lane, Modal path, cursor resume, and worker controls. | Accepted |
| local:scripts/socials/instagram/benchmark_posts_backfill.py | Posts benchmark helper | Extend into account/date-window benchmark reporting. | Accepted |
| local:scripts/socials/instagram/benchmark_comments_shards.py | Comments benchmark helper | Extend for p95 comments timing and mega-post evidence. | Accepted |
| local:trr_backend/socials/control_plane/backfill_health.py | Backfill health read model | Existing Supabase/queue/auth/proxy signal aggregation seam. | Accepted |
| local:trr_backend/socials/control_plane/queue_status.py | Queue status read model | Existing active jobs, stale jobs, recent failures, and dispatch pressure seam. | Accepted |
| local:trr_backend/socials/instagram/comments_scrapling/persistence.py | Comment persistence | Existing author metadata, reply topology, and comment media mirror fields. | Accepted |
| local:scripts/socials/media_queue_guard.py | Media queue guard | Existing media stale-claim safety pattern for media lane backoff. | Accepted |
| local:scripts/modal/verify_modal_readiness.py | Modal readiness | Existing production follow-through and remote auth validation seam. | Accepted |

## Rejected Or Deferred Candidates

| Candidate | Reason |
| --- | --- |
| PyPI `Instagram` | Rejected. PRD and runbooks require TRR-native lanes; third-party Instagram packages are not a replacement path. |
| PyPI `scraper`, `complete`, `snapshot` | Rejected. Generic or irrelevant package matches; no concrete role. |
| Homebrew packages such as `pcl`, `abcl`, `abpoa` | Rejected. Irrelevant provider matches. |
| GitHub external code results | Deferred. GitHub repository/code providers returned no useful implementation candidates for this project-specific plan. |

## Provider Notes

- Local inventory produced the useful results.
- Runtime catalog and skills.sh providers were skipped due missing runtime catalog/OIDC token.
- npm provider errored; no npm capability is needed for this backend-first plan.
