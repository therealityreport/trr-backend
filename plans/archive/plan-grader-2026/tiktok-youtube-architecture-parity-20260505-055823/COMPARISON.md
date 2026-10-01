# Comparison

## Source Plan vs Revised Plan

| Area | Source Plan | Revised Plan | Impact |
| --- | --- | --- | --- |
| Score | 81.20 | 90.92 estimated | Moves from "good but tighten" to execution-ready. |
| Test strategy | Named useful commands | Adds pre-fix expected failing tests | Reduces executor guesswork. |
| YouTube extraction | Suggested `posts_catalog/{fetcher,persistence,job_runner}` | Uses `posts_catalog/catalog.py` and explicitly blocks new worker lane | Prevents accidental stage/handler invention. |
| TikTok auth probe | Correctly identifies unsupported TikTok probe | Adds safe payload fields and script/test expectations | More directly executable. |
| Batch upsert | Included as parity goal | Makes it optional after contract equivalence | Reduces data-integrity risk. |
| Handoff | Sequential | Parallel workstreams after Phase 0 | Faster without overlapping file ownership. |
| Archive/cleanup | Missing | Added exact required sections | Meets Plan Grader contract. |

## Reasons For Delta

The source plan already had strong repo awareness and clear scope. The revised plan mainly improves:

- agent safety,
- measurable outcomes,
- exact test-first proof,
- handoff routing,
- contract boundaries around YouTube and batch persistence.

## Recommended Source Of Truth

Use `REVISED_PLAN.md` as the execution source. Keep the original plan only as source evidence.
