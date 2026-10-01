# Validation

## Files Inspected

- `/Users/thomashulihan/.codex/plugins/cache/local-plugins/plan-grader/1.0.0/SKILL.md`
- `/Users/thomashulihan/.codex/plugins/cache/local-plugins/plan-grader/1.0.0/skills/revise-plan/SKILL.md`
- `/Users/thomashulihan/.codex/skills/write-plan/SKILL.md`
- `/Users/thomashulihan/.codex/skills/orchestrate-subagents/SKILL.md`
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/TRR Backend Brain/BRAIN.md`
- `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md`
- `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/SUGGESTIONS.md`
- `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/AUDIT.md`
- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- `trr_backend/socials/instagram/runtimes/dispatcher.py`
- `trr_backend/socials/instagram/auth_resolver.py`
- `trr_backend/db/pg.py`
- `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`

## Commands Run

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git branch --show-current
```

Result: `chore/backend-batch-2026-04-28`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git status --short
```

Result: untracked `.plan-grader/` and `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
date +%Y%m%d-%H%M%S
```

Result: `20260428-165102`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "def fetch_one|conn=" trr_backend/db/pg.py trr_backend/socials/instagram/comments_scrapling/job_runner.py trr_backend/socials/instagram/posts_scrapling/job_runner.py
```

Result: confirmed `pg.fetch_one` exists and accepts `conn`; comments runner already has a `persist_conn` usage surface.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python -m json.tool .plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102/result.json >/dev/null
```

Result: valid JSON.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
find .plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102 -maxdepth 1 -type f -print | sort
```

Result: package contains `AUDIT.md`, `COMPARISON.md`, `PATCHES.md`, `REVISED_PLAN.md`, `SCORECARD.md`, `SUGGESTIONS.md`, `VALIDATION.md`, and `result.json`.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "^## (summary|project_context|assumptions|goals|non_goals|phased_implementation|ADDITIONAL SUGGESTIONS|architecture_impact|data_or_api_impact|ux_admin_ops_considerations|validation_plan|acceptance_criteria|risks_edge_cases_open_questions|follow_up_improvements|recommended_next_step_after_approval|ready_for_execution|Cleanup Note)$" .plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102/REVISED_PLAN.md
```

Result: all required schema headings are present.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "Suggestion [0-9]+ -" .plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102/REVISED_PLAN.md
```

Result: all ten source suggestions are represented as detailed tasks.

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
rg -n "superpowers:|subagent-driven-development|executing-plans|TBD|TODO|implement later|fill in details|Similar to Task|\\.\\.\\.|locals\\(\\)" .plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102/REVISED_PLAN.md
```

Result: no stale execution-skill wording, placeholders, ellipses, or rejected local-variable inspection snippets in `REVISED_PLAN.md`.

## Evidence Gaps

- No implementation tests were run because this was a planning-artifact revision, not a code mutation pass.
- No live Instagram smoke was run; the revised plan keeps live smoke manual-only.
- The current branch is not `main`, so execution via `orchestrate-subagents` is conditional.

## Assumptions

- The prior package score estimate `91` is the baseline for this revision.
- "Incorporate the suggestions" means every numbered source suggestion is selected.
- The new schema is the user-global `write-plan` contract currently installed at `/Users/thomashulihan/.codex/skills/write-plan/SKILL.md`.
