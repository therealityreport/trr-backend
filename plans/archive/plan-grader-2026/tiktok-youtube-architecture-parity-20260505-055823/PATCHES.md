# Patches

No source code patch was applied by Plan Grader. The required patch is a plan replacement: execute `REVISED_PLAN.md` instead of the source plan.

## Required Plan Patches

1. Add Phase 0 test-first grounding.
   - Source issue: the original validation section named commands but did not state enough expected pre-fix failures.
   - Revised section: `Phase 0: Current-State Tests First`.

2. Clarify the YouTube Module boundary.
   - Source issue: the original plan proposed `trr_backend/socials/youtube/posts_catalog/{fetcher.py,persistence.py,job_runner.py}`. `job_runner.py` implied a new stage or worker lane that the plan explicitly says not to create.
   - Revised section: `Phase 2: Extract YouTube Posts Catalog Module`.
   - New rule: no `youtube/jobs.py`, no new handler registration, and no new worker lane unless re-planned.

3. Make TikTok auth probe implementation testable.
   - Source issue: the original plan correctly found the unsupported TikTok probe but did not name exact expected safe payload fields.
   - Revised section: `Phase 1`, task 6.

4. Gate batch upsert.
   - Source issue: the original plan could let an executor pursue batch upsert too early.
   - Revised section: `Phase 3`, task 2.
   - New rule: batch upsert is optional and only allowed after contract-equivalence tests.

5. Improve execution handoff.
   - Source issue: the original plan recommended sequential `orchestrate-plan-execution`.
   - Revised section: `Recommended Handoff`.
   - New handoff: `orchestrate-subagents` with disjoint TikTok and YouTube workstreams, plus main-session integration.

6. Add archive and cleanup requirements.
   - Source issue: missing Plan Grader archive and cleanup sections.
   - Revised sections: `Archive Plan`, `Cleanup Note`.

## Requirement Trace

| Requirement | Revised Plan Location |
| --- | --- |
| Improve TikTok architecture | Phase 1 |
| Improve YouTube architecture | Phase 2 |
| Apply Instagram fixes where relevant | Current Repo Evidence, Phase 1 |
| Fix TikTok-specific bugs/errors | Phase 0, Phase 1 |
| Fix YouTube-specific bugs/errors | Phase 0, Phase 2 |
| Do not add comments lane | Non-Goals, Stop Rules |
| Keep current contracts unless re-planned | Non-Goals, Stop Rules |
| Full Plan Grader artifact package | This package |
