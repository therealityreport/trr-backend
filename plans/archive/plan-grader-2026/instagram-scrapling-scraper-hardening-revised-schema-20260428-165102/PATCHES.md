# Patches

This package rewrites the prior `REVISED_PLAN.md` into a new schema-compliant execution plan.

## Structural Changes

| Prior plan section | New plan section | Change |
| --- | --- | --- |
| Goal / Architecture / Tech Stack header | `summary`, `project_context`, `goals`, `architecture_impact` | Converted to `write-plan` schema. |
| Non-Goals | `non_goals` | Preserved and clarified. |
| Success Signals | `acceptance_criteria` and phase acceptance criteria | Moved into schema and per-phase criteria. |
| Task 0 | Phase 0 - Orchestrator preflight | Added `orchestrate-subagents` branch and dirty-worktree stop rules. |
| Tasks 1-7 | Phases 1-7 | Preserved technical content, grouped by ownership scope and validation responsibility. |
| Recommended Subagent Split | `recommended_next_step_after_approval` | Replaced with canonical `orchestrate-subagents` execution contract. |
| Cleanup Note | Cleanup Note | Preserved exact required text. |

## Suggestions Incorporated

Every numbered source suggestion was selected and incorporated under `ADDITIONAL SUGGESTIONS`.

| Source suggestion | New task | Concrete integration |
| --- | --- | --- |
| 1. Add a one-page Scrapling lane architecture diagram | `Suggestion 1 - Add a one-page Scrapling lane architecture diagram` | Adds Mermaid architecture docs and validation grep. |
| 2. Add a static no-cookie-values metadata scanner | `Suggestion 2 - Add a static no-cookie-values metadata scanner` | Adds required scanner/test and cookie leak acceptance criteria. |
| 3. Add a local fake Instagram response fixture pack | `Suggestion 3 - Add a local fake Instagram response fixture pack` | Adds fixture folder and fixture-backed fetcher tests. |
| 4. Track retry reason counts in job metadata | `Suggestion 4 - Track retry reason counts in job metadata` | Adds runtime metadata counters and tests. |
| 5. Add worker restart note to local runbook | `Suggestion 5 - Add worker restart note to local runbook` | Adds docs requirement near cancellation validation. |
| 6. Split shared retry helpers into unit tests | `Suggestion 6 - Split shared retry helpers into unit tests` | Adds `tests/socials/test_scrapling_http_utils.py` to Phase 2 validation. |
| 7. Add a final smoke command for one page only | `Suggestion 7 - Add a final smoke command for one page only` | Adds manual-only smoke docs and final validation boundary. |
| 8. Add cancellation latency to worker logs | `Suggestion 8 - Add cancellation latency to worker logs` | Adds job-runner logging task tied to cancellation helper. |
| 9. Add a short glossary for runtime vs lane | `Suggestion 9 - Add a short glossary for runtime vs lane` | Adds runbook glossary task. |
| 10. Create a future plan for actually implementing `ScraplingRuntime` | `Suggestion 10 - Create a future plan for implementing ScraplingRuntime` | Adds a future plan stub requirement and validation. |

## Orchestrate-Subagents Patch

The previous plan's Superpowers execution instruction is replaced by the new execution contract:

```md
recommended_next_execution_skill: `orchestrate-subagents`
ready_for_execution: `conditional_on_branch_preflight`
```

and by the `recommended_next_step_after_approval` section, which requires:

- branch `main` or explicit user approval for a different branch;
- no branches or worktrees;
- disjoint ownership scopes;
- subagents warned not to revert concurrent edits;
- main-session integration and validation;
- final report fields from the `orchestrate-subagents` completion contract.
