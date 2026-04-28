# Comparison

## Old vs New Plan

| Topic | Prior revised plan | New revised plan |
| --- | --- | --- |
| Schema | Custom task list | `write-plan-v1` sections plus phase-level criteria. |
| Execution skill | Superpowers execution language plus subagent split | Canonical `orchestrate-subagents` handoff. |
| Branch policy | Not explicit | Stops before mutation unless branch is `main` or user approves otherwise. |
| Suggestions | Optional in `SUGGESTIONS.md` | All ten incorporated as required tasks under `ADDITIONAL SUGGESTIONS`. |
| Ownership scopes | Brief subagent split | Full ownership, out-of-scope, dependencies, validation, and commit boundaries. |
| Validation | Focused tests, Ruff, compile, metadata leak tests | Adds helper tests, fixture tests, metadata scanner, docs greps, and manual smoke boundary. |
| Cleanup | Present | Preserved exactly. |

## Score Delta

| Topic | Prior score | New score | Delta | Reason |
| --- | ---: | ---: | ---: | --- |
| Current-state fit | 9 | 9 | 0 | Same repo evidence. |
| Plan schema compliance | 6 | 10 | +4 | New plan follows `write-plan` completion contract. |
| Suggestion handling | 4 | 10 | +6 | Every numbered suggestion is now a concrete required task. |
| Execution orchestration | 7 | 10 | +3 | Uses `orchestrate-subagents` with branch, ownership, prompt, and completion constraints. |
| Validation strength | 8 | 9 | +1 | Adds additional tests and scanners from suggestions. |
| Operational risk controls | 8 | 9 | +1 | Adds worker restart, cancellation logging, and manual smoke controls. |

Prior revised estimate: `91`

New revised estimate: `96`

## Net Effect

The revised plan is broader but more executable. The cost is increased implementation scope from incorporating all suggestions. The main protection against scope creep is explicit ownership boundaries and commit boundaries per phase.
