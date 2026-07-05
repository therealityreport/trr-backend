# LEDGER.v2.md

## BUGS

| ID | Severity | Finding | Evidence | Required Fix | Validation |
|---|---|---|---|---|---|
| BUG-001 | Medium | Live GitHub issue traceability is unverified in this rerun. | Supplied plan names `therealityreport/trr-backend#149`, but current grading used local repo evidence only. | Reframe GitHub issue tracker as optional traceability unless live GitHub proof is gathered. | `REVISED_PLAN.v2.md` capability table and Reality Verification no longer rely on the issue as proof. |
| BUG-002 | Medium | Tool Finder returned irrelevant external packages that could cause scope drift if adopted. | `TOOLS.v2.md` ranks PyPI/Homebrew candidates unrelated to TRR-native Instagram scraping. | Reject generic external packages and keep repo-native scraper/test/Modal seams. | `IMPROVEMENTS.v2.md` and `REVISED_PLAN.v2.md` record rejected candidates. |
| BUG-003 | Low | Previous metadata classified the package as `idea_to_plan`; current rerun is a supplied-plan review. | Plan Architect routing for compatibility alias plus supplied plan file is `existing_plan_review`. | Set v2 `result.json.triggerMode` to `existing_plan_review`. | `result.json` records the corrected trigger mode. |

## SUGGESTIONS

| ID | Type | Existing Scope | Suggested Change | Why | Acceptance Criteria |
|---|---|---|---|---|---|
| SUG-001 | Scope control | Capability table | Mark external package candidates as rejected, not partially integrated. | Prevents accidental third-party scraper adoption. | Revised plan says TRR-native seams only; package candidates are rejected. |
| SUG-002 | Execution safety | Phase 2 SQL | Add explicit migration/readback wording for any snapshot-part schema. | Keeps database changes additive and inspectable. | Phase 2 says schema is additive/nullable and requires repo DB helper readback. |
| SUG-003 | Handoff clarity | Subagent plan | Keep scopes serialized where shared contracts overlap. | Avoids parallel edits to the same queue/completion contracts. | `HANDOFF.v2.md` serializes benchmark, completion, control-plane, lane, media, and ops scopes. |

## RESTRUCTURE

| ID | Current Shape | New Shape | Dependency Impact | Ownership Impact | Validation Impact |
|---|---|---|---|---|---|
| RES-001 | GitHub issue listed as a direct capability/proof source. | GitHub issue tracker listed as optional traceability after live verification. | Removes external dependency from readiness. | Implementation lead can proceed from repo-local docs and files. | No readiness cap for unverified issue. |
| RES-002 | Tool Finder shortlist includes noisy package candidates. | Tool Finder results are filtered to repo-native tools and explicit rejections. | Avoids new packages. | Backend agents use current TRR modules/tests. | Validation stays on existing pytest/py_compile/Modal seams. |
