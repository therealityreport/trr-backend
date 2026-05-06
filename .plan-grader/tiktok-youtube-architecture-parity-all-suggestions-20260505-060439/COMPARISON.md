# Comparison

## Prior Revised Plan vs All-Suggestions Revision

| Area | Prior revised plan | All-suggestions revision | Impact |
| --- | --- | --- | --- |
| Suggestions | Listed as optional follow-ups | All ten accepted as implementation tasks | Broader execution scope and stronger traceability. |
| Plan structure | Core TikTok/Youtube phases | Adds required `ADDITIONAL SUGGESTIONS` phase | Meets suggestion-incorporation contract. |
| Metadata fixtures | Optional | Required fixture or colocated fixture coverage | Better regression protection. |
| Runbook and 30-day query | Optional | Required docs follow-through | Stronger operator adoption. |
| Smoke wrapper | Optional | Required script/help or documented stop reason | Better local verification ergonomics. |
| Queue/admin copy | Optional | Required backend contract check or app follow-through note | Reduces admin truth mismatch risk. |
| Score estimate | 90.92 | 93.20 | Higher value and durability, with more implementation cost. |

## Risk Delta

Risk increases modestly because docs, fixtures, scripts, and queue checks widen the touched surface. The revised handoff contains that risk by assigning non-overlapping ownership scopes:

- TikTok lane work stays with Worker A.
- YouTube extraction and YouTube fixtures stay with Worker B.
- Docs/scripts/general suggestions stay with Worker C.
- Shared integration files stay with the main session.

## Recommended Source Of Truth

Use this package's `REVISED_PLAN.md` as the implementation source of truth.
