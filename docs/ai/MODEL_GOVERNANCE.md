# TRR-Backend Model Governance

## Current Defaults (Pinned This Wave)

| Surface | Env key | Current default |
| --- | --- | --- |
| Gemini general | `GEMINI_MODEL` | `gemini-2.5-flash` |
| Social debug OpenAI | `SOCIAL_DEBUG_OPENAI_MODEL` | `gpt-5.3-codex` |
| Route-specific Gemini fast | `GEMINI_MODEL_FAST` | inherits `GEMINI_MODEL` when unset |
| Route-specific Gemini pro | `GEMINI_MODEL_PRO` | inherits `GEMINI_MODEL_FAST`/`GEMINI_MODEL` when unset |

Deprecated alias compatibility:
- `GEMINI-MODEL` remains supported as a temporary alias.
- Target removal date: `2026-06-30`, contingent on all deployed environments migrating to `GEMINI_MODEL`.

## Candidate Upgrade Policy

1. Keep defaults pinned during runtime/tooling modernization.
2. Evaluate candidate model upgrades in a separate change set.
3. Promote only after staged eval evidence is captured:
- output quality checks on representative prompts/flows
- latency and cost comparison
- regression pass on affected API routes/tests
- safety/error-rate review

## Promotion and Rollback

Promotion:
1. Add candidate as an explicit config option (do not remove current default yet).
2. Run regression and contract tests in staging.
3. Switch default in one PR with handoff evidence.

Rollback:
1. Revert the default env key to the previous pinned value.
2. Redeploy services.
3. Record incident notes and failed eval criteria in `docs/ai/HANDOFF.md`.
