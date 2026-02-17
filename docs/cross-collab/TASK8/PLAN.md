# Season Social Analytics V3 — Task 8 Plan

Repo: TRR-Backend
Last updated: February 17, 2026

## Goal

Deliver additive season-social analytics upgrades for weekly no-data handling and contextual sentiment classification.

## Scope

1. Add `weekly_platform_engagement` to season analytics payload:
- per-week engagement totals by platform,
- `total_engagement`,
- `has_data` for no-data week rendering.
2. Upgrade sentiment pipeline from token-only scoring to hybrid contextual analysis:
- rule-based polarity with negation/intensifiers/contrast handling,
- cast/entity-aware token treatment,
- optional Gemini pass for ambiguous comments only.
3. Fix theme-driver extraction:
- exclude cast names and handle/mention-derived tokens,
- score drivers from resolved comment sentiment labels.
4. Add/adjust backend tests for:
- sentiment rules,
- cast-name filtering,
- `weekly_platform_engagement` output shape and `has_data` behavior.
5. Document new sentiment Gemini env flags in `.env.example`.

## Out of Scope

- Schema migrations.
- screenalytics code changes.
- Breaking response-shape changes.

## Acceptance Criteria

1. `GET /api/v1/admin/socials/seasons/{season_id}/analytics` includes `weekly_platform_engagement`.
2. Weeks with zero engagement return `has_data=false` and zero platform engagement totals.
3. Sentiment drivers no longer include cast names/handles as terms.
4. Rule analyzer tests cover negation/contrast/name-only neutral behavior.
5. Targeted backend lint/compile/tests pass.
