# Security auth hygiene hardening

Last updated: 2026-04-10

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-10
  current_phase: "backend auth defaults tightened and local-only evidence artifacts untracked"
  next_action: "decide whether to remove the remaining generic service-role admin escape hatches in additional routes or keep the new opt-in flags as the long-term compatibility path"
  detail: self
```

## What changed

- `api/auth.py` now treats raw `X-TRR-Internal-Admin-Secret` fallback as opt-in via `TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK`.
- Generic `require_admin` and `require_internal_admin` service-role bypasses now require explicit opt-in flags:
  - `TRR_ADMIN_ALLOW_SERVICE_ROLE`
  - `TRR_INTERNAL_ADMIN_ALLOW_SERVICE_ROLE`
- Dedicated service-role-plus-secret exception paths remain intact for cast screentime and facebank seed flows.
- Repo hygiene now blocks tracked local-only artifacts through `scripts/check_repo_hygiene.py`.
- CI now runs the repo hygiene check before install/test steps.
- `data/tiktok_cookies.json` and `docs/ai/evidence/**` are ignored and untracked from git, while `docs/ai/evidence/README.md` remains as the tracked policy note.

## Validation snapshot

- `python -m pytest tests/api/test_auth.py tests/scripts/test_repo_hygiene.py -q` -> `17 passed`
- `python -m pytest tests/api/test_admin_cast_screentime.py -q -k "service_role"` -> `3 passed`
- `python -m pytest tests/api/routers/test_admin_person_images.py -q -k "service_role"` -> `3 passed`
- `python scripts/check_repo_hygiene.py` -> pass
- `pnpm -C apps/web exec vitest run -c vitest.config.ts tests/internal-admin-auth.test.ts tests/social-admin-proxy.test.ts` -> `16 passed`

## Notes

- `TRR-APP` did not require code changes because its internal admin proxy already signs short-lived JWTs and strips the raw secret header.
- This slice intentionally did not touch the larger streaming-risk and social-control-plane refactor items from the concern map.
