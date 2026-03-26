# Person-Gallery Source Progress + Getty Parser Hardening

- Date: `2026-03-16`
- Status: `backend implementation complete`

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-24
  current_phase: "archived continuity note"
  next_action: "See newer continuity notes if follow-up is needed"
  detail: self
```

## What Changed
- Getty search parsing now accepts both the older `data-component="Search"` payload and the current prerendered Getty search JSON shape used on live Bravo searches.
- Person image refresh streams now emit structured `source_progress` data for `imdb`, `tmdb`, `fandom`, `fandom_gallery`, and `getty_nbcumv`.
- Getty/NBCUMV zero-result searches now complete cleanly instead of being treated as backend errors when Getty truly returns no assets.

## Validation
- `pytest tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py -q`
- `ruff check api/routers/admin_person_images.py trr_backend/integrations/getty.py tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py`
- Live sanity check: `getty.search_editorial_assets("Mary Cosby", query_params={"artistexact": "bravo"})` returned real candidate details again.

## Remaining Follow-Up
- Run one real worker-backed `Get Images` pass from the app to confirm the new `source_progress` contract renders cleanly end to end in the operator UI.
