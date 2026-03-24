# Getty/NBCUMV person-gallery bucket normalization

Last updated: 2026-03-20

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-03-20
  current_phase: "Getty/NBCUMV person refresh classification remains in place, and the latest follow-up now stamps Google reverse-image-search URLs onto Getty fallback rows for manual operator use"
  next_action: "Run one live person-gallery Get Images pass only if needed to confirm the new Google reverse-search link appears on Getty fallback rows in the lightbox; otherwise archive this item in the next cleanup pass"
  detail: self
```

- Root cause split in two:
  1. `trr_backend.integrations.nbcumv` was defaulting to an empty AppSync key while the BRAVOTV reference scripts use the public AppSync key directly.
  2. The live workspace backend was still running a pre-fix non-reload process, so browser-driven refreshes kept using the old code until the workspace restart.
- `trr_backend.integrations.nbcumv` now defaults `APPSYNC_API_KEY` to the known working public key `da2-rmy4cbtcevfwrdadqabta7ezl4` when no override is present.
- The person-gallery NBCUMV crosswalk now falls back from exact `lbx_filename` lookup to date-scoped and caption-scoped searches, then matches locally by exact NUP filename or, when unambiguous, by shared `NUP_<set>` prefix.
- Getty grouped-event search remains a first-class backend path for person refreshes.
- Bravo-scoped grouped events continue to feed `show`, `wwhl`, and `bravocon` buckets, while broad person-name grouped events only survive when at least one sampled image matches the target person and the bucket resolves to `event`.
- Broad person-name grouped events now also require `minimum_grouped_image_count=2`, so one-off or zero-count Getty event buckets do not enter the person gallery inventory.
- Direct NBCUMV person discovery no longer depends only on the broken cross-show CloudSearch pager:
  - show-scoped direct searches now use `list_show_images(...)` plus local person-caption filtering via `search_person_show_catalog(...)`
  - no-show discovery supplements credited shows with CloudSearch-discovered show titles
  - Getty `Object Name` to NBCUMV matching is now case-insensitive and padding-tolerant for `NUP_<set>_<frame>` identifiers, with CloudSearch filename fallback before date/caption fallback scans
- Getty grouped-event matching no longer treats the grouped search card URL like an event page. The representative Getty detail page is fetched directly, and the target-person match test runs against that representative asset.
- The direct all-NBCUMV path no longer makes a meaningless empty-`show_id` show-catalog call before the real all-catalog search.
- Media-asset mirror writes now recover from duplicate `hosted_sha256` / `sha256` conflicts by storing the hosted location fields without reasserting the duplicate hash columns, which keeps usable duplicate-byte assets from failing the run.
- Getty fallback rows continue to persist grouped event metadata, Getty detail fields, Getty tag lists, people counts, source resolution, and Getty event URL/id/slug/date alongside the preview/watermark asset, with hosted fields repaired so those rows are visible in the gallery.
- The NBCUMV person-refresh import stage now explicitly classifies and reports three categories:
  - `nbcumv_preferred_shared` for Getty↔NBCUMV overlaps, with the NBCUMV asset imported and preferred
  - `nbcumv_only` for direct NBCUMV discoveries that have no Getty counterpart
  - `getty_watermark_fallback` for Getty-only rows that remain unmatched in NBCUMV
- `getty_matched_total` now reflects only shared Getty↔NBCUMV matches, while new counters `shared_nbcumv_total`, `shared_nbcumv_imported`, `nbcumv_only_total`, and `nbcumv_only_imported` track the breakdown explicitly in sync responses and stream-complete payloads.
- NBCUMV-imported media assets now carry `source_resolution` inside `gallery_bucket`/stored metadata so the gallery can distinguish shared NBCUMV-preferred rows from NBCUMV-only rows without inferring from source.
- The Getty/NBCUMV summary line and stream status messaging now report `shared via NBCUMV`, `NBCUMV-only`, and `Getty-only` separately instead of collapsing all hi-res imports into a single `imported` count.
- Router tests now default-stub NBCUMV direct discovery helpers so refresh tests stay deterministic and do not accidentally hit live CloudSearch.
- Focused validation passed for the CloudSearch/grouped-event/direct-discovery follow-up:
  - `ruff check trr_backend/integrations/nbcumv.py trr_backend/integrations/getty.py api/routers/admin_person_images.py tests/integrations/test_nbcumv.py tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py`
  - `pytest tests/integrations/test_nbcumv.py tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py -k 'nbcumv or getty or grouped or direct'`
- Focused validation passed after the auth/NUP fixes:
  - `ruff check trr_backend/integrations/nbcumv.py api/routers/admin_person_images.py tests/integrations/test_nbcumv.py tests/api/routers/test_admin_person_images.py`
  - `pytest tests/integrations/test_nbcumv.py tests/api/routers/test_admin_person_images.py -k 'default_public_appsync_key or nbcumv_person_media'`
- Focused validation passed for the grouped-event threshold follow-up:
  - `ruff check trr_backend/integrations/getty.py api/routers/admin_person_images.py tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py`
  - `ruff format --check trr_backend/integrations/getty.py api/routers/admin_person_images.py tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py`
  - `pytest tests/integrations/test_getty.py tests/api/routers/test_admin_person_images.py -k 'grouped_events or broad_grouped_events or minimum_count'`
- Focused validation passed for the mirror dedupe fallback:
  - `ruff check trr_backend/repositories/media_assets.py api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py`
  - `pytest tests/api/routers/test_admin_person_images.py -k 'mirror_person_media_assets and (duplicate_sha_conflict or skips_previously_failed_rows_without_force)'`
- Focused validation passed for the shared/NBCUMV-only/Getty-only classification follow-up:
  - `ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py`
  - `pytest tests/api/routers/test_admin_person_images.py -k 'nbcumv_person_media and (persists_getty_unmatched_urls or falls_back_to_direct_nbcumv_caption_search or uses_credited_shows_for_direct_nbcumv_search_without_request_context or supplements_getty_matches_with_all_nbcumv_caption_search or uses_show_index_crosswalk_when_filename_search_misses)'`
  - `pytest tests/api/routers/test_admin_person_images.py -k 'stream_progress_for_nbcumv_source or stream_uses_nbcumv_stage_totals_when_getty_candidates_are_zero'`
- Direct shell verification after the code fix and restart confirmed the backend now resolves RHOSLC with the public AppSync key:
  - `api_key da2-rmy4cbtcevfwrdadqabta7ezl4`
  - `resolve_show_by_title('The Real Housewives of Salt Lake City') -> 490e731c-d85f-474f-945b-b9681dc1931b`
- Live backend evidence after the workspace restart:
  - no new `401 Client Error: Unauthorized`
  - no new `NBCUMV unavailable`
  - no new `Getty complete with NBCUMV unavailable`
  messages in `/Users/thomashulihan/Projects/TRR/.logs/workspace/trr-backend.log` during the restarted Lisa Barlow rerun
- Managed Chrome verification on `http://admin.localhost:3000/people/lisa-barlow/gallery` after restart showed the gallery already presenting mixed `NBCUMV` and `Getty` source cards before the new rerun completed, which is materially better than the prior Getty-only/unavailable state.
- Managed Chrome verification after the follow-up gallery/UI patch showed Lisa Barlow with a single `RHOSLC` show chip, no duplicate `RHOSLC`, no empty `RHOBH`, and no zero-count event option in the `Events` menu after reload.
- Managed Chrome verification on the current rerun shows the Getty/NBCUMV stage actively matching real work instead of returning `0/0`:
  - `SYNCING 4/5`
  - `GETTY / NBCUMV scraped 104/219 · saved 0 · remaining 115`
  - current progress text: `Matching Getty asset 105/219: NUP_209194_01084.jpg`
- The current local backend log for operation `8af1d1aa-ecf5-43ff-9c4e-06af08d59200` confirms the stream is replaying real progress for Lisa Barlow after the patch:
  - the operation was created at `2026-03-17 01:08:41`
  - replay advanced through at least `after_seq=48`
  - the run reached late mirror work and no longer stops with `No Getty candidates found` or `NBCUMV direct search requires show context`
- The most recent reported failure signature was `Supabase error updating mirror result: duplicate key value violates unique constraint "media_assets_source_hosted_sha_uq"` during S3 mirroring; that path now falls back instead of counting as a failed mirrored asset.
- 2026-03-20 follow-up:
  - Getty fallback rows now persist `google_reverse_image_search_url` metadata built from the Getty preview URL so operators can open a manual Google Image Search when no automatic public replacement is available.
  - This follow-up intentionally kept the existing object-storage mirror path unchanged; hosted media still flows through the shared object-storage/R2-compatible mirror helpers rather than any local-file save path.
  - Focused validation passed:
    - `./.venv/bin/ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py`
    - `./.venv/bin/ruff format --check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py`
    - `./.venv/bin/pytest -q tests/api/routers/test_admin_person_images.py -k 'import_nbcumv_person_media_auto_replaces_bravocon_getty_asset or import_nbcumv_person_media_falls_back_to_getty_when_nbcumv_unavailable'`
