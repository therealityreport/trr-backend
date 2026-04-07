# Show Page Parity Shared Social Links

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-07
  current_phase: "show page TMDb watch-provider buckets landed"
  next_action: "Use as reference while downstream app parity work continues."
  detail: self
```

- Replaced Bravo show-link seeding in [admin_show_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py) so runtime social defaults now read from `social.shared_account_sources` instead of a hardcoded Python tuple.
- Added [20260402194500_seed_bravo_shared_account_sources.sql](/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/20260402194500_seed_bravo_shared_account_sources.sql) to seed the current Bravo shared-account catalog in Supabase.
- Added [backfill_shared_social_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/backfill_shared_social_links.py) to persist shared-source social links onto existing Bravo shows.
- Extended show overview read coverage in [test_admin_show_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py) and [test_admin_show_reads_repository.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_admin_show_reads_repository.py) for shared social-source reads and regional watch availability.
- Extended [admin_show_reads.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/admin_show_reads.py) to emit additive all-region `watch_provider_regions` payload rows grouped into TMDb-backed `stream`, `free`, and `buy_rent` buckets, with `US/GB/CA/AU` sorted ahead of other regions.
- Verified RHOSLC (`tmdb_id=110381`) already has persisted `core.show_watch_providers` rows for `US` `flatrate`, `free`, and `buy`, so no JustWatch API integration or sync-source change was needed.
- Passed: `pytest -q tests/repositories/test_admin_show_reads_repository.py`
