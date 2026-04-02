# Show Page Parity Shared Social Links

- Replaced Bravo show-link seeding in [admin_show_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py) so runtime social defaults now read from `social.shared_account_sources` instead of a hardcoded Python tuple.
- Added [20260402194500_seed_bravo_shared_account_sources.sql](/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/20260402194500_seed_bravo_shared_account_sources.sql) to seed the current Bravo shared-account catalog in Supabase.
- Added [backfill_shared_social_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/backfill_shared_social_links.py) to persist shared-source social links onto existing Bravo shows.
- Extended show overview read coverage in [test_admin_show_links.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py) and [test_admin_show_reads_repository.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_admin_show_reads_repository.py) for shared social-source reads and regional watch availability.
