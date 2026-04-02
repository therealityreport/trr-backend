begin;

insert into social.shared_account_sources (
  platform,
  source_scope,
  account_handle,
  is_active,
  scrape_priority,
  metadata,
  updated_by,
  updated_at
)
values
  ('instagram', 'bravo', 'bravotv', true, 10, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('instagram', 'bravo', 'bravodailydish', true, 20, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('instagram', 'bravo', 'bravowwhl', true, 30, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('tiktok', 'bravo', 'bravotv', true, 40, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('tiktok', 'bravo', 'bravowwhl', true, 50, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('threads', 'bravo', 'bravotv', true, 60, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('threads', 'bravo', 'bravowwhl', true, 70, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('twitter', 'bravo', 'bravotv', true, 80, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('twitter', 'bravo', 'bravowwhl', true, 90, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('youtube', 'bravo', 'bravo', true, 100, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now()),
  ('youtube', 'bravo', 'wwhl', true, 110, '{"seed_source":"migration:20260402194500_seed_bravo_shared_account_sources"}'::jsonb, 'migration:20260402194500_seed_bravo_shared_account_sources', now())
on conflict (platform, source_scope, account_handle)
do update set
  is_active = excluded.is_active,
  scrape_priority = excluded.scrape_priority,
  metadata = coalesce(social.shared_account_sources.metadata, '{}'::jsonb) || excluded.metadata,
  updated_by = excluded.updated_by,
  updated_at = now();

commit;
