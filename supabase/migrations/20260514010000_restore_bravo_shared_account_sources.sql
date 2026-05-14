begin;

with desired_sources(platform, account_handle, display_name, scrape_priority) as (
  values
    ('tiktok', 'bravotv', 'Bravo TV', 40),
    ('tiktok', 'bravowwhl', 'WWHL Bravo', 50),
    ('twitter', 'bravotv', 'Bravo TV', 80),
    ('twitter', 'bravowwhl', 'WWHL Bravo', 90),
    ('youtube', 'bravo', 'Bravo TV', 100),
    ('youtube', 'wwhl', 'WWHL Bravo', 110)
)
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
select
  platform,
  'network',
  account_handle,
  true,
  scrape_priority,
  jsonb_build_object(
    'seed_source', 'migration:20260514010000_restore_bravo_shared_account_sources',
    'network_key', 'bravo-tv',
    'network_name', 'Bravo TV',
    'display_name', display_name,
    'platform', platform,
    'account_handle', account_handle,
    'source_scope', 'network',
    'profile_kind', 'network_official'
  ),
  'migration:20260514010000_restore_bravo_shared_account_sources',
  now()
from desired_sources
on conflict (platform, source_scope, account_handle)
do update set
  is_active = true,
  scrape_priority = excluded.scrape_priority,
  metadata = (
    coalesce(social.shared_account_sources.metadata, '{}'::jsonb)
      - 'deactivated_by'
      - 'deactivated_reason'
  ) || excluded.metadata,
  updated_by = excluded.updated_by,
  updated_at = now();

commit;
