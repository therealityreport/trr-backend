-- Move WWHL Bravo's shared Bravo-network profiles under the WWHL show card
-- while preserving source_scope='network' so Bravo overall analysis keeps them.

do $$
declare
  wwhl_show_id uuid;
  wwhl_show_name text;
  updated_count integer;
begin
  select id, name
  into wwhl_show_id, wwhl_show_name
  from core.shows
  where name = 'Watch What Happens Live with Andy Cohen'
  limit 1;

  if wwhl_show_id is null then
    return;
  end if;

  with wwhl_sources(platform, account_handle, display_name) as (
    values
      ('instagram', 'bravowwhl', 'WWHL Bravo'),
      ('threads', 'bravowwhl', 'WWHL Bravo'),
      ('tiktok', 'bravowwhl', 'WWHL Bravo'),
      ('twitter', 'bravowwhl', 'WWHL Bravo'),
      ('youtube', 'wwhl', 'WWHL Bravo')
  ),
  updated as (
    update social.shared_account_sources sas
    set
      is_active = true,
      metadata = (
        coalesce(sas.metadata, '{}'::jsonb)
        - 'deactivated_by'
        - 'deactivated_reason'
      ) || jsonb_build_object(
        'assigned_show_id', wwhl_show_id::text,
        'display_name', wwhl_sources.display_name,
        'network_name', wwhl_show_name,
        'network_key', 'bravo-tv',
        'profile_kind', 'show_official',
        'assignment_mode', 'community_match',
        'assignment_rules', jsonb_build_object(
          'use_hashtags', true,
          'use_mentions', true,
          'use_collaborators', sas.platform = 'instagram',
          'use_configured_aliases', true,
          'allow_multi_show_candidates', false
        ),
        'source_scope', 'network',
        'platform', wwhl_sources.platform,
        'account_handle', wwhl_sources.account_handle
      ),
      updated_by = 'migration:20260514011500_assign_wwhl_shared_sources_to_show',
      updated_at = now()
    from wwhl_sources
    where sas.source_scope = 'network'
      and sas.platform = wwhl_sources.platform
      and sas.account_handle = wwhl_sources.account_handle
    returning sas.platform, sas.account_handle
  )
  select count(*) into updated_count from updated;

  if updated_count <> 5 then
    raise exception 'Expected to update 5 WWHL shared sources, updated %', updated_count;
  end if;
end $$;
