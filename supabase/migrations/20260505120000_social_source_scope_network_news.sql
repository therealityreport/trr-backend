begin;

-- Canonicalize shared-source taxonomy:
--   bravo -> network
--   add news as a first-class shared source scope

delete from social.shared_account_sources legacy
using social.shared_account_sources canonical
where legacy.source_scope = 'bravo'
  and canonical.source_scope = 'network'
  and legacy.platform = canonical.platform
  and legacy.account_handle = canonical.account_handle;

delete from social.shared_post_matches legacy
using social.shared_post_matches canonical
where legacy.source_scope = 'bravo'
  and canonical.source_scope = 'network'
  and legacy.platform = canonical.platform
  and legacy.source_id = canonical.source_id;

delete from social.shared_post_review_queue legacy
using social.shared_post_review_queue canonical
where legacy.source_scope = 'bravo'
  and canonical.source_scope = 'network'
  and legacy.platform = canonical.platform
  and legacy.source_id = canonical.source_id;

delete from social.season_targets legacy
using social.season_targets canonical
where legacy.source_scope = 'bravo'
  and canonical.source_scope = 'network'
  and legacy.season_id = canonical.season_id
  and legacy.platform = canonical.platform;

do $$
declare
  table_row record;
  constraint_row record;
begin
  for table_row in
    select table_name
    from information_schema.columns
    where table_schema = 'social'
      and column_name = 'source_scope'
  loop
    for constraint_row in
      select c.conname
      from pg_constraint c
      join pg_class rel on rel.oid = c.conrelid
      join pg_namespace nsp on nsp.oid = rel.relnamespace
      where nsp.nspname = 'social'
        and rel.relname = table_row.table_name
        and c.contype = 'c'
        and pg_get_constraintdef(c.oid) ilike '%source_scope%'
    loop
      execute format(
        'alter table social.%I drop constraint %I',
        table_row.table_name,
        constraint_row.conname
      );
    end loop;

    execute format(
      'update social.%I set source_scope = %L where source_scope = %L',
      table_row.table_name,
      'network',
      'bravo'
    );
    execute format(
      'alter table social.%I alter column source_scope set default %L',
      table_row.table_name,
      'network'
    );
    execute format(
      'alter table social.%I add constraint %I check (source_scope is null or source_scope in (%L, %L, %L, %L))',
      table_row.table_name,
      table_row.table_name || '_source_scope_check_v2',
      'network',
      'creator',
      'community',
      'news'
    );
  end loop;
end $$;

update social.shared_account_sources
set metadata = coalesce(metadata, '{}'::jsonb)
  || jsonb_build_object(
    'network_key', coalesce(metadata->>'network_key', 'bravo-tv'),
    'display_name', coalesce(metadata->>'display_name', 'Bravo TV'),
    'network_name', coalesce(metadata->>'network_name', 'Bravo TV')
  )
where source_scope = 'network'
  and (
    metadata->>'network_key' is null
    or metadata->>'display_name' is null
    or metadata->>'network_name' is null
  );

commit;
