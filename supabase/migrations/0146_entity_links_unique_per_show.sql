begin;

-- Ensure entity links are unique per show. The prior unique key omitted show_id,
-- which caused cross-show collisions for shared cast members and source URLs.

do $$
begin
  if exists (
    select 1
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    join pg_namespace n on n.oid = t.relnamespace
    where n.nspname = 'core'
      and t.relname = 'entity_links'
      and c.conname = 'entity_links_unique_active'
  ) then
    alter table core.entity_links
      drop constraint entity_links_unique_active;
  end if;
end $$;

alter table core.entity_links
  add constraint entity_links_unique_active
  unique (show_id, entity_type, entity_id, link_kind, season_number, url_key);

commit;
