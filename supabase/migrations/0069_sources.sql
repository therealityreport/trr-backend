begin;

create schema if not exists core;

create table if not exists core.sources (
  id text primary key,
  category text not null,
  aliases text[] default '{}'::text[],
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

insert into core.sources (id, category, aliases)
values
  ('imdb','vendor','{}'),
  ('tmdb','vendor','{}'),
  ('wikidata','vendor','{}'),
  ('tvdb','vendor','{}'),
  ('tvrage','vendor','{}'),
  ('fandom','vendor','{}'),
  ('facebook','social','{}'),
  ('instagram','social','{}'),
  ('twitter','social', array['x']),
  ('tiktok','social','{}'),
  ('youtube','social','{}')
on conflict (id) do update
set category = excluded.category,
    aliases  = excluded.aliases,
    updated_at = now();

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'sources_set_updated_at') then
    create trigger sources_set_updated_at
    before update on core.sources
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- Grants / RLS (adjust to your access model)
grant select on table core.sources to anon, authenticated;
grant all privileges on table core.sources to service_role;

alter table core.sources enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'sources'
      and policyname = 'core_sources_public_read'
  ) then
    create policy core_sources_public_read
      on core.sources for select to anon, authenticated
      using (true);
  end if;
end $$;

commit;
