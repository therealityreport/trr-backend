begin;

-- Add wikipedia as a supported source id.
insert into core.sources (id, category, aliases)
values ('wikipedia', 'identifier', array['wikipedia.org'])
on conflict (id) do update
set
  category = excluded.category,
  aliases = excluded.aliases,
  updated_at = now();

-- Generic link registry for show/season/person admin curation and discovery.
create table if not exists core.entity_links (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null check (entity_type in ('show', 'season', 'person')),
  entity_id uuid not null,
  show_id uuid references core.shows(id) on delete cascade,
  season_number integer not null default 0 check (season_number >= 0 and season_number <= 200),
  link_group text not null check (link_group in ('official', 'social', 'knowledge', 'cast_announcements', 'other')),
  link_kind text not null,
  label text,
  url text not null,
  url_key text not null,
  status text not null default 'pending' check (status in ('pending', 'approved', 'rejected')),
  confidence numeric(5,4),
  discovered_by text,
  source text,
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint entity_links_unique_active
    unique (entity_type, entity_id, link_kind, season_number, url_key)
);

create index if not exists entity_links_show_id_idx on core.entity_links(show_id);
create index if not exists entity_links_entity_idx on core.entity_links(entity_type, entity_id);
create index if not exists entity_links_status_idx on core.entity_links(status);
create index if not exists entity_links_group_idx on core.entity_links(link_group);

-- Show-scoped role catalog.
create table if not exists core.show_role_catalog (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  name text not null,
  normalized_name text not null,
  is_active boolean not null default true,
  sort_order integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint show_role_catalog_show_normalized_unique unique (show_id, normalized_name)
);

create index if not exists show_role_catalog_show_id_idx on core.show_role_catalog(show_id, is_active, sort_order);

-- Per-show, per-person, per-season role assignments.
create table if not exists core.show_cast_role_assignments (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  person_id uuid not null references core.people(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete set null,
  season_number integer not null default 0 check (season_number >= 0 and season_number <= 200),
  role_id uuid not null references core.show_role_catalog(id) on delete cascade,
  source text not null default 'manual',
  confidence numeric(5,4),
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint show_cast_role_assignments_unique unique (show_id, person_id, season_number, role_id)
);

create index if not exists show_cast_role_assignments_show_person_idx
  on core.show_cast_role_assignments(show_id, person_id, season_number);
create index if not exists show_cast_role_assignments_role_idx
  on core.show_cast_role_assignments(role_id);

create or replace view core.v_show_cast_roles_enriched as
select
  sc.show_id,
  sc.person_id,
  max(coalesce(p.full_name, sc.cast_member_name)) as person_name,
  count(distinct case
    when coalesce(vec.appearance_type, 'appears') <> 'archive_footage' then vec.episode_id
    else null
  end)::int as total_episodes,
  count(distinct case
    when coalesce(vec.appearance_type, '') = 'archive_footage' then vec.episode_id
    else null
  end)::int as archive_episodes,
  count(distinct case when vec.season_number is not null then vec.season_number end)::int as seasons_appeared,
  array_remove(array_agg(distinct vec.season_number), null) as season_numbers,
  max(case when vec.season_number is not null then vec.season_number else null end)::int as latest_season,
  array_remove(array_agg(distinct case when rc.is_active then rc.name else null end), null) as roles
from core.v_show_cast sc
left join core.people p
  on p.id = sc.person_id
left join core.v_episode_credits vec
  on vec.show_id = sc.show_id
 and vec.person_id = sc.person_id
left join core.show_cast_role_assignments sra
  on sra.show_id = sc.show_id
 and sra.person_id = sc.person_id
left join core.show_role_catalog rc
  on rc.id = sra.role_id
 and rc.show_id = sra.show_id
group by sc.show_id, sc.person_id;

grant select on table core.entity_links to authenticated;
grant all privileges on table core.entity_links to service_role;
grant select on table core.show_role_catalog to authenticated;
grant all privileges on table core.show_role_catalog to service_role;
grant select on table core.show_cast_role_assignments to authenticated;
grant all privileges on table core.show_cast_role_assignments to service_role;
grant select on table core.v_show_cast_roles_enriched to authenticated;
grant select on table core.v_show_cast_roles_enriched to service_role;

alter table core.entity_links enable row level security;
alter table core.show_role_catalog enable row level security;
alter table core.show_cast_role_assignments enable row level security;

drop policy if exists entity_links_select_authenticated on core.entity_links;
create policy entity_links_select_authenticated on core.entity_links
for select to authenticated
using (true);

drop policy if exists show_role_catalog_select_authenticated on core.show_role_catalog;
create policy show_role_catalog_select_authenticated on core.show_role_catalog
for select to authenticated
using (true);

drop policy if exists show_cast_role_assignments_select_authenticated on core.show_cast_role_assignments;
create policy show_cast_role_assignments_select_authenticated on core.show_cast_role_assignments
for select to authenticated
using (true);

commit;
