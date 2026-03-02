begin;

create schema if not exists admin;

create table if not exists admin.brand_families (
  id uuid primary key default gen_random_uuid(),
  family_key text not null unique,
  display_name text not null,
  owner_wikidata_id text,
  owner_label text,
  is_active boolean not null default true,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists brand_families_active_idx
  on admin.brand_families (is_active, updated_at desc);

create table if not exists admin.brand_family_members (
  id uuid primary key default gen_random_uuid(),
  family_id uuid not null references admin.brand_families(id) on delete cascade,
  entity_type text not null check (entity_type in ('network', 'streaming')),
  entity_key text not null,
  entity_display_name text not null,
  source text not null default 'manual' check (source in ('manual', 'suggested_owner', 'system')),
  confidence numeric(5,4),
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (family_id, entity_type, entity_key)
);

create index if not exists brand_family_members_lookup_idx
  on admin.brand_family_members (entity_type, entity_key, updated_at desc);

create table if not exists admin.brand_family_link_rules (
  id uuid primary key default gen_random_uuid(),
  family_id uuid not null references admin.brand_families(id) on delete cascade,
  link_group text not null check (link_group in ('official', 'social', 'knowledge', 'cast_announcements', 'other')),
  link_kind text not null,
  label text,
  url text not null,
  url_key text not null,
  coverage_type text not null check (
    coverage_type in (
      'family_all_shows',
      'family_network_shows',
      'family_streaming_shows',
      'franchise_rule',
      'show_wikidata_exact',
      'show_name_contains'
    )
  ),
  coverage_value text,
  source text not null default 'manual' check (source in ('manual', 'wikipedia_import', 'system')),
  priority int not null default 100,
  auto_apply boolean not null default true,
  is_active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_by text,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists brand_family_link_rules_active_idx
  on admin.brand_family_link_rules (family_id, is_active, priority asc, updated_at desc);

create unique index if not exists brand_family_link_rules_unique_idx
  on admin.brand_family_link_rules (family_id, link_kind, coverage_type, coalesce(coverage_value, ''), url_key);

create table if not exists admin.brand_family_wikipedia_show_links (
  id uuid primary key default gen_random_uuid(),
  family_id uuid not null references admin.brand_families(id) on delete cascade,
  entity_type text not null check (entity_type in ('network', 'streaming')),
  entity_key text not null,
  brand_wikipedia_url text,
  show_url text not null,
  show_url_key text not null,
  show_title text,
  wikidata_id text,
  matched_show_id uuid references core.shows(id) on delete set null,
  match_method text,
  import_source text not null default 'manual',
  is_applied boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (entity_type, entity_key, show_url_key)
);

create index if not exists brand_family_wiki_links_family_idx
  on admin.brand_family_wikipedia_show_links (family_id, entity_type, entity_key, updated_at desc);

alter table admin.network_streaming_completion
  add column if not exists owner_wikidata_id text,
  add column if not exists owner_label text;

create index if not exists network_streaming_completion_owner_idx
  on admin.network_streaming_completion (owner_wikidata_id, owner_label, entity_type, entity_key);

grant usage on schema admin to service_role;
grant all privileges on table admin.brand_families to service_role;
grant all privileges on table admin.brand_family_members to service_role;
grant all privileges on table admin.brand_family_link_rules to service_role;
grant all privileges on table admin.brand_family_wikipedia_show_links to service_role;

commit;
