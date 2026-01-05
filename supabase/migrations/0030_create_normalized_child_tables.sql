begin;

-- =============================================================================
-- Migration 0030: Create normalized child tables for repeating fields
-- =============================================================================
-- Replaces JSON arrays with proper normalized tables for genres, keywords,
-- streaming providers, and networks.

-- ---------------------------------------------------------------------------
-- core.show_genres - genres by source (IMDb/TMDb)
-- ---------------------------------------------------------------------------
create table if not exists core.show_genres (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  source text not null check (source in ('imdb', 'tmdb')),
  genre text not null,
  created_at timestamptz not null default now(),
  constraint show_genres_unique unique (show_id, source, genre)
);

create index if not exists show_genres_show_id_idx on core.show_genres (show_id);
create index if not exists show_genres_genre_idx on core.show_genres (genre);

-- ---------------------------------------------------------------------------
-- core.show_keywords - keywords by source (IMDb/TMDb)
-- ---------------------------------------------------------------------------
create table if not exists core.show_keywords (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  source text not null check (source in ('imdb', 'tmdb')),
  keyword text not null,
  created_at timestamptz not null default now(),
  constraint show_keywords_unique unique (show_id, source, keyword)
);

create index if not exists show_keywords_show_id_idx on core.show_keywords (show_id);
create index if not exists show_keywords_keyword_idx on core.show_keywords (keyword);

-- ---------------------------------------------------------------------------
-- core.show_streaming_providers - streaming providers by region
-- ---------------------------------------------------------------------------
create table if not exists core.show_streaming_providers (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  provider text not null,
  region text not null default 'US',
  provider_type text check (provider_type in ('flatrate', 'rent', 'buy', 'ads')),
  tmdb_provider_id integer,
  logo_path text,
  display_priority integer,
  created_at timestamptz not null default now(),
  constraint show_streaming_providers_unique unique (show_id, provider, region)
);

create index if not exists show_streaming_providers_show_id_idx on core.show_streaming_providers (show_id);
create index if not exists show_streaming_providers_provider_idx on core.show_streaming_providers (provider);
create index if not exists show_streaming_providers_region_idx on core.show_streaming_providers (region);

-- ---------------------------------------------------------------------------
-- core.show_networks - networks associated with a show
-- ---------------------------------------------------------------------------
create table if not exists core.show_networks (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  network text not null,
  tmdb_network_id integer,
  logo_path text,
  origin_country text,
  is_primary boolean default false,
  created_at timestamptz not null default now(),
  constraint show_networks_unique unique (show_id, network)
);

create index if not exists show_networks_show_id_idx on core.show_networks (show_id);
create index if not exists show_networks_network_idx on core.show_networks (network);

-- ---------------------------------------------------------------------------
-- core.show_tags - content tags from IMDb (e.g., "Reality-TV", "Competition")
-- ---------------------------------------------------------------------------
create table if not exists core.show_tags (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  source text not null check (source in ('imdb', 'tmdb')),
  tag text not null,
  created_at timestamptz not null default now(),
  constraint show_tags_unique unique (show_id, source, tag)
);

create index if not exists show_tags_show_id_idx on core.show_tags (show_id);
create index if not exists show_tags_tag_idx on core.show_tags (tag);

-- ---------------------------------------------------------------------------
-- Grants and RLS
-- ---------------------------------------------------------------------------
grant select on table core.show_genres to anon, authenticated;
grant select on table core.show_keywords to anon, authenticated;
grant select on table core.show_streaming_providers to anon, authenticated;
grant select on table core.show_networks to anon, authenticated;
grant select on table core.show_tags to anon, authenticated;

grant all privileges on table core.show_genres to service_role;
grant all privileges on table core.show_keywords to service_role;
grant all privileges on table core.show_streaming_providers to service_role;
grant all privileges on table core.show_networks to service_role;
grant all privileges on table core.show_tags to service_role;

alter table core.show_genres enable row level security;
alter table core.show_keywords enable row level security;
alter table core.show_streaming_providers enable row level security;
alter table core.show_networks enable row level security;
alter table core.show_tags enable row level security;

drop policy if exists core_show_genres_public_read on core.show_genres;
create policy core_show_genres_public_read on core.show_genres
  for select to anon, authenticated using (true);

drop policy if exists core_show_keywords_public_read on core.show_keywords;
create policy core_show_keywords_public_read on core.show_keywords
  for select to anon, authenticated using (true);

drop policy if exists core_show_streaming_providers_public_read on core.show_streaming_providers;
create policy core_show_streaming_providers_public_read on core.show_streaming_providers
  for select to anon, authenticated using (true);

drop policy if exists core_show_networks_public_read on core.show_networks;
create policy core_show_networks_public_read on core.show_networks
  for select to anon, authenticated using (true);

drop policy if exists core_show_tags_public_read on core.show_tags;
create policy core_show_tags_public_read on core.show_tags
  for select to anon, authenticated using (true);

commit;
