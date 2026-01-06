begin;

-- ---------------------------------------------------------------------------
-- core.tmdb_networks
-- ---------------------------------------------------------------------------

create table if not exists core.tmdb_networks (
  id integer primary key,
  name text not null,
  origin_country text null,
  tmdb_logo_path text null,
  hosted_logo_key text null,
  hosted_logo_url text null,
  hosted_logo_sha256 text null,
  hosted_logo_content_type text null,
  hosted_logo_bytes bigint null,
  hosted_logo_etag text null,
  hosted_logo_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists tmdb_networks_name_idx
  on core.tmdb_networks (name);

-- Maintain updated_at automatically on updates.
drop trigger if exists core_tmdb_networks_set_updated_at on core.tmdb_networks;
create trigger core_tmdb_networks_set_updated_at
before update on core.tmdb_networks
for each row
execute function core.set_updated_at();

alter table core.tmdb_networks enable row level security;

drop policy if exists core_tmdb_networks_public_read on core.tmdb_networks;
create policy core_tmdb_networks_public_read on core.tmdb_networks
for select
using (true);

drop policy if exists core_tmdb_networks_service_role on core.tmdb_networks;
create policy core_tmdb_networks_service_role on core.tmdb_networks
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

grant select on table core.tmdb_networks to anon, authenticated;
grant all privileges on table core.tmdb_networks to service_role;

-- ---------------------------------------------------------------------------
-- core.tmdb_production_companies
-- ---------------------------------------------------------------------------

create table if not exists core.tmdb_production_companies (
  id integer primary key,
  name text not null,
  origin_country text null,
  tmdb_logo_path text null,
  hosted_logo_key text null,
  hosted_logo_url text null,
  hosted_logo_sha256 text null,
  hosted_logo_content_type text null,
  hosted_logo_bytes bigint null,
  hosted_logo_etag text null,
  hosted_logo_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists tmdb_production_companies_name_idx
  on core.tmdb_production_companies (name);

-- Maintain updated_at automatically on updates.
drop trigger if exists core_tmdb_production_companies_set_updated_at on core.tmdb_production_companies;
create trigger core_tmdb_production_companies_set_updated_at
before update on core.tmdb_production_companies
for each row
execute function core.set_updated_at();

alter table core.tmdb_production_companies enable row level security;

drop policy if exists core_tmdb_production_companies_public_read on core.tmdb_production_companies;
create policy core_tmdb_production_companies_public_read on core.tmdb_production_companies
for select
using (true);

drop policy if exists core_tmdb_production_companies_service_role on core.tmdb_production_companies;
create policy core_tmdb_production_companies_service_role on core.tmdb_production_companies
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

grant select on table core.tmdb_production_companies to anon, authenticated;
grant all privileges on table core.tmdb_production_companies to service_role;

-- ---------------------------------------------------------------------------
-- core.tmdb_watch_providers
-- ---------------------------------------------------------------------------

create table if not exists core.tmdb_watch_providers (
  provider_id integer primary key,
  provider_name text not null,
  display_priority integer null,
  tmdb_logo_path text null,
  hosted_logo_key text null,
  hosted_logo_url text null,
  hosted_logo_sha256 text null,
  hosted_logo_content_type text null,
  hosted_logo_bytes bigint null,
  hosted_logo_etag text null,
  hosted_logo_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists tmdb_watch_providers_name_idx
  on core.tmdb_watch_providers (provider_name);

-- Maintain updated_at automatically on updates.
drop trigger if exists core_tmdb_watch_providers_set_updated_at on core.tmdb_watch_providers;
create trigger core_tmdb_watch_providers_set_updated_at
before update on core.tmdb_watch_providers
for each row
execute function core.set_updated_at();

alter table core.tmdb_watch_providers enable row level security;

drop policy if exists core_tmdb_watch_providers_public_read on core.tmdb_watch_providers;
create policy core_tmdb_watch_providers_public_read on core.tmdb_watch_providers
for select
using (true);

drop policy if exists core_tmdb_watch_providers_service_role on core.tmdb_watch_providers;
create policy core_tmdb_watch_providers_service_role on core.tmdb_watch_providers
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

grant select on table core.tmdb_watch_providers to anon, authenticated;
grant all privileges on table core.tmdb_watch_providers to service_role;

-- ---------------------------------------------------------------------------
-- core.show_watch_providers
-- ---------------------------------------------------------------------------

create table if not exists core.show_watch_providers (
  show_id uuid not null references core.shows (id) on delete cascade,
  region text not null,
  offer_type text not null,
  provider_id integer not null references core.tmdb_watch_providers (provider_id) on delete cascade,
  display_priority integer null,
  link text null,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (show_id, region, offer_type, provider_id)
);

create index if not exists show_watch_providers_show_id_idx
  on core.show_watch_providers (show_id);
create index if not exists show_watch_providers_provider_id_idx
  on core.show_watch_providers (provider_id);
create index if not exists show_watch_providers_region_idx
  on core.show_watch_providers (region);
create index if not exists show_watch_providers_offer_type_idx
  on core.show_watch_providers (offer_type);

-- Maintain updated_at automatically on updates.
drop trigger if exists core_show_watch_providers_set_updated_at on core.show_watch_providers;
create trigger core_show_watch_providers_set_updated_at
before update on core.show_watch_providers
for each row
execute function core.set_updated_at();

alter table core.show_watch_providers enable row level security;

drop policy if exists core_show_watch_providers_public_read on core.show_watch_providers;
create policy core_show_watch_providers_public_read on core.show_watch_providers
for select
using (true);

drop policy if exists core_show_watch_providers_service_role on core.show_watch_providers;
create policy core_show_watch_providers_service_role on core.show_watch_providers
for all
using (auth.role() = 'service_role')
with check (auth.role() = 'service_role');

grant select on table core.show_watch_providers to anon, authenticated;
grant all privileges on table core.show_watch_providers to service_role;

commit;
