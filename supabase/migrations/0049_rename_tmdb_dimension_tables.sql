begin;

-- ---------------------------------------------------------------------------
-- Rename TMDb dimension tables to canonical names
-- ---------------------------------------------------------------------------

do $$
begin
  if to_regclass('core.tmdb_networks') is not null then
    alter table core.tmdb_networks rename to networks;
  end if;
  if to_regclass('core.tmdb_production_companies') is not null then
    alter table core.tmdb_production_companies rename to production_companies;
  end if;
  if to_regclass('core.tmdb_watch_providers') is not null then
    alter table core.tmdb_watch_providers rename to watch_providers;
  end if;
end $$;

-- ---------------------------------------------------------------------------
-- Ensure canonical tables are multi-source capable
-- ---------------------------------------------------------------------------

do $$
begin
  if to_regclass('core.networks') is not null then
    alter table core.networks add column if not exists logo_path text null;
    alter table core.networks add column if not exists tmdb_meta jsonb null;
    alter table core.networks add column if not exists tmdb_fetched_at timestamptz null;
    alter table core.networks add column if not exists imdb_meta jsonb null;
    alter table core.networks add column if not exists imdb_fetched_at timestamptz null;
  end if;

  if to_regclass('core.production_companies') is not null then
    alter table core.production_companies add column if not exists logo_path text null;
    alter table core.production_companies add column if not exists tmdb_meta jsonb null;
    alter table core.production_companies add column if not exists tmdb_fetched_at timestamptz null;
    alter table core.production_companies add column if not exists imdb_meta jsonb null;
    alter table core.production_companies add column if not exists imdb_fetched_at timestamptz null;
  end if;

  if to_regclass('core.watch_providers') is not null then
    alter table core.watch_providers add column if not exists logo_path text null;
    alter table core.watch_providers add column if not exists tmdb_meta jsonb null;
    alter table core.watch_providers add column if not exists tmdb_fetched_at timestamptz null;
    alter table core.watch_providers add column if not exists imdb_meta jsonb null;
    alter table core.watch_providers add column if not exists imdb_fetched_at timestamptz null;
  end if;
end $$;

-- Backfill canonical logo_path from existing hosted keys.
update core.networks set logo_path = hosted_logo_key where logo_path is null and hosted_logo_key is not null;
update core.production_companies set logo_path = hosted_logo_key where logo_path is null and hosted_logo_key is not null;
update core.watch_providers set logo_path = hosted_logo_key where logo_path is null and hosted_logo_key is not null;

commit;
