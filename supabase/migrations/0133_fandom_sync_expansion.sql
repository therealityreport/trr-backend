begin;

alter table core.cast_fandom
  add column if not exists dynamic_sections jsonb,
  add column if not exists bio_card jsonb,
  add column if not exists casting_summary text,
  add column if not exists citations jsonb,
  add column if not exists conflicts jsonb,
  add column if not exists source_variants jsonb,
  add column if not exists ai_model text,
  add column if not exists ai_generated_at timestamptz;

create table if not exists core.season_fandom (
  id uuid primary key default gen_random_uuid(),
  season_id uuid not null references core.seasons (id) on delete cascade,
  show_id uuid not null references core.shows (id) on delete cascade,
  season_number integer not null,
  source text not null,
  source_url text not null,
  page_title text,
  page_revision_id bigint,
  scraped_at timestamptz not null default now(),
  summary text,
  dynamic_sections jsonb,
  citations jsonb,
  conflicts jsonb,
  source_variants jsonb,
  ai_model text,
  ai_generated_at timestamptz,
  raw_html_sha256 text
);

create unique index if not exists season_fandom_season_source_key
  on core.season_fandom (season_id, source);

create index if not exists core_season_fandom_show_id_idx
  on core.season_fandom (show_id);

create index if not exists core_season_fandom_season_number_idx
  on core.season_fandom (season_number);

grant select on table core.season_fandom to anon, authenticated;
grant all privileges on table core.season_fandom to service_role;

commit;
