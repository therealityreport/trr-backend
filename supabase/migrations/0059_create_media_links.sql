begin;

-- ---------------------------------------------------------------------------
-- core.media_links - Polymorphic links between entities and media assets
-- ---------------------------------------------------------------------------

create table if not exists core.media_links (
  id uuid primary key default gen_random_uuid(),

  -- Entity reference (polymorphic)
  entity_type text not null,
  entity_id uuid not null,

  -- Media reference
  media_asset_id uuid not null references core.media_assets(id) on delete cascade,

  -- Context
  kind text not null,
  position integer null,
  is_primary boolean not null default false,
  context jsonb not null default '{}'::jsonb,

  -- Timestamps
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Indexes
create index if not exists media_links_entity_idx
  on core.media_links (entity_type, entity_id);

create index if not exists media_links_media_asset_idx
  on core.media_links (media_asset_id);

create index if not exists media_links_kind_idx
  on core.media_links (entity_type, entity_id, kind);

create index if not exists media_links_kind_position_idx
  on core.media_links (entity_type, entity_id, kind, position);

-- Enforce one primary per (entity_type, entity_id, kind)
create unique index if not exists media_links_one_primary_per_entity_kind
  on core.media_links (entity_type, entity_id, kind)
  where is_primary = true;

-- updated_at trigger
create trigger core_media_links_set_updated_at
before update on core.media_links
for each row
execute function core.set_updated_at();

commit;
