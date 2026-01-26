begin;

create unique index if not exists media_assets_source_hosted_sha_uq
  on core.media_assets(source, hosted_sha256)
  where hosted_sha256 is not null;

create unique index if not exists media_assets_source_asset_id_unique
  on core.media_assets (source, source_asset_id)
  where source_asset_id is not null;

create unique index if not exists media_assets_source_url_unique
  on core.media_assets (source, source_url)
  where source_url is not null;

create unique index if not exists media_links_entity_kind_asset_uq
  on core.media_links(entity_type, entity_id, kind, media_asset_id);

create unique index if not exists media_links_one_primary_uq
  on core.media_links(entity_type, entity_id, kind)
  where is_primary = true;

commit;
