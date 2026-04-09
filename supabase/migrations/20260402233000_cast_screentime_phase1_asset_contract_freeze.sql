begin;

alter table if exists ml.analysis_media_assets
  add column if not exists legacy_screenalytics_video_asset_id uuid null;

create unique index if not exists ml_analysis_media_assets_legacy_screenalytics_video_asset_uidx
  on ml.analysis_media_assets (legacy_screenalytics_video_asset_id)
  where legacy_screenalytics_video_asset_id is not null;

do $$
begin
  if to_regclass('screenalytics.video_assets') is null then
    raise notice 'screenalytics.video_assets not present; skipping cast screentime Phase 1 legacy asset backfill';
    return;
  end if;

  with legacy_assets as (
    select
      sva.id as legacy_screenalytics_video_asset_id,
      sva.episode_id,
      sva.season_id,
      sva.show_id,
      sva.media_asset_id,
      sva.source_url,
      sva.source_json,
      sva.duration_seconds,
      sva.metadata,
      coalesce(nullif(trim(sva.video_class), ''), 'episode') as video_class,
      nullif(trim(sva.promo_subtype), '') as promo_subtype,
      coalesce(nullif(trim(sva.source_import_type), ''), 'upload') as source_import_type,
      sva.created_at,
      sva.updated_at,
      case
        when coalesce(nullif(trim(sva.video_class), ''), 'episode') = 'episode' then 'episode'
        when nullif(trim(sva.promo_subtype), '') = 'trailer' then 'trailer'
        else 'extras'
      end as media_type,
      case
        when nullif(trim(sva.promo_subtype), '') = 'episode_teaser' then 'episode_teaser'
        else null
      end as media_kind
    from screenalytics.video_assets sva
  ),
  missing_assets as (
    select la.*
    from legacy_assets la
    left join ml.analysis_media_assets ma
      on ma.legacy_screenalytics_video_asset_id = la.legacy_screenalytics_video_asset_id
    where ma.id is null
  )
  insert into ml.analysis_media_assets (
    episode_id,
    season_id,
    show_id,
    media_asset_id,
    legacy_screenalytics_video_asset_id,
    source_url,
    source_json,
    duration_seconds,
    metadata,
    video_class,
    promo_subtype,
    media_type,
    media_kind,
    source_import_type,
    created_at,
    updated_at
  )
  select
    ma.episode_id,
    ma.season_id,
    ma.show_id,
    ma.media_asset_id,
    ma.legacy_screenalytics_video_asset_id,
    ma.source_url,
    coalesce(ma.source_json, '{}'::jsonb) ||
      jsonb_build_object(
        'legacy_bridge',
        jsonb_build_object(
          'source_table', 'screenalytics.video_assets',
          'legacy_video_asset_id', ma.legacy_screenalytics_video_asset_id::text
        )
      ),
    ma.duration_seconds,
    coalesce(ma.metadata, '{}'::jsonb) ||
      jsonb_build_object(
        'legacy_bridge',
        jsonb_build_object(
          'source_table', 'screenalytics.video_assets',
          'legacy_video_asset_id', ma.legacy_screenalytics_video_asset_id::text
        )
      ),
    ma.video_class,
    ma.promo_subtype,
    ma.media_type,
    ma.media_kind,
    ma.source_import_type,
    ma.created_at,
    ma.updated_at
  from missing_assets ma;

  with legacy_assets as (
    select
      sva.id as legacy_screenalytics_video_asset_id,
      sva.episode_id,
      sva.season_id,
      sva.show_id,
      sva.media_asset_id,
      sva.source_url,
      sva.source_json,
      sva.duration_seconds,
      sva.metadata,
      coalesce(nullif(trim(sva.video_class), ''), 'episode') as video_class,
      nullif(trim(sva.promo_subtype), '') as promo_subtype,
      coalesce(nullif(trim(sva.source_import_type), ''), 'upload') as source_import_type,
      sva.updated_at,
      case
        when coalesce(nullif(trim(sva.video_class), ''), 'episode') = 'episode' then 'episode'
        when nullif(trim(sva.promo_subtype), '') = 'trailer' then 'trailer'
        else 'extras'
      end as media_type,
      case
        when nullif(trim(sva.promo_subtype), '') = 'episode_teaser' then 'episode_teaser'
        else null
      end as media_kind
    from screenalytics.video_assets sva
  )
  update ml.analysis_media_assets ma
  set
    episode_id = coalesce(ma.episode_id, la.episode_id),
    season_id = coalesce(ma.season_id, la.season_id),
    show_id = coalesce(ma.show_id, la.show_id),
    media_asset_id = coalesce(ma.media_asset_id, la.media_asset_id),
    source_url = coalesce(ma.source_url, la.source_url),
    source_json = coalesce(ma.source_json, '{}'::jsonb) ||
      coalesce(la.source_json, '{}'::jsonb) ||
      jsonb_build_object(
        'legacy_bridge',
        jsonb_build_object(
          'source_table', 'screenalytics.video_assets',
          'legacy_video_asset_id', la.legacy_screenalytics_video_asset_id::text
        )
      ),
    duration_seconds = coalesce(ma.duration_seconds, la.duration_seconds),
    metadata = coalesce(ma.metadata, '{}'::jsonb) ||
      coalesce(la.metadata, '{}'::jsonb) ||
      jsonb_build_object(
        'legacy_bridge',
        jsonb_build_object(
          'source_table', 'screenalytics.video_assets',
          'legacy_video_asset_id', la.legacy_screenalytics_video_asset_id::text
        )
      ),
    video_class = coalesce(nullif(trim(ma.video_class), ''), la.video_class),
    promo_subtype = coalesce(nullif(trim(ma.promo_subtype), ''), la.promo_subtype),
    media_type = coalesce(nullif(trim(ma.media_type), ''), la.media_type),
    media_kind = coalesce(nullif(trim(ma.media_kind), ''), la.media_kind),
    source_import_type = coalesce(nullif(trim(ma.source_import_type), ''), la.source_import_type),
    updated_at = coalesce(greatest(ma.updated_at, la.updated_at), ma.updated_at, la.updated_at, now())
  from legacy_assets la
  where ma.legacy_screenalytics_video_asset_id = la.legacy_screenalytics_video_asset_id;
end
$$;

commit;
