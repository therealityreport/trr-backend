alter table if exists screenalytics.video_assets
  add column if not exists video_class text not null default 'episode',
  add column if not exists promo_subtype text null,
  add column if not exists source_import_type text not null default 'upload';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_video_assets_video_class_check'
      and conrelid = 'screenalytics.video_assets'::regclass
  ) then
    alter table screenalytics.video_assets
      add constraint screenalytics_video_assets_video_class_check
      check (video_class in ('episode', 'promo'));
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_video_assets_promo_subtype_check'
      and conrelid = 'screenalytics.video_assets'::regclass
  ) then
    alter table screenalytics.video_assets
      add constraint screenalytics_video_assets_promo_subtype_check
      check (
        promo_subtype is null
        or promo_subtype in ('trailer', 'episode_teaser')
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_video_assets_source_import_type_check'
      and conrelid = 'screenalytics.video_assets'::regclass
  ) then
    alter table screenalytics.video_assets
      add constraint screenalytics_video_assets_source_import_type_check
      check (
        source_import_type in (
          'upload',
          'youtube_url_import',
          'social_youtube_import',
          'external_url_import'
        )
      );
  end if;
end $$;

alter table if exists screenalytics.media_upload_sessions
  add column if not exists video_class text not null default 'episode',
  add column if not exists promo_subtype text null,
  add column if not exists source_import_type text not null default 'upload',
  add column if not exists owner_scope text not null default 'season';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_media_upload_sessions_video_class_check'
      and conrelid = 'screenalytics.media_upload_sessions'::regclass
  ) then
    alter table screenalytics.media_upload_sessions
      add constraint screenalytics_media_upload_sessions_video_class_check
      check (video_class in ('episode', 'promo'));
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_media_upload_sessions_promo_subtype_check'
      and conrelid = 'screenalytics.media_upload_sessions'::regclass
  ) then
    alter table screenalytics.media_upload_sessions
      add constraint screenalytics_media_upload_sessions_promo_subtype_check
      check (
        promo_subtype is null
        or promo_subtype in ('trailer', 'episode_teaser')
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_media_upload_sessions_source_import_type_check'
      and conrelid = 'screenalytics.media_upload_sessions'::regclass
  ) then
    alter table screenalytics.media_upload_sessions
      add constraint screenalytics_media_upload_sessions_source_import_type_check
      check (
        source_import_type in (
          'upload',
          'youtube_url_import',
          'social_youtube_import',
          'external_url_import'
        )
      );
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_media_upload_sessions_owner_scope_check'
      and conrelid = 'screenalytics.media_upload_sessions'::regclass
  ) then
    alter table screenalytics.media_upload_sessions
      add constraint screenalytics_media_upload_sessions_owner_scope_check
      check (owner_scope in ('show', 'season', 'episode'));
  end if;
end $$;

create index if not exists screenalytics_video_assets_video_class_idx
  on screenalytics.video_assets (video_class);

create index if not exists screenalytics_video_assets_show_video_class_idx
  on screenalytics.video_assets (show_id, video_class);
