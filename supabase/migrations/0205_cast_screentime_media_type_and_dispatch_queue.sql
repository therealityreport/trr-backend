alter table if exists screenalytics.video_assets
  add column if not exists media_type text,
  add column if not exists media_kind text;

update screenalytics.video_assets
set media_type = case
    when coalesce(video_class, 'episode') = 'episode' then 'episode'
    when coalesce(promo_subtype, '') = 'trailer' then 'trailer'
    else 'extras'
  end,
  media_kind = case
    when coalesce(video_class, '') = 'promo' and coalesce(promo_subtype, '') = 'episode_teaser'
      then coalesce(media_kind, 'episode_teaser')
    else media_kind
  end
where media_type is null;

alter table if exists screenalytics.video_assets
  alter column media_type set default 'episode';

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'screenalytics'
      and table_name = 'video_assets'
      and column_name = 'media_type'
  ) then
    update screenalytics.video_assets
    set media_type = 'episode'
    where media_type is null;
    alter table screenalytics.video_assets
      alter column media_type set not null;
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_video_assets_media_type_check'
      and conrelid = 'screenalytics.video_assets'::regclass
  ) then
    alter table screenalytics.video_assets
      add constraint screenalytics_video_assets_media_type_check
      check (media_type in ('episode', 'trailer', 'extras'));
  end if;
end
$$;

create index if not exists screenalytics_video_assets_media_type_idx
  on screenalytics.video_assets (media_type);

create index if not exists screenalytics_video_assets_show_media_type_idx
  on screenalytics.video_assets (show_id, media_type);

alter table if exists screenalytics.media_upload_sessions
  add column if not exists media_type text,
  add column if not exists media_kind text;

update screenalytics.media_upload_sessions
set media_type = case
    when coalesce(video_class, 'episode') = 'episode' then 'episode'
    when coalesce(promo_subtype, '') = 'trailer' then 'trailer'
    else 'extras'
  end,
  media_kind = case
    when coalesce(video_class, '') = 'promo' and coalesce(promo_subtype, '') = 'episode_teaser'
      then coalesce(media_kind, 'episode_teaser')
    else media_kind
  end
where media_type is null;

alter table if exists screenalytics.media_upload_sessions
  alter column media_type set default 'episode';

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'screenalytics'
      and table_name = 'media_upload_sessions'
      and column_name = 'media_type'
  ) then
    update screenalytics.media_upload_sessions
    set media_type = 'episode'
    where media_type is null;
    alter table screenalytics.media_upload_sessions
      alter column media_type set not null;
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'screenalytics_media_upload_sessions_media_type_check'
      and conrelid = 'screenalytics.media_upload_sessions'::regclass
  ) then
    alter table screenalytics.media_upload_sessions
      add constraint screenalytics_media_upload_sessions_media_type_check
      check (media_type in ('episode', 'trailer', 'extras'));
  end if;
end
$$;

alter table if exists screenalytics.runs_v2
  add column if not exists candidate_scope_policy_json jsonb not null default '{}'::jsonb,
  add column if not exists cast_coverage_summary_json jsonb not null default '{}'::jsonb,
  add column if not exists dispatch_status text,
  add column if not exists dispatch_job_id text,
  add column if not exists dispatch_accepted_at timestamptz;

create index if not exists screenalytics_runs_v2_dispatch_status_idx
  on screenalytics.runs_v2 (dispatch_status);
