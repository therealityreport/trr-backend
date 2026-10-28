-- Additive summary projection. Raw data remains authoritative and unchanged.
-- Backfill existing rows in bounded batches after applying this migration.
-- Readers use raw_data while a row is unbackfilled; old releases ignore this column.
begin;
set local lock_timeout = '5s';

create or replace function social.instagram_profile_summary_raw(payload jsonb)
returns jsonb
language sql immutable parallel safe
set search_path = pg_catalog
as $function$
  -- Preserve copied values verbatim, including nulls inside malformed count objects.
  select jsonb_build_object(
    'source_account', payload -> 'source_account',
    'owner_username', payload -> 'owner_username',
    'username', payload -> 'username',
    'collaborators', payload -> 'collaborators',
    'collaborators_detail', payload -> 'collaborators_detail',
    'comments_count', payload -> 'comments_count',
    'comments', payload -> 'comments',
    'comment_count', payload -> 'comment_count',
    'commentsCount', payload -> 'commentsCount',
    'fb_comment_count', payload -> 'fb_comment_count',
    'edge_media_to_comment', jsonb_build_object('count', payload -> 'edge_media_to_comment' -> 'count'),
    'edge_media_to_parent_comment', jsonb_build_object('count', payload -> 'edge_media_to_parent_comment' -> 'count'),
    'edge_media_preview_comment', jsonb_build_object('count', payload -> 'edge_media_preview_comment' -> 'count'),
    'media', jsonb_build_object('comments_count', payload -> 'media' -> 'comments_count', 'comments', payload -> 'media' -> 'comments', 'comment_count', payload -> 'media' -> 'comment_count', 'commentsCount', payload -> 'media' -> 'commentsCount'),
    'metrics', jsonb_build_object('comments_count', payload -> 'metrics' -> 'comments_count', 'comments', payload -> 'metrics' -> 'comments'),
    'facebook_crosspost', jsonb_build_object('comments_count', payload -> 'facebook_crosspost' -> 'comments_count', 'fb_comment_count', payload -> 'facebook_crosspost' -> 'fb_comment_count')
  );
$function$;

create or replace function social.refresh_instagram_profile_summary_raw()
returns trigger
language plpgsql
set search_path = pg_catalog
as $function$
begin
  new.profile_summary_raw_data := social.instagram_profile_summary_raw(new.raw_data);
  return new;
end;
$function$;

alter table social.instagram_posts
  add column if not exists profile_summary_raw_data jsonb;
comment on column social.instagram_posts.profile_summary_raw_data is
  'Derived raw-data fields for profile summary reads; maintained by trigger. NULL means awaiting backfill.';
drop trigger if exists trg_instagram_profile_summary_raw on social.instagram_posts;
create trigger trg_instagram_profile_summary_raw
before insert or update of raw_data, profile_summary_raw_data on social.instagram_posts
for each row execute function social.refresh_instagram_profile_summary_raw();

alter table social.instagram_account_catalog_posts
  add column if not exists profile_summary_raw_data jsonb;
comment on column social.instagram_account_catalog_posts.profile_summary_raw_data is
  'Derived raw-data fields for profile summary reads; maintained by trigger. NULL means awaiting backfill.';
drop trigger if exists trg_instagram_profile_summary_raw on social.instagram_account_catalog_posts;
create trigger trg_instagram_profile_summary_raw
before insert or update of raw_data, profile_summary_raw_data on social.instagram_account_catalog_posts
for each row execute function social.refresh_instagram_profile_summary_raw();

commit;
