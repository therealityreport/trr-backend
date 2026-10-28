-- Per-post scalar header values. Original source fields remain authoritative.
-- Backfill NULL rows in bounded batches before restarting profile readers.
begin;
set local lock_timeout = '5s';

create or replace function social.instagram_profile_header_values(
  _source_account text, _owner_username text, _username text,
  _collaborators jsonb, _collaborators_detail jsonb, _raw_data jsonb,
  _comments_count bigint, _fb_comment_count bigint, _media_urls jsonb, _hosted_media_urls jsonb
) returns jsonb
language plpgsql immutable parallel safe
set search_path = pg_catalog
as $function$
declare
  handles text[];
begin
  select coalesce(array_agg(distinct handle order by handle) filter (where handle is not null), array[]::text[])
  into handles
  from (
select unnest(array[
  nullif(ltrim(lower(trim(coalesce(_source_account, ''))), '@'), ''),
  nullif(ltrim(lower(trim(coalesce(_owner_username, ''))), '@'), ''),
  nullif(ltrim(lower(trim(coalesce(_username, ''))), '@'), ''),
  nullif(ltrim(lower(trim(coalesce(_raw_data ->> 'source_account', ''))), '@'), ''),
  nullif(ltrim(lower(trim(coalesce(_raw_data ->> 'owner_username', ''))), '@'), ''),
  nullif(ltrim(lower(trim(coalesce(_raw_data ->> 'username', ''))), '@'), '')
]) as handle
union all
select nullif(ltrim(lower(trim(coalesce(
    case when jsonb_typeof(value)='object' then coalesce(value->>'username',value->>'handle',value->>'user_name',value->>'name')
         when jsonb_typeof(value)='string' then value #>> '{}'
         else trim(both '"' from value::text) end, ''))), '@'), '') as handle
  from jsonb_array_elements(case
    when jsonb_typeof(coalesce(_collaborators,'null'::jsonb))='array' then _collaborators
    when jsonb_typeof(coalesce(_collaborators,'null'::jsonb)) in ('object','string') then jsonb_build_array(_collaborators)
    else '[]'::jsonb end) as items(value)
union all
select nullif(ltrim(lower(trim(coalesce(
    case when jsonb_typeof(value)='object' then coalesce(value->>'username',value->>'handle',value->>'user_name',value->>'name')
         when jsonb_typeof(value)='string' then value #>> '{}'
         else trim(both '"' from value::text) end, ''))), '@'), '') as handle
  from jsonb_array_elements(case
    when jsonb_typeof(coalesce(_collaborators_detail,'null'::jsonb))='array' then _collaborators_detail
    when jsonb_typeof(coalesce(_collaborators_detail,'null'::jsonb)) in ('object','string') then jsonb_build_array(_collaborators_detail)
    else '[]'::jsonb end) as items(value)
union all
select nullif(ltrim(lower(trim(coalesce(
    case when jsonb_typeof(value)='object' then coalesce(value->>'username',value->>'handle',value->>'user_name',value->>'name')
         when jsonb_typeof(value)='string' then value #>> '{}'
         else trim(both '"' from value::text) end, ''))), '@'), '') as handle
  from jsonb_array_elements(case
    when jsonb_typeof(coalesce(_raw_data -> 'collaborators','null'::jsonb))='array' then _raw_data -> 'collaborators'
    when jsonb_typeof(coalesce(_raw_data -> 'collaborators','null'::jsonb)) in ('object','string') then jsonb_build_array(_raw_data -> 'collaborators')
    else '[]'::jsonb end) as items(value)
union all
select nullif(ltrim(lower(trim(coalesce(
    case when jsonb_typeof(value)='object' then coalesce(value->>'username',value->>'handle',value->>'user_name',value->>'name')
         when jsonb_typeof(value)='string' then value #>> '{}'
         else trim(both '"' from value::text) end, ''))), '@'), '') as handle
  from jsonb_array_elements(case
    when jsonb_typeof(coalesce(_raw_data -> 'collaborators_detail','null'::jsonb))='array' then _raw_data -> 'collaborators_detail'
    when jsonb_typeof(coalesce(_raw_data -> 'collaborators_detail','null'::jsonb)) in ('object','string') then jsonb_build_array(_raw_data -> 'collaborators_detail')
    else '[]'::jsonb end) as items(value)
  ) identities;
  return jsonb_build_object(
    'accounts', to_jsonb(handles),
    'reported_comments', greatest(
      coalesce(_comments_count, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) ->> 'comments_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) ->> 'comments', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) ->> 'comment_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) ->> 'commentsCount', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'edge_media_to_comment' ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'edge_media_to_parent_comment' ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'edge_media_preview_comment' ->> 'count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'media' ->> 'comments_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'media' ->> 'comments', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'media' ->> 'comment_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'media' ->> 'commentsCount', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'metrics' ->> 'comments_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'metrics' ->> 'comments', ''), '[^0-9]', '', 'g'), '')::bigint, 0), 0),
    'facebook_comments', greatest(
      coalesce(nullif(regexp_replace(coalesce(_fb_comment_count::text, ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'facebook_crosspost' ->> 'comments_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) -> 'facebook_crosspost' ->> 'fb_comment_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0),
      coalesce(nullif(regexp_replace(coalesce(coalesce(_raw_data, '{}'::jsonb) ->> 'fb_comment_count', ''), '[^0-9]', '', 'g'), '')::bigint, 0)),
    'media_count', jsonb_array_length(coalesce(_media_urls,'[]'::jsonb)),
    'hosted_media_count', jsonb_array_length(coalesce(_hosted_media_urls,'[]'::jsonb))
  );
exception
  -- An invalid legacy value must not turn a source write into a new failure.
  -- NULL preserves the reader's existing calculation/error behavior for that row.
  when invalid_text_representation or numeric_value_out_of_range or invalid_parameter_value then
    return null;
end;
$function$;

create or replace function social.refresh_instagram_profile_header()
returns trigger language plpgsql
set search_path = pg_catalog
as $function$
begin
  new.profile_summary_header := social.instagram_profile_header_values(
    new.source_account,new.owner_username,new.username,new.collaborators,new.collaborators_detail,new.raw_data,
    new.comments_count,new.fb_comment_count,new.media_urls,new.hosted_media_urls
  );
  return new;
end;
$function$;

alter table social.instagram_posts add column if not exists profile_summary_header jsonb;
-- Prefer keeping this small value inline; oversized identity lists may still toast.
alter table social.instagram_posts alter column profile_summary_header set storage main;
comment on column social.instagram_posts.profile_summary_header is
  'Trigger-maintained exact account identities and scalar counts for uncached profile headers. NULL uses legacy fallback.';
drop trigger if exists trg_instagram_profile_header on social.instagram_posts;
create trigger trg_instagram_profile_header
before insert or update of source_account,owner_username,username,collaborators,collaborators_detail,
  raw_data,comments_count,fb_comment_count,media_urls,hosted_media_urls,profile_summary_header
on social.instagram_posts
for each row execute function social.refresh_instagram_profile_header();
commit;
