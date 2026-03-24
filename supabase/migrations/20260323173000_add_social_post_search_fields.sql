begin;

create extension if not exists pg_trgm with schema extensions;

create or replace function social._search_unique_text_array(input_values text[])
returns text[]
language sql
immutable
as $$
  select coalesce(array_agg(value order by value), array[]::text[])
  from (
    select distinct lower(btrim(item)) as value
    from unnest(coalesce(input_values, array[]::text[])) as raw(item)
    where nullif(btrim(item), '') is not null
  ) normalized
$$;

create or replace function social._normalize_search_handle(value text)
returns text
language sql
immutable
as $$
  select nullif(
    regexp_replace(
      lower(
        split_part(
          split_part(
            split_part(
              regexp_replace(regexp_replace(coalesce(value, ''), '^@+', ''), '^https?://', '', 'i'),
              '?',
              1
            ),
            '#',
            1
          ),
          '/',
          1
        )
      ),
      '[^a-z0-9._-]+',
      '',
      'g'
    ),
    ''
  )
$$;

create or replace function social._normalize_search_hashtag(value text)
returns text
language sql
immutable
as $$
  select nullif(
    regexp_replace(
      lower(regexp_replace(coalesce(value, ''), '^#+', '')),
      '[^a-z0-9]+',
      '',
      'g'
    ),
    ''
  )
$$;

create or replace function social._search_identity(value text)
returns text
language sql
immutable
as $$
  select nullif(regexp_replace(lower(coalesce(value, '')), '[^a-z0-9]+', '', 'g'), '')
$$;

create or replace function social._jsonb_text_values(payload jsonb)
returns text[]
language sql
immutable
as $$
  select coalesce(array_agg(value order by value), array[]::text[])
  from (
    select btrim(item.value) as value
    from jsonb_array_elements_text(coalesce(payload, '[]'::jsonb)) as item(value)
    where nullif(btrim(item.value), '') is not null
  ) normalized
$$;

create or replace function social._jsonb_detail_handle_values(payload jsonb)
returns text[]
language sql
immutable
as $$
  select coalesce(array_agg(value order by value), array[]::text[])
  from (
    select social._normalize_search_handle(
      coalesce(
        detail.value ->> 'username',
        detail.value ->> 'user_name',
        detail.value ->> 'userName',
        detail.value ->> 'screen_name',
        detail.value ->> 'screenName',
        detail.value ->> 'handle',
        detail.value ->> 'author',
        detail.value ->> 'source_account',
        detail.value -> 'user' ->> 'username',
        detail.value -> 'user' ->> 'user_name',
        detail.value -> 'user' ->> 'userName',
        detail.value -> 'user' ->> 'screen_name',
        detail.value -> 'user' ->> 'screenName',
        detail.value -> 'user' ->> 'handle'
      )
    ) as value
    from jsonb_array_elements(coalesce(payload, '[]'::jsonb)) as detail(value)
  ) normalized
  where value is not null
$$;

create or replace function social._regex_capture_values(source text, pattern text, capture_index int)
returns text[]
language sql
immutable
as $$
  select coalesce(array_agg(match[capture_index] order by match[capture_index]), array[]::text[])
  from regexp_matches(coalesce(source, ''), pattern, 'g') as match
$$;

create or replace function social._build_post_search_text(platform text, post_json jsonb)
returns text
language sql
immutable
as $$
  select lower(
    trim(
      both ' ' from concat_ws(
        ' ',
        case
          when platform = 'youtube' then coalesce(post_json ->> 'title', '')
          else ''
        end,
        case
          when platform = 'instagram' then coalesce(post_json ->> 'caption', '')
          when platform = 'tiktok' then coalesce(
            post_json ->> 'description',
            post_json -> 'raw_data' ->> 'description',
            post_json -> 'raw_data' ->> 'caption',
            post_json -> 'raw_data' ->> 'text',
            ''
          )
          when platform = 'twitter' then coalesce(post_json ->> 'text', '')
          when platform = 'youtube' then concat_ws(
            ' ',
            coalesce(post_json ->> 'description', ''),
            coalesce(post_json ->> 'transcript_text', '')
          )
          when platform = 'facebook' then coalesce(
            post_json ->> 'caption',
            post_json -> 'raw_data' ->> 'text',
            post_json -> 'raw_data' ->> 'message',
            ''
          )
          when platform = 'threads' then coalesce(
            post_json ->> 'text',
            post_json -> 'raw_data' ->> 'caption',
            ''
          )
          else ''
        end,
        case
          when platform = 'instagram' and nullif(post_json ->> 'shortcode', '') is not null
            then 'https://www.instagram.com/p/' || (post_json ->> 'shortcode') || '/'
          when platform = 'tiktok'
            and nullif(post_json ->> 'video_id', '') is not null
            and nullif(lower(post_json ->> 'source_account'), '') is not null
            then 'https://www.tiktok.com/@' || lower(post_json ->> 'source_account') || '/video/' || (post_json ->> 'video_id')
          when platform = 'twitter'
            and nullif(post_json ->> 'tweet_id', '') is not null
            and nullif(lower(post_json ->> 'source_account'), '') is not null
            then 'https://x.com/' || lower(post_json ->> 'source_account') || '/status/' || (post_json ->> 'tweet_id')
          when platform = 'youtube' and nullif(post_json ->> 'video_id', '') is not null
            then 'https://www.youtube.com/watch?v=' || (post_json ->> 'video_id')
          when platform = 'facebook'
            and nullif(post_json ->> 'post_id', '') is not null
            and nullif(lower(post_json ->> 'source_account'), '') is not null
            then 'https://www.facebook.com/' || lower(post_json ->> 'source_account') || '/posts/' || (post_json ->> 'post_id')
          when platform = 'threads'
            and nullif(post_json ->> 'post_id', '') is not null
            and nullif(lower(post_json ->> 'source_account'), '') is not null
            then 'https://www.threads.com/@' || lower(post_json ->> 'source_account') || '/post/' || (post_json ->> 'post_id')
          else ''
        end
      )
    )
  )
$$;

create or replace function social._build_post_search_hashtags(platform text, post_json jsonb, token_text text)
returns text[]
language sql
immutable
as $$
  with raw_values as (
    select unnest(social._jsonb_text_values(post_json -> 'hashtags')) as value
    union all
    select unnest(
      case
        when platform = 'youtube' then social._jsonb_text_values(post_json -> 'tags')
        else array[]::text[]
      end
    ) as value
    union all
    select unnest(
      social._regex_capture_values(
        token_text,
        '(^|[^[:alnum:]_#])#([A-Za-z0-9_]+)',
        2
      )
    ) as value
  )
  select social._search_unique_text_array(
    array_agg(social._normalize_search_hashtag(value))
  )
  from raw_values
$$;

create or replace function social._build_post_search_handles(post_json jsonb, token_text text)
returns text[]
language sql
immutable
as $$
  with raw_values as (
    select unnest(social._jsonb_text_values(post_json -> 'mentions')) as value
    union all
    select unnest(social._jsonb_text_values(post_json -> 'collaborators')) as value
    union all
    select unnest(social._jsonb_text_values(post_json -> 'profile_tags')) as value
    union all
    select unnest(social._jsonb_detail_handle_values(post_json -> 'collaborators_detail')) as value
    union all
    select unnest(
      social._regex_capture_values(
        token_text,
        '(^|[^[:alnum:]_.])@([A-Za-z0-9_.]+)',
        2
      )
    ) as value
  )
  select social._search_unique_text_array(
    array_agg(social._normalize_search_handle(value))
  )
  from raw_values
$$;

create or replace function social._build_post_search_handle_identities(
  post_json jsonb,
  token_text text,
  search_hashtags text[],
  search_handles text[]
)
returns text[]
language sql
immutable
as $$
  with raw_values as (
    select unnest(coalesce(search_handles, array[]::text[])) as value
    union all
    select unnest(coalesce(search_hashtags, array[]::text[])) as value
    union all
    select unnest(
      social._regex_capture_values(
        token_text,
        '(^|[^[:alnum:]_.])@([A-Za-z0-9_.]+(?:\s+[A-Z][A-Za-z0-9_.]*){0,3})',
        2
      )
    ) as value
  )
  select social._search_unique_text_array(
    array_agg(social._search_identity(value))
  )
  from raw_values
$$;

commit;
