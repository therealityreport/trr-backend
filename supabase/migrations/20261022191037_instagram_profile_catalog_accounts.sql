-- Exact catalog ownership identities, computed once on source writes rather than
-- by expanding every row's JSON for every uncached profile summary.
begin;
set local lock_timeout = '5s';

create or replace function social.instagram_profile_catalog_accounts(
  _source_account text, _owner_username text, _collaborators jsonb, _raw_data jsonb
) returns jsonb
language sql immutable parallel safe
set search_path = pg_catalog
as $function$
  select coalesce(jsonb_agg(distinct handle order by handle) filter (where handle is not null), '[]'::jsonb)
  from (
    select nullif(ltrim(lower(trim(coalesce(identity, ''))), '@'), '') as handle
    from unnest(array[_source_account, _owner_username,
      _raw_data ->> 'source_account', _raw_data ->> 'owner_username', _raw_data ->> 'username'
    ]) as direct(identity)
    union all
    select nullif(ltrim(lower(trim(coalesce(
      case when jsonb_typeof(value) = 'object' then
        coalesce(value ->> 'username', value ->> 'handle', value ->> 'user_name', value ->> 'name')
      when jsonb_typeof(value) = 'string' then value #>> '{}'
      else trim(both '"' from value::text) end, ''))), '@'), '') as handle
    from (values (_collaborators), (_raw_data -> 'collaborators'),
      (_raw_data -> 'collaborators_detail')) as paths(path)
    cross join lateral jsonb_array_elements(case
      when jsonb_typeof(path) = 'array' then path
      when jsonb_typeof(path) in ('object', 'string') then jsonb_build_array(path)
      else '[]'::jsonb end) as items(value)
  ) as identities;
$function$;

create or replace function social.refresh_instagram_profile_catalog_accounts()
returns trigger language plpgsql
set search_path = pg_catalog
as $function$
begin
  new.profile_summary_accounts := social.instagram_profile_catalog_accounts(
    new.source_account, new.owner_username, new.collaborators, new.raw_data
  );
  return new;
end;
$function$;

alter table social.instagram_account_catalog_posts add column if not exists profile_summary_accounts jsonb;
alter table social.instagram_account_catalog_posts alter column profile_summary_accounts set storage main;
comment on column social.instagram_account_catalog_posts.profile_summary_accounts is
  'Trigger-maintained exact owner/coauthor identities for profile summaries. NULL uses the legacy predicate until backfilled.';
drop trigger if exists trg_instagram_profile_catalog_accounts on social.instagram_account_catalog_posts;
create trigger trg_instagram_profile_catalog_accounts
before insert or update of source_account, owner_username, collaborators, raw_data, profile_summary_accounts
on social.instagram_account_catalog_posts
for each row execute function social.refresh_instagram_profile_catalog_accounts();

create index if not exists instagram_catalog_profile_accounts_gin
  -- jsonb_ops hashes oversized malformed legacy identities and rechecks exact
  -- membership; text[] GIN would reject those otherwise valid source writes.
  on social.instagram_account_catalog_posts using gin (profile_summary_accounts jsonb_ops);
create index if not exists instagram_catalog_profile_accounts_unfilled_idx
  on social.instagram_account_catalog_posts (id) where profile_summary_accounts is null;
-- Backfill NULL rows in bounded batches, then ANALYZE before deploying readers.
commit;
