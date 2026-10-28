-- Compact catalog heap for exact uncached profile-header aggregation.
-- Additive only: existing source rows stay on the legacy path until backfilled.
begin;
set local lock_timeout = '5s';

alter table social.instagram_account_catalog_posts
  add column if not exists profile_summary_catalog_ready boolean;
comment on column social.instagram_account_catalog_posts.profile_summary_catalog_ready is
  'Trigger-maintained compact summary readiness. NULL/false rows retain legacy reads and errors.';

-- Copy the actual native counter/date/status types, including integer counters
-- whose addition must continue to overflow at read time exactly as before.
create table if not exists social.instagram_profile_catalog_summary as
select id as post_id, profile_summary_accounts, assignment_status,
       likes, comments_count, views, posted_at,
       nullif(trim(coalesce(caption, text, '')), '') is not null as has_caption,
       jsonb_array_length(coalesce(hashtags, '[]'::jsonb)) as hashtag_count
from social.instagram_account_catalog_posts
with no data;

do $block$
begin
  if not exists (select 1 from pg_constraint
    where conrelid = 'social.instagram_profile_catalog_summary'::regclass
      and conname = 'instagram_profile_catalog_summary_pkey') then
    alter table social.instagram_profile_catalog_summary
      add constraint instagram_profile_catalog_summary_pkey primary key (post_id);
  end if;
  if not exists (select 1 from pg_constraint
    where conrelid = 'social.instagram_profile_catalog_summary'::regclass
      and conname = 'instagram_profile_catalog_summary_post_fkey') then
    alter table social.instagram_profile_catalog_summary
      add constraint instagram_profile_catalog_summary_post_fkey foreign key (post_id)
      references social.instagram_account_catalog_posts(id) on delete cascade on update cascade;
  end if;
end;
$block$;

create index if not exists instagram_profile_catalog_summary_accounts_gin
  on social.instagram_profile_catalog_summary using gin (profile_summary_accounts jsonb_ops);
create index if not exists instagram_catalog_profile_summary_unready_idx
  on social.instagram_account_catalog_posts (id) where profile_summary_catalog_ready is distinct from true;

create or replace function social.prepare_instagram_profile_catalog_summary()
returns trigger language plpgsql
set search_path = pg_catalog
as $function$
begin
  -- Do not reject malformed legacy hashtags on a source write. Their original
  -- jsonb_array_length error must still occur only when a matching summary reads.
  new.profile_summary_catalog_ready := new.hashtags is null or jsonb_typeof(new.hashtags) = 'array';
  return new;
end;
$function$;

create or replace function social.refresh_instagram_profile_catalog_summary()
returns trigger language plpgsql security definer
set search_path = pg_catalog
as $function$
begin
  if new.profile_summary_catalog_ready then
    insert into social.instagram_profile_catalog_summary as summary
      (post_id, profile_summary_accounts, assignment_status, likes, comments_count,
       views, posted_at, has_caption, hashtag_count)
    values (new.id, coalesce(new.profile_summary_accounts, social.instagram_profile_catalog_accounts(
              new.source_account, new.owner_username, new.collaborators, new.raw_data)),
            new.assignment_status, new.likes, new.comments_count, new.views, new.posted_at,
            nullif(trim(coalesce(new.caption, new.text, '')), '') is not null,
            jsonb_array_length(coalesce(new.hashtags, '[]'::jsonb)))
    on conflict (post_id) do update set
      profile_summary_accounts = excluded.profile_summary_accounts,
      assignment_status = excluded.assignment_status,
      likes = excluded.likes,
      comments_count = excluded.comments_count,
      views = excluded.views,
      posted_at = excluded.posted_at,
      has_caption = excluded.has_caption,
      hashtag_count = excluded.hashtag_count
    where (summary.profile_summary_accounts, summary.assignment_status, summary.likes,
           summary.comments_count, summary.views, summary.posted_at, summary.has_caption, summary.hashtag_count)
      is distinct from
          (excluded.profile_summary_accounts, excluded.assignment_status, excluded.likes,
           excluded.comments_count, excluded.views, excluded.posted_at, excluded.has_caption, excluded.hashtag_count);
  else
    delete from social.instagram_profile_catalog_summary where post_id = new.id;
  end if;
  return new;
end;
$function$;

drop trigger if exists trg_instagram_profile_catalog_summary_ready on social.instagram_account_catalog_posts;
create trigger trg_instagram_profile_catalog_summary_ready
before insert or update on social.instagram_account_catalog_posts
for each row execute function social.prepare_instagram_profile_catalog_summary();
drop trigger if exists trg_instagram_profile_catalog_summary on social.instagram_account_catalog_posts;
create trigger trg_instagram_profile_catalog_summary
after insert or update on social.instagram_account_catalog_posts
for each row execute function social.refresh_instagram_profile_catalog_summary();

alter table social.instagram_profile_catalog_summary enable row level security;
revoke all on social.instagram_profile_catalog_summary from public;
-- Source writers keep their existing privileges; only the narrowly scoped
-- trigger runs as its owner to maintain this private derived relation.
revoke all on function social.refresh_instagram_profile_catalog_summary() from public;
do $block$
declare role_name text;
begin
  foreach role_name in array array['anon', 'authenticated'] loop
    if exists (select 1 from pg_roles where rolname = role_name) then
      execute format('revoke all on social.instagram_profile_catalog_summary from %I', role_name);
      execute format('revoke all on function social.refresh_instagram_profile_catalog_summary() from %I', role_name);
    end if;
  end loop;
end;
$block$;

-- Backfill WHERE marker IS NULL by updating it=NULL in bounded batches with SKIP LOCKED;
-- triggers recompute it and copy all inputs atomically. ANALYZE both relations.
-- Reader rollback is safe without removing this table, marker, or triggers.
commit;
