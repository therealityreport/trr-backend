create table if not exists social.instagram_post_comment_rollups (
  post_id uuid primary key references social.instagram_posts(id) on delete cascade,
  active_comment_count integer not null default 0 check (active_comment_count >= 0),
  missing_comment_count integer not null default 0 check (missing_comment_count >= 0),
  total_comment_count integer not null default 0 check (total_comment_count >= 0),
  updated_at timestamptz not null default now()
);

create index if not exists idx_instagram_post_comment_rollups_active_count
  on social.instagram_post_comment_rollups (active_comment_count desc, post_id);

alter table social.instagram_post_comment_rollups enable row level security;

grant all privileges on table social.instagram_post_comment_rollups to service_role;
revoke all on table social.instagram_post_comment_rollups from anon, authenticated;

drop policy if exists instagram_post_comment_rollups_service_role_all on social.instagram_post_comment_rollups;
create policy instagram_post_comment_rollups_service_role_all
on social.instagram_post_comment_rollups
for all
to service_role
using (true)
with check (true);

create or replace function social.refresh_instagram_post_comment_rollup(target_post_id uuid)
returns void
language plpgsql
security definer
set search_path = social, public
as $$
begin
  if target_post_id is null then
    return;
  end if;

  insert into social.instagram_post_comment_rollups (
    post_id,
    active_comment_count,
    missing_comment_count,
    total_comment_count,
    updated_at
  )
  select
    target_post_id,
    count(*) filter (where coalesce(c.is_missing, false) = false)::integer,
    count(*) filter (where coalesce(c.is_missing, false) = true)::integer,
    count(*)::integer,
    now()
  from social.instagram_comments c
  where c.post_id = target_post_id
  on conflict (post_id) do update set
    active_comment_count = excluded.active_comment_count,
    missing_comment_count = excluded.missing_comment_count,
    total_comment_count = excluded.total_comment_count,
    updated_at = excluded.updated_at;
end;
$$;

create or replace function social.refresh_instagram_post_comment_rollup_tg()
returns trigger
language plpgsql
security definer
set search_path = social, public
as $$
begin
  if tg_op = 'INSERT' then
    perform social.refresh_instagram_post_comment_rollup(changed.post_id)
    from (
      select distinct post_id
      from new_rows
      where post_id is not null
    ) changed;
  elsif tg_op = 'UPDATE' then
    perform social.refresh_instagram_post_comment_rollup(changed.post_id)
    from (
      select post_id from new_rows where post_id is not null
      union
      select post_id from old_rows where post_id is not null
    ) changed;
  elsif tg_op = 'DELETE' then
    perform social.refresh_instagram_post_comment_rollup(changed.post_id)
    from (
      select distinct post_id
      from old_rows
      where post_id is not null
    ) changed;
  end if;

  return null;
end;
$$;

grant execute on function social.refresh_instagram_post_comment_rollup(uuid) to service_role;
grant execute on function social.refresh_instagram_post_comment_rollup_tg() to service_role;

drop trigger if exists instagram_comments_rollup_refresh_tg on social.instagram_comments;
drop trigger if exists instagram_comments_rollup_refresh_insert_tg on social.instagram_comments;
drop trigger if exists instagram_comments_rollup_refresh_update_tg on social.instagram_comments;
drop trigger if exists instagram_comments_rollup_refresh_delete_tg on social.instagram_comments;

create trigger instagram_comments_rollup_refresh_insert_tg
after insert
on social.instagram_comments
referencing new table as new_rows
for each statement
execute function social.refresh_instagram_post_comment_rollup_tg();

create trigger instagram_comments_rollup_refresh_update_tg
after update
on social.instagram_comments
referencing old table as old_rows new table as new_rows
for each statement
execute function social.refresh_instagram_post_comment_rollup_tg();

create trigger instagram_comments_rollup_refresh_delete_tg
after delete
on social.instagram_comments
referencing old table as old_rows
for each statement
execute function social.refresh_instagram_post_comment_rollup_tg();

insert into social.instagram_post_comment_rollups (
  post_id,
  active_comment_count,
  missing_comment_count,
  total_comment_count,
  updated_at
)
select
  c.post_id,
  count(*) filter (where coalesce(c.is_missing, false) = false)::integer,
  count(*) filter (where coalesce(c.is_missing, false) = true)::integer,
  count(*)::integer,
  now()
from social.instagram_comments c
where c.post_id is not null
group by c.post_id
on conflict (post_id) do update set
  active_comment_count = excluded.active_comment_count,
  missing_comment_count = excluded.missing_comment_count,
  total_comment_count = excluded.total_comment_count,
  updated_at = excluded.updated_at;
