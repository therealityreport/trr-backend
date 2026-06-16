create table if not exists social.instagram_post_comment_rollups (
  post_id uuid primary key references social.instagram_posts(id) on delete cascade,
  active_comment_count integer not null default 0 check (active_comment_count >= 0),
  missing_comment_count integer not null default 0 check (missing_comment_count >= 0),
  total_comment_count integer not null default 0 check (total_comment_count >= 0),
  updated_at timestamptz not null default now()
);

create index if not exists idx_instagram_post_comment_rollups_active_count
  on social.instagram_post_comment_rollups (active_comment_count desc, post_id);

create or replace function social.refresh_instagram_post_comment_rollup(target_post_id uuid)
returns void
language plpgsql
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
as $$
begin
  if tg_op = 'DELETE' then
    perform social.refresh_instagram_post_comment_rollup(old.post_id);
    return old;
  end if;

  perform social.refresh_instagram_post_comment_rollup(new.post_id);
  if tg_op = 'UPDATE' and old.post_id is distinct from new.post_id then
    perform social.refresh_instagram_post_comment_rollup(old.post_id);
  end if;
  return new;
end;
$$;

drop trigger if exists instagram_comments_rollup_refresh_tg on social.instagram_comments;
create trigger instagram_comments_rollup_refresh_tg
after insert or update of post_id, is_missing or delete
on social.instagram_comments
for each row
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
