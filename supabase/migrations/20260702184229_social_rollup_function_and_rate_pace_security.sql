begin;

alter function social.refresh_tiktok_post_comment_rollup(uuid)
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_tiktok_post_comment_rollup(uuid)
  from public, anon, authenticated;
grant execute on function social.refresh_tiktok_post_comment_rollup(uuid)
  to service_role;

alter function social.refresh_tiktok_post_comment_rollup_tg()
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_tiktok_post_comment_rollup_tg()
  from public, anon, authenticated;
grant execute on function social.refresh_tiktok_post_comment_rollup_tg()
  to service_role;

alter function social.refresh_youtube_post_comment_rollup(uuid)
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_youtube_post_comment_rollup(uuid)
  from public, anon, authenticated;
grant execute on function social.refresh_youtube_post_comment_rollup(uuid)
  to service_role;

alter function social.refresh_youtube_post_comment_rollup_tg()
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_youtube_post_comment_rollup_tg()
  from public, anon, authenticated;
grant execute on function social.refresh_youtube_post_comment_rollup_tg()
  to service_role;

-- The table exists in live Supabase and is used by the Instagram comments
-- pacer; keep fresh database replays aligned before locking RLS/grants down.
create table if not exists social.ig_comment_rate_pace (
  rate_key text primary key,
  last_start timestamptz not null default now()
);

alter table social.ig_comment_rate_pace enable row level security;

grant all privileges on table social.ig_comment_rate_pace to service_role;
revoke all on table social.ig_comment_rate_pace from anon, authenticated;

drop policy if exists ig_comment_rate_pace_service_role_all on social.ig_comment_rate_pace;
create policy ig_comment_rate_pace_service_role_all
on social.ig_comment_rate_pace
for all
to service_role
using (true)
with check (true);

commit;
