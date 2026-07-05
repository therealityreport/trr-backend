begin;

alter function admin.set_updated_at()
  set search_path = admin, pg_temp;

alter function firebase_surveys.set_updated_at()
  set search_path = firebase_surveys, pg_temp;

alter function social.refresh_instagram_post_comment_rollup(uuid)
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_instagram_post_comment_rollup(uuid)
  from public, anon, authenticated;
grant execute on function social.refresh_instagram_post_comment_rollup(uuid)
  to service_role;

alter function social.refresh_instagram_post_comment_rollup_tg()
  security definer
  set search_path = social, pg_temp;

revoke execute on function social.refresh_instagram_post_comment_rollup_tg()
  from public, anon, authenticated;
grant execute on function social.refresh_instagram_post_comment_rollup_tg()
  to service_role;

alter function surveys.submit_response(uuid, jsonb)
  security invoker
  set search_path = surveys, auth, pg_temp;

revoke execute on function surveys.submit_response(uuid, jsonb)
  from public, anon;
grant execute on function surveys.submit_response(uuid, jsonb)
  to authenticated, service_role;

create schema if not exists extensions;
create extension if not exists vector with schema extensions;
alter extension vector set schema extensions;

commit;
