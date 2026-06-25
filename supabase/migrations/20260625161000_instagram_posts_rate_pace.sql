create table if not exists social.ig_posts_rate_pace (
  rate_key text primary key,
  last_start timestamptz not null default now()
);

alter table social.ig_posts_rate_pace enable row level security;

grant all privileges on table social.ig_posts_rate_pace to service_role;
revoke all on table social.ig_posts_rate_pace from anon, authenticated;

drop policy if exists ig_posts_rate_pace_service_role_all on social.ig_posts_rate_pace;
create policy ig_posts_rate_pace_service_role_all
on social.ig_posts_rate_pace
for all
to service_role
using (true)
with check (true);
