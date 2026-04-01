begin;

alter table social.instagram_posts
  add column if not exists metadata_last_attempted_at timestamptz,
  add column if not exists metadata_last_failed_at timestamptz,
  add column if not exists metadata_consecutive_failures integer not null default 0;

commit;
