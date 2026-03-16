begin;

alter table if exists social.facebook_posts
  add column if not exists asset_manifest jsonb not null default '{}'::jsonb;

commit;
