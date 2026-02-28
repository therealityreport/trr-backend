begin;

alter table social.tiktok_posts
  add column if not exists saves integer not null default 0;

commit;
