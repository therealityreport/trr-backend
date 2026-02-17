begin;

-- Add thumbnail columns used by season social analytics cards.
alter table social.instagram_posts
  add column if not exists thumbnail_url text;

alter table social.tiktok_posts
  add column if not exists thumbnail_url text;

commit;
