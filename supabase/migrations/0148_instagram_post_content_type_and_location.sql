begin;

-- Add support for storing parsed Instagram post content type + location metadata from API payload.
alter table social.instagram_posts
  add column if not exists content_type text,
  add column if not exists location jsonb;

commit;
