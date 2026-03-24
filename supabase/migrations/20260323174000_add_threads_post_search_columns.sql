begin;

alter table social.meta_threads_posts
  add column if not exists search_text text not null default '',
  add column if not exists search_hashtags text[] not null default array[]::text[],
  add column if not exists search_handles text[] not null default array[]::text[],
  add column if not exists search_handle_identities text[] not null default array[]::text[];

commit;
