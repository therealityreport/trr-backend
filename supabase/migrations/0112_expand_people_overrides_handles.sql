-- Migration 0112: Expand core.people_overrides social handles

begin;

alter table core.people_overrides
  add column if not exists tiktok_handle text,
  add column if not exists twitter_handle text,
  add column if not exists youtube_handle text;

commit;

