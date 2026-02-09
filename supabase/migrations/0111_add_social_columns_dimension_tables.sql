-- Migration 0111: Add manually-maintained social IDs to dimension tables

begin;

alter table core.networks
  add column if not exists facebook_id text,
  add column if not exists instagram_id text,
  add column if not exists twitter_id text,
  add column if not exists tiktok_id text;

alter table core.watch_providers
  add column if not exists facebook_id text,
  add column if not exists instagram_id text,
  add column if not exists twitter_id text,
  add column if not exists tiktok_id text;

commit;

