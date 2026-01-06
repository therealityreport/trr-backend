begin;

alter table core.season_images add column if not exists hosted_url text;
alter table core.season_images add column if not exists hosted_sha256 text;
alter table core.season_images add column if not exists hosted_key text;
alter table core.season_images add column if not exists hosted_bucket text;
alter table core.season_images add column if not exists hosted_content_type text;
alter table core.season_images add column if not exists hosted_bytes bigint;
alter table core.season_images add column if not exists hosted_etag text;
alter table core.season_images add column if not exists hosted_at timestamptz;

commit;
