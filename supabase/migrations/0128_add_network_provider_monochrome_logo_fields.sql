begin;

alter table core.networks
  add column if not exists hosted_logo_black_key text,
  add column if not exists hosted_logo_black_url text,
  add column if not exists hosted_logo_black_sha256 text,
  add column if not exists hosted_logo_black_content_type text,
  add column if not exists hosted_logo_black_bytes bigint,
  add column if not exists hosted_logo_black_etag text,
  add column if not exists hosted_logo_black_at timestamptz,
  add column if not exists hosted_logo_white_key text,
  add column if not exists hosted_logo_white_url text,
  add column if not exists hosted_logo_white_sha256 text,
  add column if not exists hosted_logo_white_content_type text,
  add column if not exists hosted_logo_white_bytes bigint,
  add column if not exists hosted_logo_white_etag text,
  add column if not exists hosted_logo_white_at timestamptz;

alter table core.watch_providers
  add column if not exists hosted_logo_black_key text,
  add column if not exists hosted_logo_black_url text,
  add column if not exists hosted_logo_black_sha256 text,
  add column if not exists hosted_logo_black_content_type text,
  add column if not exists hosted_logo_black_bytes bigint,
  add column if not exists hosted_logo_black_etag text,
  add column if not exists hosted_logo_black_at timestamptz,
  add column if not exists hosted_logo_white_key text,
  add column if not exists hosted_logo_white_url text,
  add column if not exists hosted_logo_white_sha256 text,
  add column if not exists hosted_logo_white_content_type text,
  add column if not exists hosted_logo_white_bytes bigint,
  add column if not exists hosted_logo_white_etag text,
  add column if not exists hosted_logo_white_at timestamptz;

commit;
