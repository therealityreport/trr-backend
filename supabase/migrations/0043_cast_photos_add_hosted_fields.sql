begin;

alter table core.cast_photos add column if not exists hosted_bucket text;
alter table core.cast_photos add column if not exists hosted_key text;
alter table core.cast_photos add column if not exists hosted_url text;
alter table core.cast_photos add column if not exists hosted_sha256 text;
alter table core.cast_photos add column if not exists hosted_content_type text;
alter table core.cast_photos add column if not exists hosted_bytes integer;
alter table core.cast_photos add column if not exists hosted_etag text;
alter table core.cast_photos add column if not exists hosted_at timestamptz;

create index if not exists cast_photos_person_source_idx
  on core.cast_photos (person_id, source);

create index if not exists cast_photos_hosted_at_idx
  on core.cast_photos (hosted_at)
  where hosted_at is not null;

create index if not exists cast_photos_hosted_sha_idx
  on core.cast_photos (hosted_sha256)
  where hosted_sha256 is not null;

create or replace view core.v_cast_photos as
select
  cast_photos.*,
  coalesce(hosted_url, image_url, url, thumb_url) as display_url
from core.cast_photos;

commit;
