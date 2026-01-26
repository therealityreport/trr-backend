begin;

-- Shows: TMDb metadata
insert into core.show_source_latest(
  show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
)
select id, 'tmdb', 'details', coalesce(tmdb_fetched_at, now()), 'legacy_column', 'success',
       tmdb_meta, core.jsonb_sha256(tmdb_meta)
from core.shows
where tmdb_meta is not null
on conflict (show_id, source_id, variant) do update
set fetched_at = excluded.fetched_at,
    fetch_method = excluded.fetch_method,
    status = excluded.status,
    payload = excluded.payload,
    payload_sha256 = excluded.payload_sha256;

insert into core.show_source_history(
  show_id, source_id, variant, fetched_at, fetch_method, status, payload, payload_sha256
)
select id, 'tmdb', 'details', coalesce(tmdb_fetched_at, now()), 'legacy_column', 'success',
       tmdb_meta, core.jsonb_sha256(tmdb_meta)
from core.shows s
where tmdb_meta is not null
and not exists (
  select 1 from core.show_source_history h
  where h.show_id = s.id and h.source_id = 'tmdb' and h.variant = 'details'
    and h.payload_sha256 = core.jsonb_sha256(s.tmdb_meta)
);

commit;
