begin;

-- View for show images enriched with show name and original URL.
-- Excludes vote fields; joins by tmdb_id (authoritative).

create schema if not exists core;

create or replace view core.v_show_images as
select
  si.id,
  si.show_id,
  si.tmdb_id,
  s.title as show_name,
  si.source,
  si.kind,
  si.iso_639_1,
  si.file_path,
  si.url_original,
  si.width,
  si.height,
  si.aspect_ratio,
  si.fetched_at
from core.show_images si
join core.shows s
  on si.tmdb_id = s.tmdb_id;

grant usage on schema core to anon, authenticated, service_role;
grant select on table core.v_show_images to anon, authenticated, service_role;

commit;

