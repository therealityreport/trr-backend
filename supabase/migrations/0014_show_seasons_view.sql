begin;

-- View for seasons enriched with show name (for API/debugging convenience).

create schema if not exists core;

create or replace view core.v_show_seasons as
select
  sh.title as show_name,
  se.*
from core.seasons se
join core.shows sh
  on se.show_id = sh.id;

grant usage on schema core to anon, authenticated, service_role;
grant select on table core.v_show_seasons to anon, authenticated, service_role;

commit;

