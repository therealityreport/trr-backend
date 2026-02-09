-- Migration 0106: Drop unused games schema + publish stable show-cast consumer view
--
-- Notes:
-- - `games.*` is unused and safe to remove (update seeds/docs accordingly).
-- - `core.v_show_cast` is a stable consumer contract for cast reads built on credits.

begin;

-- Stable consumer alias over credits-based validation view.
create or replace view core.v_show_cast as
select
  show_name,
  cast_member_name,
  show_id,
  person_id,
  billing_order,
  role,
  credit_category,
  id,
  created_at,
  updated_at,
  source_type
from core.v_show_cast_from_credits;

comment on view core.v_show_cast is
'Consumer view: credits-based replacement for legacy show_cast contract.
Alias over core.v_show_cast_from_credits.';

grant select on core.v_show_cast to anon, authenticated, service_role;

-- Drop unused games schema (and all tables).
drop schema if exists games cascade;

commit;

