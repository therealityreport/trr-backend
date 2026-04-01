-- Narrow show-cast consumer views to true cast rows only.

begin;

create or replace view core.v_show_cast_from_credits as
select
  sh.name as show_name,
  p.full_name as cast_member_name,
  c.show_id,
  c.person_id,
  c.billing_order,
  c.role,
  c.credit_category,
  c.id,
  c.created_at,
  c.updated_at,
  c.source_type
from core.credits c
join core.shows sh on sh.id = c.show_id
join core.people p on p.id = c.person_id
where c.credit_category = 'Self';

comment on view core.v_show_cast_from_credits is
'Validation view: matches core.show_cast shape but pulls only cast rows from core.credits.';

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
'Consumer view: credits-based replacement for legacy show_cast contract, limited to Self cast rows.';

grant select on core.v_show_cast_from_credits to anon, authenticated, service_role;
grant select on core.v_show_cast to anon, authenticated, service_role;

commit;
