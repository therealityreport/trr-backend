-- Migration 0114: Create core.v_cast_summary view
--
-- Aggregation view over core.credits + core.credit_occurrences for cast summary data.

begin;

create or replace view core.v_cast_summary as
with base as (
  select
    cr.person_id,
    cr.show_id,
    cr.credit_category,
    cr.billing_order,
    co.episode_id,
    e.season_number,
    e.air_date
  from core.credits cr
  left join core.credit_occurrences co on co.credit_id = cr.id
  left join core.episodes e on e.id = co.episode_id
),
season_counts as (
  select
    person_id,
    show_id,
    credit_category,
    billing_order,
    season_number,
    count(distinct episode_id)::int as episode_count
  from base
  where season_number is not null
  group by person_id, show_id, credit_category, billing_order, season_number
),
agg as (
  select
    person_id,
    show_id,
    credit_category,
    billing_order,
    count(distinct episode_id)::int as total_episodes,
    min(air_date) as first_appearance,
    max(air_date) as last_appearance
  from base
  group by person_id, show_id, credit_category, billing_order
),
episodes_by_season as (
  select
    person_id,
    show_id,
    credit_category,
    billing_order,
    jsonb_object_agg(season_number::text, episode_count) as episodes_by_season
  from season_counts
  group by person_id, show_id, credit_category, billing_order
)
select
  a.person_id,
  a.show_id,
  a.credit_category,
  a.billing_order,
  a.total_episodes,
  ebs.episodes_by_season,
  a.first_appearance,
  a.last_appearance
from agg a
left join episodes_by_season ebs
  on ebs.person_id = a.person_id
  and ebs.show_id = a.show_id
  and ebs.credit_category = a.credit_category
  and ebs.billing_order = a.billing_order;

comment on view core.v_cast_summary is
'Cast summary per (person, show, category, billing) derived from credits + credit_occurrences.';

grant select on core.v_cast_summary to anon, authenticated, service_role;

commit;
