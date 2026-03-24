begin;

create or replace view core.v_episode_cast as
select
    co.episode_id,
    c.show_id,
    c.person_id,
    c.credit_category,
    c.role,
    c.billing_order
from core.credit_occurrences co
join core.credits c on c.id = co.credit_id;

comment on view core.v_episode_cast is
'Episode-level cast: who appears in each episode.
Simple join from credit_occurrences to credits.
Used by Screenalytics for episode-specific candidate selection.';

grant select on core.v_episode_cast to service_role;

create or replace view core.v_season_cast as
select
    e.season_id,
    c.show_id,
    c.person_id,
    count(distinct e.id)::int as episodes_in_season
from core.episodes e
join core.credit_occurrences co on co.episode_id = e.id
join core.credits c on c.id = co.credit_id
group by e.season_id, c.show_id, c.person_id;

comment on view core.v_season_cast is
'Season-level cast: distinct people appearing in any episode of a season.
Includes episode count within that season.
Used by Screenalytics for season-level candidate selection.';

grant select on core.v_season_cast to service_role;

create index if not exists credit_occurrences_episode_credit_idx
    on core.credit_occurrences (episode_id, credit_id);

create index if not exists credits_show_person_category_idx
    on core.credits (show_id, person_id, credit_category);

create index if not exists episodes_season_id_idx
    on core.episodes (season_id);

commit;
