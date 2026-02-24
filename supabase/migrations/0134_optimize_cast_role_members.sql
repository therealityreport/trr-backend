begin;

-- Support frequent cast-role member reads by show/person.
create index if not exists credits_show_person_idx
  on core.credits (show_id, person_id);

-- Support first-photo lookup ordered by gallery index.
create index if not exists cast_photos_person_gallery_idx
  on core.cast_photos (person_id, gallery_index)
  where gallery_index is not null;

-- Rebuild read model using pre-aggregated episode + role stats to avoid
-- multiplicative joins between episode evidence and role assignments.
create or replace view core.v_show_cast_roles_enriched as
with base_cast as (
  select
    sc.show_id,
    sc.person_id,
    max(coalesce(p.full_name, sc.cast_member_name)) as person_name
  from core.v_show_cast sc
  left join core.people p
    on p.id = sc.person_id
  group by sc.show_id, sc.person_id
),
episode_stats as (
  select
    vec.show_id,
    vec.person_id,
    count(distinct case
      when coalesce(vec.appearance_type, 'appears') <> 'archive_footage' then vec.episode_id
      else null
    end)::int as total_episodes,
    count(distinct case
      when coalesce(vec.appearance_type, '') = 'archive_footage' then vec.episode_id
      else null
    end)::int as archive_episodes,
    count(distinct case when vec.season_number is not null then vec.season_number end)::int as seasons_appeared,
    array_remove(array_agg(distinct vec.season_number), null) as season_numbers,
    max(case when vec.season_number is not null then vec.season_number else null end)::int as latest_season
  from core.v_episode_credits vec
  group by vec.show_id, vec.person_id
),
role_stats as (
  select
    sra.show_id,
    sra.person_id,
    array_remove(array_agg(distinct case when rc.is_active then rc.name else null end), null) as roles
  from core.show_cast_role_assignments sra
  join core.show_role_catalog rc
    on rc.id = sra.role_id
   and rc.show_id = sra.show_id
  group by sra.show_id, sra.person_id
)
select
  bc.show_id,
  bc.person_id,
  bc.person_name,
  coalesce(es.total_episodes, 0)::int as total_episodes,
  coalesce(es.archive_episodes, 0)::int as archive_episodes,
  coalesce(es.seasons_appeared, 0)::int as seasons_appeared,
  coalesce(es.season_numbers, '{}'::int[]) as season_numbers,
  es.latest_season,
  coalesce(rs.roles, '{}'::text[]) as roles
from base_cast bc
left join episode_stats es
  on es.show_id = bc.show_id
 and es.person_id = bc.person_id
left join role_stats rs
  on rs.show_id = bc.show_id
 and rs.person_id = bc.person_id;

grant select on table core.v_show_cast_roles_enriched to authenticated;
grant select on table core.v_show_cast_roles_enriched to service_role;

commit;
