\i scripts/db/guard_core_schema.sql

\echo 'Wave 1 core/admin representative query'
explain (analyze, buffers)
with added as (
  select distinct btrim(cs.trr_show_id::text) as show_id
  from admin.covered_shows cs
  where btrim(cs.trr_show_id::text) <> ''
),
network_source as (
  select
    s.id as show_id,
    btrim(network_name) as display_name,
    lower(btrim(network_name)) as name_key
  from core.shows s
  cross join lateral unnest(coalesce(s.networks, array[]::text[])) as network_name
  where btrim(network_name) <> ''
),
network_grouped as (
  select
    ns.name_key,
    min(ns.display_name) as name,
    count(distinct ns.show_id)::int as available_show_count,
    count(distinct case when a.show_id is not null then ns.show_id end)::int as added_show_count
  from network_source ns
  left join added a on a.show_id = ns.show_id::text
  group by ns.name_key
)
select
  ng.name,
  ng.available_show_count,
  ng.added_show_count,
  comp.resolution_status,
  comp.last_attempt_at
from network_grouped ng
left join lateral (
  select
    c.resolution_status,
    c.last_attempt_at
  from admin.network_streaming_completion c
  where c.entity_type = 'network'
    and c.entity_key = ng.name_key
  order by c.updated_at desc
  limit 1
) comp on true
order by ng.available_show_count desc, ng.name asc
limit 25;

\echo 'Wave 1 social representative query'
explain (analyze, buffers)
with active_runs as (
  select id
  from social.scrape_runs
  where status = any(array['queued', 'running', 'dispatching', 'dispatch_blocked', 'finalizing']::text[])
)
select
  coalesce(j.platform, 'unknown') as platform,
  coalesce(j.job_type, 'unknown') as job_type,
  coalesce(j.status, 'unknown') as status,
  lower(
    coalesce(
      nullif(j.config->>'stage', ''),
      nullif(j.metadata->>'stage', ''),
      nullif(j.job_type, ''),
      'unknown'
    )
  ) as stage,
  count(*)::bigint as total
from social.scrape_jobs j
left join active_runs ar on ar.id = j.run_id
where j.status = any(array['queued', 'running', 'dispatching', 'dispatch_blocked', 'finalizing']::text[])
   or ar.id is not null
group by 1, 2, 3, 4
order by platform, job_type, status, stage;
