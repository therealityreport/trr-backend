\i scripts/db/guard_core_schema.sql

with query_patterns as (
  select
    entry->>'label' as label,
    entry->>'pattern' as pattern
  from jsonb_array_elements(%(query_patterns_json)s::jsonb) as entry
)
select
  qp.label,
  ps.queryid::text as queryid,
  ps.calls,
  ps.total_exec_time,
  case when ps.calls > 0 then ps.total_exec_time / ps.calls else null end as mean_exec_time,
  ps.rows,
  ps.query
from query_patterns qp
join pg_stat_statements ps on ps.query ilike qp.pattern
order by qp.label, ps.total_exec_time desc, ps.calls desc;
