\i scripts/db/guard_core_schema.sql

with known_pids as (
  select jsonb_array_elements_text(%(known_pids_json)s::jsonb)::int as pid
),
planned_indexes as (
  select
    entry->>'schema' as schema_name,
    entry->>'table' as table_name,
    entry->>'index_name' as index_name
  from jsonb_array_elements(%(planned_indexes_json)s::jsonb) as entry
),
query_patterns as (
  select
    entry->>'label' as label,
    entry->>'pattern' as pattern
  from jsonb_array_elements(%(query_patterns_json)s::jsonb) as entry
),
build_progress as (
  select coalesce(jsonb_agg(to_jsonb(progress)), '[]'::jsonb) as payload
  from (
    select *
    from pg_stat_progress_create_index
    where pid in (select pid from known_pids)
  ) as progress
),
locks as (
  select coalesce(jsonb_agg(to_jsonb(lock_rows)), '[]'::jsonb) as payload
  from (
    select
      activity.pid,
      activity.application_name,
      activity.wait_event_type,
      activity.wait_event,
      activity.state,
      activity.query,
      pg_blocking_pids(activity.pid) as blocking_pids
    from pg_stat_activity activity
    where activity.pid in (select pid from known_pids)
       or exists (
         select 1
         from known_pids kp
         where kp.pid = any(pg_blocking_pids(activity.pid))
       )
  ) as lock_rows
),
activity as (
  select coalesce(jsonb_agg(to_jsonb(activity_rows)), '[]'::jsonb) as payload
  from (
    select
      pid,
      state,
      application_name,
      wait_event_type,
      wait_event,
      backend_type,
      query
    from pg_stat_activity
    where pid in (select pid from known_pids)
  ) as activity_rows
),
statement_stats as (
  select coalesce(jsonb_agg(to_jsonb(stmt_rows)), '[]'::jsonb) as payload
  from (
    select
      qp.label,
      ps.queryid::text as queryid,
      ps.calls,
      ps.total_exec_time,
      case when ps.calls > 0 then ps.total_exec_time / ps.calls else null end as mean_exec_time,
      ps.query
    from query_patterns qp
    join pg_stat_statements ps on ps.query ilike qp.pattern
  ) as stmt_rows
),
table_stats as (
  select coalesce(jsonb_agg(to_jsonb(table_rows)), '[]'::jsonb) as payload
  from (
    select
      ns.nspname as schema_name,
      cls.relname as table_name,
      st.seq_scan,
      st.n_tup_ins,
      st.n_tup_upd,
      st.n_tup_del
    from planned_indexes planned
    join pg_namespace ns on ns.nspname = planned.schema_name
    join pg_class cls on cls.relname = planned.table_name and cls.relnamespace = ns.oid
    join pg_stat_user_tables st on st.relid = cls.oid
  ) as table_rows
),
index_stats as (
  select coalesce(jsonb_agg(to_jsonb(index_rows)), '[]'::jsonb) as payload
  from (
    select
      planned.schema_name,
      planned.table_name,
      planned.index_name,
      coalesce(st.idx_scan, 0) as idx_scan
    from planned_indexes planned
    left join pg_namespace ns on ns.nspname = planned.schema_name
    left join pg_class cls on cls.relname = planned.index_name and cls.relnamespace = ns.oid
    left join pg_stat_user_indexes st on st.indexrelid = cls.oid
  ) as index_rows
),
invalid_indexes as (
  select coalesce(jsonb_agg(to_jsonb(invalid_rows)), '[]'::jsonb) as payload
  from (
    select
      planned.schema_name,
      planned.table_name,
      planned.index_name
    from planned_indexes planned
    join pg_namespace ns on ns.nspname = planned.schema_name
    join pg_class cls on cls.relname = planned.index_name and cls.relnamespace = ns.oid
    join pg_index idx on idx.indexrelid = cls.oid
    where not idx.indisvalid
  ) as invalid_rows
)
select jsonb_build_object(
  'captured_at', now() at time zone 'utc',
  'build_progress', (select payload from build_progress),
  'locks', (select payload from locks),
  'activity', (select payload from activity),
  'statement_stats', (select payload from statement_stats),
  'table_stats', (select payload from table_stats),
  'index_stats', (select payload from index_stats),
  'invalid_indexes', (select payload from invalid_indexes)
) as snapshot;
