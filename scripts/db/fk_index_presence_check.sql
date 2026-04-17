\i scripts/db/guard_core_schema.sql

with expected_indexes as (
  select
    entry->>'schema' as schema_name,
    entry->>'table' as table_name,
    entry->>'index_name' as index_name,
    array(select jsonb_array_elements_text(entry->'columns')) as expected_columns,
    nullif(entry->>'predicate', '') as expected_predicate
  from jsonb_array_elements(%(expected_indexes_json)s::jsonb) as entry
),
actual_indexes as (
  select
    ns.nspname as schema_name,
    tbl.relname as table_name,
    idx.relname as index_name,
    pgidx.indisvalid,
    array_agg(att.attname order by ordinality) as actual_columns,
    pg_get_expr(pgidx.indpred, pgidx.indrelid) as actual_predicate
  from pg_class idx
  join pg_index pgidx on pgidx.indexrelid = idx.oid
  join pg_class tbl on tbl.oid = pgidx.indrelid
  join pg_namespace ns on ns.oid = tbl.relnamespace
  join unnest(string_to_array(pgidx.indkey::text, ' ')::int[]) with ordinality as keys(attnum, ordinality) on true
  join pg_attribute att on att.attrelid = tbl.oid and att.attnum = keys.attnum
  group by ns.nspname, tbl.relname, idx.relname, pgidx.indisvalid, pgidx.indpred, pgidx.indrelid
)
select
  expected.schema_name,
  expected.table_name,
  expected.index_name,
  actual.indisvalid,
  expected.expected_columns,
  actual.actual_columns,
  expected.expected_predicate,
  actual.actual_predicate,
  case
    when actual.index_name is null then 'missing'
    when not actual.indisvalid then 'invalid'
    when actual.actual_columns <> expected.expected_columns then 'wrong_columns'
    when coalesce(actual.actual_predicate, '') <> coalesce(expected.expected_predicate, '') then 'wrong_predicate'
    else 'ok'
  end as status
from expected_indexes expected
left join actual_indexes actual
  on actual.schema_name = expected.schema_name
 and actual.table_name = expected.table_name
 and actual.index_name = expected.index_name
where actual.index_name is null
   or not actual.indisvalid
   or actual.actual_columns <> expected.expected_columns
   or coalesce(actual.actual_predicate, '') <> coalesce(expected.expected_predicate, '')
order by expected.schema_name, expected.table_name, expected.index_name;
