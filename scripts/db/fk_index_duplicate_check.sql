\i scripts/db/guard_core_schema.sql

with expected_indexes as (
  select
    entry->>'schema' as schema_name,
    entry->>'table' as table_name,
    entry->>'index_name' as index_name,
    array(select jsonb_array_elements_text(entry->'columns')) as expected_columns
  from jsonb_array_elements(%(expected_indexes_json)s::jsonb) as entry
),
existing_indexes as (
  select
    ns.nspname as schema_name,
    tbl.relname as table_name,
    idx.relname as index_name,
    array_agg(att.attname order by ordinality) as index_columns
  from pg_class idx
  join pg_index pgidx on pgidx.indexrelid = idx.oid
  join pg_class tbl on tbl.oid = pgidx.indrelid
  join pg_namespace ns on ns.oid = tbl.relnamespace
  join unnest(string_to_array(pgidx.indkey::text, ' ')::int[]) with ordinality as keys(attnum, ordinality) on true
  join pg_attribute att on att.attrelid = tbl.oid and att.attnum = keys.attnum
  group by ns.nspname, tbl.relname, idx.relname
)
select
  expected.schema_name,
  expected.table_name,
  expected.index_name as planned_index_name,
  existing.index_name as existing_index_name,
  expected.expected_columns,
  existing.index_columns
from expected_indexes expected
join existing_indexes existing
  on existing.schema_name = expected.schema_name
 and existing.table_name = expected.table_name
 and existing.index_name <> expected.index_name
where existing.index_columns[1:cardinality(expected.expected_columns)] = expected.expected_columns
order by expected.schema_name, expected.table_name, expected.index_name, existing.index_name;
