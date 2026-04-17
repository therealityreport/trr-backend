\i scripts/db/guard_core_schema.sql

with planned_indexes as (
  select
    entry->>'schema' as schema_name,
    entry->>'table' as table_name,
    entry->>'index_name' as index_name
  from jsonb_array_elements(%(planned_indexes_json)s::jsonb) as entry
)
select
  planned.schema_name,
  planned.table_name,
  planned.index_name,
  idx.indisvalid,
  format('drop index concurrently if exists %I.%I;', ns.nspname, cls.relname) as cleanup_sql
from planned_indexes planned
join pg_namespace ns on ns.nspname = planned.schema_name
join pg_class cls on cls.relname = planned.index_name and cls.relnamespace = ns.oid
join pg_index idx on idx.indexrelid = cls.oid
where not idx.indisvalid
order by planned.schema_name, planned.table_name, planned.index_name;
