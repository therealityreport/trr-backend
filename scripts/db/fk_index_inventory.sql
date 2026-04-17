\i scripts/db/guard_core_schema.sql

with owned_schemas as (
  select jsonb_array_elements_text(%(owned_schemas_json)s::jsonb) as schema_name
),
hot_tables as (
  select
    entry->>'schema' as schema_name,
    entry->>'table' as table_name
  from jsonb_array_elements(%(hot_tables_json)s::jsonb) as entry
),
fk_constraints as (
  select
    n.nspname as schema_name,
    c.relname as table_name,
    con.conname as constraint_name,
    rn.nspname as referenced_schema,
    rc.relname as referenced_table,
    con.conrelid,
    con.conkey::int[] as fk_attnums,
    array_agg(a.attname order by key_cols.ordinality) as fk_columns_in_order,
    array_remove(
      array_agg(case when not a.attnotnull then a.attname end order by key_cols.ordinality),
      null
    ) as nullable_columns
  from pg_constraint con
  join pg_class c on c.oid = con.conrelid
  join pg_namespace n on n.oid = c.relnamespace
  join pg_class rc on rc.oid = con.confrelid
  join pg_namespace rn on rn.oid = rc.relnamespace
  join unnest(con.conkey) with ordinality as key_cols(attnum, ordinality) on true
  join pg_attribute a on a.attrelid = con.conrelid and a.attnum = key_cols.attnum
  where con.contype = 'f'
    and n.nspname in (select schema_name from owned_schemas)
  group by
    n.nspname,
    c.relname,
    con.conname,
    rn.nspname,
    rc.relname,
    con.conrelid,
    con.conkey
),
index_details as (
  select
    tn.nspname as schema_name,
    tc.relname as table_name,
    ic.relname as index_name,
    idx.indrelid,
    idx.indisvalid,
    idx.indpred is not null as has_predicate,
    array(
      select idx.indkey[s]::int
      from generate_series(0, idx.indnkeyatts - 1) as s
    ) as index_attnums
  from pg_index idx
  join pg_class tc on tc.oid = idx.indrelid
  join pg_namespace tn on tn.oid = tc.relnamespace
  join pg_class ic on ic.oid = idx.indexrelid
),
fk_with_indexes as (
  select
    fk.*,
    coalesce(pc.reltuples::bigint, 0) as estimated_row_count,
    case
      when cardinality(fk.fk_columns_in_order) = 1
        then (
          select st.null_frac
          from pg_stats st
          where st.schemaname = fk.schema_name
            and st.tablename = fk.table_name
            and st.attname = fk.fk_columns_in_order[1]
          limit 1
        )
      else null
    end as single_column_null_frac,
    exists (
      select 1
      from hot_tables ht
      where ht.schema_name = fk.schema_name
        and ht.table_name = fk.table_name
    ) as hot_table,
    exists (
      select 1
      from index_details idx
      where idx.indrelid = fk.conrelid
        and idx.indisvalid
        and idx.index_attnums[1:cardinality(fk.fk_attnums)] = fk.fk_attnums
    ) as covered_by_existing_index
  from fk_constraints fk
  join pg_class pc on pc.oid = fk.conrelid
),
candidate_indexes as (
  select
    schema_name,
    table_name,
    constraint_name,
    referenced_schema,
    referenced_table,
    fk_columns_in_order,
    nullable_columns,
    estimated_row_count,
    single_column_null_frac,
    hot_table,
    covered_by_existing_index,
    schema_name || '_' || table_name || '_' || array_to_string(fk_columns_in_order, '_') || '_idx'
      as proposed_index_name,
    fk_columns_in_order as proposed_index_columns,
    case
      when cardinality(fk_columns_in_order) = 1
        and cardinality(nullable_columns) = 1
        and coalesce(single_column_null_frac, 0) > 0.5
        then format('%%I is not null', fk_columns_in_order[1])
      else null
    end as proposed_partial_predicate,
    case
      when hot_table or estimated_row_count > 10000000 then '3h'
      else '30min'
    end as statement_timeout_tier,
    case
      when covered_by_existing_index then 'skip-covered'
      else 'add'
    end as decision
  from fk_with_indexes
)
select
  schema_name,
  table_name,
  constraint_name,
  referenced_schema,
  referenced_table,
  fk_columns_in_order,
  nullable_columns,
  estimated_row_count,
  single_column_null_frac,
  hot_table,
  covered_by_existing_index,
  proposed_index_name,
  proposed_index_columns,
  proposed_partial_predicate,
  statement_timeout_tier,
  decision
from candidate_indexes
order by schema_name, table_name, constraint_name;
