\set ON_ERROR_STOP on
\pset pager off

-- Usage:
--   cd /Users/thomashulihan/Projects/TRR/TRR-Backend
--   ./scripts/db/run_sql.sh scripts/db/verify_socialblade_snapshot_fk_indexes.sql

\i scripts/db/guard_core_schema.sql

with expected(index_name, table_name, column_name, referenced_schema, referenced_table) as (
  values
    ('instagram_following_snapshots_last_job_id_idx', 'instagram_profile_following_snapshots', 'last_scrape_job_id', 'social', 'scrape_jobs'),
    ('instagram_following_snapshots_last_run_id_idx', 'instagram_profile_following_snapshots', 'last_scrape_run_id', 'social', 'scrape_runs'),
    ('instagram_relationship_snapshot_items_rel_row_id_idx', 'instagram_profile_relationship_snapshot_items', 'relationship_row_id', 'social', 'instagram_profile_relationships'),
    ('instagram_relationship_snapshot_items_last_job_id_idx', 'instagram_profile_relationship_snapshot_items', 'last_scrape_job_id', 'social', 'scrape_jobs'),
    ('instagram_relationship_snapshot_items_last_run_id_idx', 'instagram_profile_relationship_snapshot_items', 'last_scrape_run_id', 'social', 'scrape_runs')
),
fk_status as (
  select
    e.index_name,
    exists (
      select 1
      from pg_constraint con
      join pg_class tbl on tbl.oid = con.conrelid
      join pg_namespace tbl_ns on tbl_ns.oid = tbl.relnamespace
      join pg_class ref_tbl on ref_tbl.oid = con.confrelid
      join pg_namespace ref_ns on ref_ns.oid = ref_tbl.relnamespace
      join unnest(con.conkey) with ordinality as fk_cols(attnum, ordinality) on true
      join pg_attribute att on att.attrelid = tbl.oid and att.attnum = fk_cols.attnum
      where con.contype = 'f'
        and tbl_ns.nspname = 'social'
        and tbl.relname = e.table_name
        and ref_ns.nspname = e.referenced_schema
        and ref_tbl.relname = e.referenced_table
      group by con.oid
      having array_agg(att.attname::text order by fk_cols.ordinality) = array[e.column_name]
    ) as fk_exists
  from expected e
),
index_status as (
  select
    e.index_name,
    exists (
      select 1
      from pg_class idx
      join pg_namespace idx_ns on idx_ns.oid = idx.relnamespace
      join pg_index pgidx on pgidx.indexrelid = idx.oid
      join pg_class tbl on tbl.oid = pgidx.indrelid
      join pg_namespace tbl_ns on tbl_ns.oid = tbl.relnamespace
      where idx_ns.nspname = 'social'
        and idx.relname = e.index_name
        and tbl_ns.nspname = 'social'
        and tbl.relname = e.table_name
        and pgidx.indisvalid
        and (
          array(
            select att.attname::text
            from unnest(string_to_array(pgidx.indkey::text, ' ')::int[]) with ordinality as keys(attnum, ordinality)
            join pg_attribute att on att.attrelid = tbl.oid and att.attnum = keys.attnum
            order by keys.ordinality
          )
        )[1] = e.column_name
    ) as index_exists
  from expected e
)
select
  e.table_name,
  e.column_name,
  e.referenced_schema || '.' || e.referenced_table as references_table,
  e.index_name,
  fk.fk_exists,
  idx.index_exists,
  case
    when fk.fk_exists and idx.index_exists then 'ok'
    when not fk.fk_exists then 'missing_fk'
    else 'missing_or_invalid_index'
  end as status
from expected e
join fk_status fk using (index_name)
join index_status idx using (index_name)
order by e.table_name, e.column_name;

do $$
declare
  failures text;
begin
  with expected(index_name, table_name, column_name, referenced_schema, referenced_table) as (
    values
      ('instagram_following_snapshots_last_job_id_idx', 'instagram_profile_following_snapshots', 'last_scrape_job_id', 'social', 'scrape_jobs'),
      ('instagram_following_snapshots_last_run_id_idx', 'instagram_profile_following_snapshots', 'last_scrape_run_id', 'social', 'scrape_runs'),
      ('instagram_relationship_snapshot_items_rel_row_id_idx', 'instagram_profile_relationship_snapshot_items', 'relationship_row_id', 'social', 'instagram_profile_relationships'),
      ('instagram_relationship_snapshot_items_last_job_id_idx', 'instagram_profile_relationship_snapshot_items', 'last_scrape_job_id', 'social', 'scrape_jobs'),
      ('instagram_relationship_snapshot_items_last_run_id_idx', 'instagram_profile_relationship_snapshot_items', 'last_scrape_run_id', 'social', 'scrape_runs')
  ),
  fk_status as (
    select
      e.index_name,
      exists (
        select 1
        from pg_constraint con
        join pg_class tbl on tbl.oid = con.conrelid
        join pg_namespace tbl_ns on tbl_ns.oid = tbl.relnamespace
        join pg_class ref_tbl on ref_tbl.oid = con.confrelid
        join pg_namespace ref_ns on ref_ns.oid = ref_tbl.relnamespace
        join unnest(con.conkey) with ordinality as fk_cols(attnum, ordinality) on true
        join pg_attribute att on att.attrelid = tbl.oid and att.attnum = fk_cols.attnum
        where con.contype = 'f'
          and tbl_ns.nspname = 'social'
          and tbl.relname = e.table_name
          and ref_ns.nspname = e.referenced_schema
          and ref_tbl.relname = e.referenced_table
        group by con.oid
        having array_agg(att.attname::text order by fk_cols.ordinality) = array[e.column_name]
      ) as fk_exists
    from expected e
  ),
  index_status as (
    select
      e.index_name,
      exists (
        select 1
        from pg_class idx
        join pg_namespace idx_ns on idx_ns.oid = idx.relnamespace
        join pg_index pgidx on pgidx.indexrelid = idx.oid
        join pg_class tbl on tbl.oid = pgidx.indrelid
        join pg_namespace tbl_ns on tbl_ns.oid = tbl.relnamespace
        where idx_ns.nspname = 'social'
          and idx.relname = e.index_name
          and tbl_ns.nspname = 'social'
          and tbl.relname = e.table_name
          and pgidx.indisvalid
          and (
            array(
              select att.attname::text
              from unnest(string_to_array(pgidx.indkey::text, ' ')::int[]) with ordinality as keys(attnum, ordinality)
              join pg_attribute att on att.attrelid = tbl.oid and att.attnum = keys.attnum
              order by keys.ordinality
            )
          )[1] = e.column_name
      ) as index_exists
    from expected e
  )
  select string_agg(e.index_name || ':' ||
    case
      when not fk.fk_exists then 'missing_fk'
      else 'missing_or_invalid_index'
    end, ', ' order by e.index_name)
  into failures
  from expected e
  join fk_status fk using (index_name)
  join index_status idx using (index_name)
  where not fk.fk_exists
     or not idx.index_exists;

  if failures is not null then
    raise exception 'SocialBlade snapshot FK index verification failed: %', failures;
  end if;

  raise notice 'SocialBlade snapshot FK index verification passed.';
end $$;
