-- =============================================================================
-- Verification checks before applying cleanup migration 0033
-- =============================================================================
-- Run this BEFORE applying 0033_cleanup_legacy_jsonb_columns.sql
-- Ensures there are no duplicate canonical IDs or image identities that would
-- violate the new unique constraints.
--
-- Usage:
--   ./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
--
-- Or manually:
--   psql "$SUPABASE_DB_URL" -f scripts/db/verify_pre_0033_cleanup.sql
--
-- All queries should return 0 rows before proceeding.
-- =============================================================================

-- Include core schema guard (aborts if wrong database)
\i scripts/db/guard_core_schema.sql

\echo ''
\echo '=== Checking for duplicate IMDb IDs in core.shows ==='
\echo '(Should return 0 rows)'
select imdb_id, count(*) as duplicate_count
from core.shows
where imdb_id is not null and btrim(imdb_id) <> ''
group by imdb_id
having count(*) > 1;

\echo ''
\echo '=== Checking for duplicate TMDb IDs in core.shows ==='
\echo '(Should return 0 rows)'
select tmdb_id, count(*) as duplicate_count
from core.shows
where tmdb_id is not null
group by tmdb_id
having count(*) > 1;

\echo ''
\echo '=== Checking for duplicate image identities (show_id, source, source_image_id) ==='
\echo '(Should return 0 rows)'
select show_id, source, source_image_id, count(*) as duplicate_count
from core.show_images
where source_image_id is not null
group by show_id, source, source_image_id
having count(*) > 1;

\echo ''
\echo '=== Checking for duplicate TMDb image identities (tmdb_id, source, kind, file_path) ==='
\echo '(Should return 0 rows)'
select tmdb_id, source, kind, file_path, count(*) as duplicate_count
from core.show_images
where tmdb_id is not null and file_path is not null
group by tmdb_id, source, kind, file_path
having count(*) > 1;

\echo ''
\echo '=== Verification complete ==='
\echo 'If all queries returned 0 rows, you can safely apply migration 0033.'
\echo 'If any duplicates were found, resolve them before proceeding.'
