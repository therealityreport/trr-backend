-- =============================================================================
-- Reload PostgREST Schema Cache
-- =============================================================================
-- Run this after migrations to force PostgREST to reload its schema cache.
-- This avoids PGRST204 errors ("Could not find column X in the schema cache").
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -f scripts/db/reload_postgrest_schema.sql
-- =============================================================================

\echo 'Reloading PostgREST schema cache...'

select pg_notify('pgrst', 'reload schema');

\echo 'Reloading PostgREST config...'

select pg_notify('pgrst', 'reload config');

\echo 'Done. PostgREST should reload within a few seconds.'
