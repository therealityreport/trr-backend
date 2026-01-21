-- Reload PostgREST schema cache
--
-- Run this after applying migrations that add or modify database functions.
-- PostgREST caches the database schema at startup and needs to be notified
-- to reload when functions are added/changed.
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -f scripts/reload_postgrest_schema.sql
--
-- Or use the convenience wrapper:
--   ./scripts/reload_postgrest_schema.sh
--
-- See: docs/runbooks/postgrest_schema_cache.md

NOTIFY pgrst, 'reload schema';
