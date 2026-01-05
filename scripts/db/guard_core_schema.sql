-- =============================================================================
-- Core Schema Guard
-- =============================================================================
-- Include this at the top of any SQL script that requires the `core` schema.
-- Aborts immediately with a clear error if schema or tables are missing.
--
-- Usage:
--   \i scripts/db/guard_core_schema.sql
--   -- Your script continues here...
--
-- Or concatenate in shell:
--   cat scripts/db/guard_core_schema.sql your_script.sql | psql "$DATABASE_URL"
-- =============================================================================

-- Fail fast if core schema doesn't exist
do $$
begin
  if not exists (
    select 1 from information_schema.schemata where schema_name = 'core'
  ) then
    raise exception E'\n'
      '╔══════════════════════════════════════════════════════════════════════╗\n'
      '║  ERROR: Schema "core" does not exist!                                ║\n'
      '╠══════════════════════════════════════════════════════════════════════╣\n'
      '║  You are connected to the WRONG database.                            ║\n'
      '║                                                                       ║\n'
      '║  This script requires a Supabase database with the TRR schema.       ║\n'
      '║                                                                       ║\n'
      '║  Check your environment:                                             ║\n'
      '║    - SUPABASE_DB_URL should point to your Supabase project          ║\n'
      '║    - For local dev: run `supabase start` first                      ║\n'
      '║                                                                       ║\n'
      '║  Aborting to prevent damage to wrong database.                       ║\n'
      '╚══════════════════════════════════════════════════════════════════════╝\n';
  end if;
end $$;

-- Fail fast if core.shows table doesn't exist
do $$
begin
  if not exists (
    select 1 from information_schema.tables
    where table_schema = 'core' and table_name = 'shows'
  ) then
    raise exception E'\n'
      '╔══════════════════════════════════════════════════════════════════════╗\n'
      '║  ERROR: Table "core.shows" does not exist!                           ║\n'
      '╠══════════════════════════════════════════════════════════════════════╣\n'
      '║  The core schema exists but migrations have not been applied.        ║\n'
      '║                                                                       ║\n'
      '║  Run: supabase db push                                               ║\n'
      '║                                                                       ║\n'
      '║  Aborting to prevent errors.                                         ║\n'
      '╚══════════════════════════════════════════════════════════════════════╝\n';
  end if;
end $$;

-- Success: schema and core tables exist
do $$
begin
  raise notice 'Core schema guard passed: core.shows exists';
end $$;
