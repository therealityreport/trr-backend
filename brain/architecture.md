# TRR-Backend Architecture Notes

- API entrypoints live under `api/`
- shared backend logic lives under `trr_backend/`
- schema and database contracts live under `supabase/`
- scripts and operational helpers live under `scripts/`
- when PostgREST or exposed SQL contracts change, run `./scripts/reload_postgrest_schema.sh`
