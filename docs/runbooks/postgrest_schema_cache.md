# PostgREST Schema Cache Reload

## Problem: RPC Function Not Found (PGRST202)

### Symptoms

- **Error code**: `PGRST202`
- **Error message**: `"Could not find the function <schema>.<function_name>"`
- **When it happens**: After creating or updating database functions via migrations
- **Root cause**: PostgREST caches the database schema and doesn't automatically reload when functions are added/modified

### Why This Happens

PostgREST loads the database schema (tables, functions, views) into memory at startup for performance. When you:
1. Apply a migration that adds/modifies a function (e.g., `create or replace function core.some_rpc(...)`)
2. Try to call that function via `db.schema("core").rpc("some_rpc", {...})`

PostgREST returns PGRST202 because it hasn't reloaded the schema cache yet.

---

## Quick Fix

### Option 1: Reload Schema Cache (Recommended)

The official Supabase solution is to send a PostgreSQL `NOTIFY` signal:

**Using psql:**
```bash
psql "$SUPABASE_DB_URL" -c "NOTIFY pgrst, 'reload schema';"
```

**Using Supabase SQL Editor (hosted Supabase):**
```sql
NOTIFY pgrst, 'reload schema';
```

**Using helper script:**
```bash
./scripts/reload_postgrest_schema.sh
```

### Option 2: Restart PostgREST (local dev only)

For local Supabase via Docker:
```bash
docker restart supabase_rest_$(basename $(pwd))
# Or if using supabase CLI:
supabase stop && supabase start
```

**Note**: Do NOT restart PostgREST in production/hosted Supabase. Use `NOTIFY` instead.

---

## Prevention Checklist

### Before Deploying Migrations with New Functions

1. **Apply the migration** (adds the function to PostgreSQL)
   ```bash
   supabase db push
   # Or for hosted:
   # Apply migration via Supabase Dashboard → Database → Migrations
   ```

2. **Reload PostgREST schema cache**
   ```bash
   ./scripts/reload_postgrest_schema.sh
   # Or run: NOTIFY pgrst, 'reload schema';
   ```

3. **Verify the function is accessible**
   ```bash
   # Test the RPC endpoint
   curl -X POST "https://your-project.supabase.co/rest/v1/rpc/your_function" \
     -H "apikey: $SUPABASE_SERVICE_ROLE_KEY" \
     -H "Content-Type: application/json" \
     -d '{"param": "value"}'
   ```

### Exposed Schemas (Hosted Supabase Only)

**Critical**: PostgREST will NEVER serve functions/tables from schemas that aren't exposed.

**Check exposure**:
1. Go to Supabase Dashboard → Settings → API → Exposed schemas
2. Ensure `core` (and any custom schemas) are listed
3. If you add a schema, run `NOTIFY pgrst, 'reload schema';` after

**Default exposed schemas**: `public`, `storage`, `graphql_public`

If your functions are in `core` schema and it's not listed, PostgREST will return PGRST106 (schema not exposed) or PGRST202 (function not found).

---

## Code Best Practices

### Use the Helper Function

Always call RPC functions via `call_rpc_with_cache_reload_hint()` to get helpful error messages:

**Good:**
```python
from trr_backend.db.supabase import call_rpc_with_cache_reload_hint

result = call_rpc_with_cache_reload_hint(
    db,
    schema="core",
    function_name="upsert_show_images",
    params={"show_id": "123", "images": [...]},
)
```

**Bad:**
```python
# Direct RPC call - gives cryptic PGRST202 error
response = db.schema("core").rpc("upsert_show_images", {"show_id": "123", ...}).execute()
```

The helper automatically catches PGRST202 and prints the reload command.

---

## Troubleshooting

### Function Still Not Found After Reload

1. **Verify the function exists in PostgreSQL:**
   ```bash
   psql "$SUPABASE_DB_URL" -c "\\df+ core.your_function"
   ```

2. **Check function grants (service_role needs access):**
   ```sql
   GRANT EXECUTE ON FUNCTION core.your_function TO service_role;
   ```

3. **Check schema exposure (hosted Supabase):**
   - Ensure `core` is in Settings → API → Exposed schemas

4. **Check PostgREST logs:**
   ```bash
   # Local dev:
   docker logs supabase_rest_$(basename $(pwd))

   # Hosted: View logs in Supabase Dashboard → Logs → PostgREST
   ```

### Different Error: PGRST106 (Schema Not Exposed)

This means the schema exists but PostgREST isn't configured to serve it.

**Fix (hosted Supabase):**
1. Dashboard → Settings → API → Exposed schemas → Add `core`
2. Run `NOTIFY pgrst, 'reload schema';`

**Fix (local Supabase):**
Add to `supabase/config.toml`:
```toml
[api]
schemas = ["public", "storage", "graphql_public", "core"]
```

Then restart: `supabase stop && supabase start`

---

## References

- [Supabase PostgREST Schema Cache Docs](https://supabase.com/docs/guides/api/using-custom-schemas)
- [PostgREST Schema Cache Documentation](https://postgrest.org/en/stable/schema_cache.html)
- [Supabase Python Client: SyncClientOptions](https://github.com/orgs/supabase/discussions/33811)
