-- Generalize SocialBlade growth storage from person-scoped Instagram rows
-- to account-scoped rows keyed by platform + account handle.

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ADD COLUMN IF NOT EXISTS platform text;

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ADD COLUMN IF NOT EXISTS account_handle text;

UPDATE pipeline.socialblade_growth_data
SET platform = COALESCE(NULLIF(lower(platform), ''), 'instagram')
WHERE platform IS NULL OR NULLIF(lower(platform), '') IS NULL;

UPDATE pipeline.socialblade_growth_data
SET account_handle = COALESCE(NULLIF(lower(account_handle), ''), lower(instagram_handle))
WHERE account_handle IS NULL OR NULLIF(lower(account_handle), '') IS NULL;

UPDATE pipeline.socialblade_growth_data
SET instagram_handle = COALESCE(NULLIF(lower(instagram_handle), ''), account_handle)
WHERE instagram_handle IS NULL
   OR NULLIF(lower(instagram_handle), '') IS NULL
   OR instagram_handle IS DISTINCT FROM COALESCE(NULLIF(lower(instagram_handle), ''), account_handle);

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ALTER COLUMN platform SET DEFAULT 'instagram';

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ALTER COLUMN platform SET NOT NULL;

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ALTER COLUMN account_handle SET NOT NULL;

ALTER TABLE IF EXISTS pipeline.socialblade_growth_data
  ALTER COLUMN person_id DROP NOT NULL;

WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY platform, account_handle
      ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST, id DESC
    ) AS rn
  FROM pipeline.socialblade_growth_data
)
DELETE FROM pipeline.socialblade_growth_data target
USING ranked
WHERE target.id = ranked.id
  AND ranked.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS socialblade_growth_data_platform_account_handle_idx
  ON pipeline.socialblade_growth_data (platform, account_handle);

CREATE INDEX IF NOT EXISTS socialblade_growth_data_person_platform_account_handle_idx
  ON pipeline.socialblade_growth_data (person_id, platform, account_handle);
