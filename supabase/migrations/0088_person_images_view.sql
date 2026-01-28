-- Migration: Create person images view for Screenalytics facebank seeding
-- This view provides easy access to person images via media_links/media_assets.

BEGIN;

-- =============================================================================
-- Person images view for Screenalytics facebank seeding
-- =============================================================================

CREATE OR REPLACE VIEW core.v_person_images AS
SELECT
    ml.id,
    ml.entity_id AS person_id,
    ma.id AS media_asset_id,
    ma.source,
    ma.source_asset_id,
    ml.kind,
    ma.width,
    ma.height,
    ma.hosted_key,
    ma.hosted_url,
    ml.is_primary,
    ml.position,
    COALESCE(ma.hosted_url, ma.source_url) AS served_url,
    ma.created_at,
    ma.updated_at
FROM core.media_links ml
JOIN core.media_assets ma ON ml.media_asset_id = ma.id
WHERE ml.entity_type = 'person';

COMMENT ON VIEW core.v_person_images IS
'Person images for Screenalytics facebank seeding.
Includes width, height, hosted_key, is_primary.
Joins media_links (entity_type=person) to media_assets.';

-- service_role only (Screenalytics backend access)
GRANT SELECT ON core.v_person_images TO service_role;

-- Note: Required indexes already exist from migration 0059:
-- - media_links_entity_idx (entity_type, entity_id)
-- - media_links_media_asset_idx (media_asset_id)

COMMIT;
