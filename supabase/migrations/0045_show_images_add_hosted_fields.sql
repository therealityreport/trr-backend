-- Migration: Add hosted/S3 fields to core.show_images
-- Mirrors the pattern from core.cast_photos (migration 0043)

-- Add hosted columns for S3 mirroring
ALTER TABLE core.show_images
    ADD COLUMN IF NOT EXISTS hosted_bucket text,
    ADD COLUMN IF NOT EXISTS hosted_key text,
    ADD COLUMN IF NOT EXISTS hosted_url text,
    ADD COLUMN IF NOT EXISTS hosted_sha256 text,
    ADD COLUMN IF NOT EXISTS hosted_content_type text,
    ADD COLUMN IF NOT EXISTS hosted_bytes bigint,
    ADD COLUMN IF NOT EXISTS hosted_etag text,
    ADD COLUMN IF NOT EXISTS hosted_at timestamptz;

-- Index for tracking when images were hosted
CREATE INDEX IF NOT EXISTS idx_show_images_hosted_at
    ON core.show_images (hosted_at)
    WHERE hosted_at IS NOT NULL;

-- Index for deduplication by content hash
CREATE INDEX IF NOT EXISTS idx_show_images_hosted_sha256
    ON core.show_images (hosted_sha256)
    WHERE hosted_sha256 IS NOT NULL;

-- Partial index to speed up backfill queries (WHERE hosted_url IS NULL)
CREATE INDEX IF NOT EXISTS idx_show_images_missing_hosted
    ON core.show_images (source, show_id)
    WHERE hosted_url IS NULL;

-- View that provides served_url (always prefer hosted when available)
CREATE OR REPLACE VIEW core.v_show_images_served AS
SELECT
    *,
    coalesce(hosted_url, url) AS served_url
FROM core.show_images;

-- Grant access to the view
GRANT SELECT ON core.v_show_images_served TO anon, authenticated, service_role;
