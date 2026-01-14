-- Migration: Add source tracking to show_cast
-- Tracks whether cast data came from HTML scraping or JSON API fallback

BEGIN;

-- Add source_type column with default value
ALTER TABLE core.show_cast
ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'fullcredits_html';

-- Add constraint to enforce valid values
ALTER TABLE core.show_cast
DROP CONSTRAINT IF EXISTS show_cast_source_type_check;

ALTER TABLE core.show_cast
ADD CONSTRAINT show_cast_source_type_check
CHECK (source_type IN ('fullcredits_html', 'credits_api_fallback', 'manual'));

-- Add index for analytics queries
CREATE INDEX IF NOT EXISTS idx_show_cast_source_type
ON core.show_cast(source_type);

-- Add column comment for documentation
COMMENT ON COLUMN core.show_cast.source_type IS
'Data source: fullcredits_html (HTML scrape), credits_api_fallback (JSON API), manual (user entry)';

COMMIT;
