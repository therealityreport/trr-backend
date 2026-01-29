BEGIN;

-- Add slug column for URL-friendly survey identifiers
ALTER TABLE surveys.surveys
ADD COLUMN IF NOT EXISTS slug text;

-- Create unique index on slug (partial - only where slug is not null)
CREATE UNIQUE INDEX IF NOT EXISTS surveys_slug_unique
  ON surveys.surveys (slug)
  WHERE slug IS NOT NULL;

-- Backfill existing surveys with auto-generated slugs from title
UPDATE surveys.surveys
SET slug = lower(regexp_replace(title, '[^a-zA-Z0-9]+', '-', 'g'))
WHERE slug IS NULL;

COMMENT ON COLUMN surveys.surveys.slug IS
  'URL-friendly identifier for the survey. Must be unique when set.';

COMMIT;
