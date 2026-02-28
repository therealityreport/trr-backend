-- Add slug column to core.shows for structured media file naming
ALTER TABLE core.shows ADD COLUMN IF NOT EXISTS slug text;

CREATE UNIQUE INDEX IF NOT EXISTS core_shows_slug_unique
  ON core.shows (slug) WHERE slug IS NOT NULL AND btrim(slug) <> '';
