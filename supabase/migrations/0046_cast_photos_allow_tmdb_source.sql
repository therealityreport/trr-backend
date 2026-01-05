-- Migration: Allow TMDb as a source for cast_photos
-- Makes IMDb-specific columns nullable for non-IMDb sources

-- Drop NOT NULL constraints for columns that are IMDb-specific
ALTER TABLE core.cast_photos ALTER COLUMN imdb_person_id DROP NOT NULL;
ALTER TABLE core.cast_photos ALTER COLUMN source_image_id DROP NOT NULL;
ALTER TABLE core.cast_photos ALTER COLUMN url_path DROP NOT NULL;

-- Add check constraint: imdb_person_id required only for imdb source
ALTER TABLE core.cast_photos ADD CONSTRAINT cast_photos_imdb_person_id_chk
  CHECK (source <> 'imdb' OR imdb_person_id IS NOT NULL);

-- Add check constraint: source_image_id required only for imdb source
-- (existing constraint cast_photos_source_image_id_chk already handles this)

-- Add partial index for TMDb source to speed up queries
CREATE INDEX IF NOT EXISTS idx_cast_photos_source_tmdb
  ON core.cast_photos (person_id, source)
  WHERE source = 'tmdb';
