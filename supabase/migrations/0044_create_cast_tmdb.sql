-- Migration: Create cast_tmdb table for TMDb person data
-- Stores person details and external IDs from TMDb API

CREATE TABLE IF NOT EXISTS core.cast_tmdb (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_id UUID NOT NULL REFERENCES core.people(id) ON DELETE CASCADE,

    -- TMDb person ID (from /3/person/{id})
    tmdb_id INTEGER NOT NULL,

    -- Person details from /3/person/{person_id}
    name TEXT,
    also_known_as TEXT[],  -- Array of alternative names
    biography TEXT,
    birthday DATE,
    deathday DATE,
    gender SMALLINT DEFAULT 0,  -- 0=not set, 1=female, 2=male, 3=non-binary
    adult BOOLEAN DEFAULT TRUE,
    homepage TEXT,
    known_for_department TEXT,
    place_of_birth TEXT,
    popularity NUMERIC(10, 3) DEFAULT 0,
    profile_path TEXT,  -- TMDb profile image path

    -- External IDs from /3/person/{person_id}/external_ids
    imdb_id TEXT,
    freebase_mid TEXT,
    freebase_id TEXT,
    tvrage_id INTEGER,
    wikidata_id TEXT,
    facebook_id TEXT,
    instagram_id TEXT,
    tiktok_id TEXT,
    twitter_id TEXT,
    youtube_id TEXT,

    -- Metadata
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure one TMDb record per person
    CONSTRAINT cast_tmdb_person_id_unique UNIQUE (person_id),
    -- Also ensure tmdb_id is unique
    CONSTRAINT cast_tmdb_tmdb_id_unique UNIQUE (tmdb_id)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cast_tmdb_person_id ON core.cast_tmdb(person_id);
CREATE INDEX IF NOT EXISTS idx_cast_tmdb_tmdb_id ON core.cast_tmdb(tmdb_id);
CREATE INDEX IF NOT EXISTS idx_cast_tmdb_imdb_id ON core.cast_tmdb(imdb_id) WHERE imdb_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cast_tmdb_instagram_id ON core.cast_tmdb(instagram_id) WHERE instagram_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cast_tmdb_twitter_id ON core.cast_tmdb(twitter_id) WHERE twitter_id IS NOT NULL;

-- Trigger to update updated_at on modification
CREATE OR REPLACE FUNCTION core.cast_tmdb_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS cast_tmdb_updated_at ON core.cast_tmdb;
CREATE TRIGGER cast_tmdb_updated_at
    BEFORE UPDATE ON core.cast_tmdb
    FOR EACH ROW
    EXECUTE FUNCTION core.cast_tmdb_set_updated_at();

-- Grant permissions
GRANT SELECT ON core.cast_tmdb TO authenticated;
GRANT SELECT ON core.cast_tmdb TO service_role;
GRANT INSERT, UPDATE, DELETE ON core.cast_tmdb TO service_role;

-- Comment on table and columns
COMMENT ON TABLE core.cast_tmdb IS 'TMDb person data including details and external IDs';
COMMENT ON COLUMN core.cast_tmdb.tmdb_id IS 'TMDb person ID';
COMMENT ON COLUMN core.cast_tmdb.also_known_as IS 'Alternative names from TMDb';
COMMENT ON COLUMN core.cast_tmdb.gender IS '0=not set, 1=female, 2=male, 3=non-binary';
COMMENT ON COLUMN core.cast_tmdb.profile_path IS 'TMDb profile image path (append to https://image.tmdb.org/t/p/w500/)';
COMMENT ON COLUMN core.cast_tmdb.imdb_id IS 'IMDb person ID (nm...)';
COMMENT ON COLUMN core.cast_tmdb.fetched_at IS 'When the TMDb data was last fetched';
