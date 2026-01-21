-- Migration: Create canonical credits tables for Phase 5 consolidation
-- This creates core.credits + core.credit_occurrences as the new canonical
-- model for "who is in each show" and "who appears in each episode".
-- Legacy tables (show_cast, episode_appearances, cast_memberships, episode_cast)
-- remain intact for rollback safety.

BEGIN;

-- ---------------------------------------------------------------------------
-- core.credits - Canonical show-level membership
-- One row per person/role/category/source on a show.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.credits (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Core identity
  show_id uuid NOT NULL REFERENCES core.shows(id) ON DELETE CASCADE,
  person_id uuid NOT NULL REFERENCES core.people(id) ON DELETE CASCADE,

  -- Credit details
  credit_category text NOT NULL,  -- 'Self', 'cast', 'crew', 'guest', etc.
  role text NULL,                 -- Character name, job title, or NULL
  billing_order integer NULL,

  -- Source tracking (reuse existing source_type values from show_cast)
  source_type text NOT NULL,

  -- Extensible metadata for source-specific fields
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- Timestamps
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Uniqueness: one credit per person/show/category/role/source
-- Use COALESCE for role to handle NULL correctly in unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS credits_unique_idx
  ON core.credits (show_id, person_id, credit_category, COALESCE(role, ''), source_type);

-- Query indexes
CREATE INDEX IF NOT EXISTS credits_show_id_idx
  ON core.credits (show_id);

CREATE INDEX IF NOT EXISTS credits_person_id_idx
  ON core.credits (person_id);

CREATE INDEX IF NOT EXISTS credits_show_id_category_idx
  ON core.credits (show_id, credit_category);

CREATE INDEX IF NOT EXISTS credits_source_type_idx
  ON core.credits (source_type);

-- Source type constraint (matches show_cast.source_type values)
-- Use DO block for idempotency - ADD CONSTRAINT IF NOT EXISTS is not available
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'credits_source_type_check'
        AND conrelid = 'core.credits'::regclass
    ) THEN
        ALTER TABLE core.credits
        ADD CONSTRAINT credits_source_type_check
        CHECK (source_type IN (
            'fullcredits_html',
            'credits_graphql_paginated',
            'credits_graphql_paginated_partial',
            'credits_api_top_billed',
            'credits_api_fallback',
            'manual'
        ));
    END IF;
END $$;

-- updated_at trigger
DROP TRIGGER IF EXISTS core_credits_set_updated_at ON core.credits;
CREATE TRIGGER core_credits_set_updated_at
BEFORE UPDATE ON core.credits
FOR EACH ROW
EXECUTE FUNCTION core.set_updated_at();

COMMENT ON TABLE core.credits IS
'Canonical show-level credits. One row per person/role/category/source.
Replaces show_cast as the source of truth for "who is in this show".';

COMMENT ON COLUMN core.credits.credit_category IS
'Credit category: Self, cast, crew, guest, etc.';

COMMENT ON COLUMN core.credits.source_type IS
'Data source:
- fullcredits_html: HTML scraping (complete when available)
- credits_graphql_paginated: GraphQL pagination (complete, filtered for main cast)
- credits_graphql_paginated_partial: GraphQL pagination hit limits
- credits_api_top_billed: JSON API (partial - top-billed only)
- credits_api_fallback: Deprecated JSON API fallback (legacy)
- manual: Human entered';


-- ---------------------------------------------------------------------------
-- core.credit_occurrences - Per-episode presence
-- Links credits to specific episodes they appear in.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.credit_occurrences (
  credit_id uuid NOT NULL REFERENCES core.credits(id) ON DELETE CASCADE,
  episode_id uuid NOT NULL REFERENCES core.episodes(id) ON DELETE CASCADE,

  -- Appearance type for future extensibility
  appearance_type text NOT NULL DEFAULT 'appears',

  -- Timestamps
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  -- Composite primary key
  PRIMARY KEY (credit_id, episode_id)
);

-- Critical index for "who is in episode X?" queries
CREATE INDEX IF NOT EXISTS credit_occurrences_episode_id_idx
  ON core.credit_occurrences (episode_id);

-- updated_at trigger
DROP TRIGGER IF EXISTS core_credit_occurrences_set_updated_at ON core.credit_occurrences;
CREATE TRIGGER core_credit_occurrences_set_updated_at
BEFORE UPDATE ON core.credit_occurrences
FOR EACH ROW
EXECUTE FUNCTION core.set_updated_at();

COMMENT ON TABLE core.credit_occurrences IS
'Per-episode credit presence. Links credits to episodes they appear in.
Enables "who is in episode X?" via single join chain.';

COMMENT ON COLUMN core.credit_occurrences.appearance_type IS
'Type of appearance: appears (default), archive_footage, uncredited, etc.';


-- ---------------------------------------------------------------------------
-- Grants and RLS
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA core TO anon, authenticated, service_role;

-- credits table
GRANT SELECT ON TABLE core.credits TO anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE core.credits TO service_role;

ALTER TABLE core.credits ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS core_credits_public_read ON core.credits;
CREATE POLICY core_credits_public_read ON core.credits
FOR SELECT TO anon, authenticated
USING (true);

-- credit_occurrences table
GRANT SELECT ON TABLE core.credit_occurrences TO anon, authenticated;
GRANT ALL PRIVILEGES ON TABLE core.credit_occurrences TO service_role;

ALTER TABLE core.credit_occurrences ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS core_credit_occurrences_public_read ON core.credit_occurrences;
CREATE POLICY core_credit_occurrences_public_read ON core.credit_occurrences
FOR SELECT TO anon, authenticated
USING (true);

COMMIT;
