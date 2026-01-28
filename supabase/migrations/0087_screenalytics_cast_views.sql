-- Migration: Create Screenalytics support views for episode and season cast
-- These views provide easy access to credit data for Screenalytics candidate selection.

BEGIN;

-- =============================================================================
-- Episode-level cast view (who appears in each episode)
-- =============================================================================

CREATE OR REPLACE VIEW core.v_episode_cast AS
SELECT
    co.episode_id,
    c.show_id,
    c.person_id,
    c.credit_category,
    c.role,
    c.billing_order
FROM core.credit_occurrences co
JOIN core.credits c ON c.id = co.credit_id;

COMMENT ON VIEW core.v_episode_cast IS
'Episode-level cast: who appears in each episode.
Simple join from credit_occurrences to credits.
Used by Screenalytics for episode-specific candidate selection.';

-- service_role only (Screenalytics backend access)
GRANT SELECT ON core.v_episode_cast TO service_role;

-- =============================================================================
-- Season-level cast view (distinct people in any episode of a season)
-- =============================================================================

CREATE OR REPLACE VIEW core.v_season_cast AS
SELECT
    e.season_id,
    c.show_id,
    c.person_id,
    COUNT(DISTINCT e.id)::int AS episodes_in_season
FROM core.episodes e
JOIN core.credit_occurrences co ON co.episode_id = e.id
JOIN core.credits c ON c.id = co.credit_id
GROUP BY e.season_id, c.show_id, c.person_id;

COMMENT ON VIEW core.v_season_cast IS
'Season-level cast: distinct people appearing in any episode of a season.
Includes episode count within that season.
Used by Screenalytics for season-level candidate selection.';

-- service_role only
GRANT SELECT ON core.v_season_cast TO service_role;

-- =============================================================================
-- Indexes to support the views (if not already present)
-- =============================================================================

-- Composite index for credit_occurrences join pattern
CREATE INDEX IF NOT EXISTS credit_occurrences_episode_credit_idx
    ON core.credit_occurrences (episode_id, credit_id);

-- Composite index for category filtering on credits
CREATE INDEX IF NOT EXISTS credits_show_person_category_idx
    ON core.credits (show_id, person_id, credit_category);

-- Episode -> season lookup (for season_cast aggregation)
CREATE INDEX IF NOT EXISTS episodes_season_id_idx
    ON core.episodes (season_id);

COMMIT;
