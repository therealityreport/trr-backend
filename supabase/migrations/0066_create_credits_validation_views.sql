-- Migration: Create validation views for credits consolidation
-- These views match the shape of legacy tables but pull from the new
-- canonical credits tables. Used to validate parity before cutover.
-- Names intentionally do NOT collide with existing tables/views.

BEGIN;

-- ---------------------------------------------------------------------------
-- core.v_show_cast_from_credits
-- Validation view matching core.show_cast column contract.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.v_show_cast_from_credits AS
SELECT
  sh.name AS show_name,
  p.full_name AS cast_member_name,
  c.show_id,
  c.person_id,
  c.billing_order,
  c.role,
  c.credit_category,
  c.id,
  c.created_at,
  c.updated_at,
  c.source_type
FROM core.credits c
JOIN core.shows sh ON sh.id = c.show_id
JOIN core.people p ON p.id = c.person_id;

COMMENT ON VIEW core.v_show_cast_from_credits IS
'Validation view: matches core.show_cast shape but pulls from core.credits.
Used to verify parity before cutover.';

GRANT SELECT ON core.v_show_cast_from_credits TO anon, authenticated, service_role;


-- ---------------------------------------------------------------------------
-- core.v_episode_appearances_from_credits
-- Validation view matching core.episode_appearances / v_episode_appearances contract.
-- Aggregates credit_occurrences back into the array-based format.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.v_episode_appearances_from_credits AS
SELECT
  sh.name AS show_name,
  p.full_name AS cast_member_name,

  -- Aggregate seasons (distinct, sorted)
  COALESCE(
    ARRAY(
      SELECT DISTINCT sea.season_number
      FROM core.credit_occurrences co2
      JOIN core.episodes e2 ON e2.id = co2.episode_id
      JOIN core.seasons sea ON sea.id = e2.season_id
      WHERE co2.credit_id = c.id
        AND sea.season_number IS NOT NULL
      ORDER BY sea.season_number
    ),
    '{}'::integer[]
  ) AS seasons,

  -- Aggregate tmdb_season_ids (distinct, sorted by season_number)
  COALESCE(
    ARRAY(
      SELECT DISTINCT sea.tmdb_season_id
      FROM core.credit_occurrences co2
      JOIN core.episodes e2 ON e2.id = co2.episode_id
      JOIN core.seasons sea ON sea.id = e2.season_id
      WHERE co2.credit_id = c.id
        AND sea.tmdb_season_id IS NOT NULL
      ORDER BY sea.tmdb_season_id
    ),
    '{}'::integer[]
  ) AS tmdb_season_ids,

  -- Show identifiers
  sh.tmdb_id AS tmdb_show_id,
  sh.imdb_id AS imdb_show_id,

  -- Aggregate imdb_episode_title_ids (sorted by season/episode/air_date)
  COALESCE(
    ARRAY(
      SELECT e2.imdb_episode_id
      FROM core.credit_occurrences co2
      JOIN core.episodes e2 ON e2.id = co2.episode_id
      LEFT JOIN core.seasons sea ON sea.id = e2.season_id
      WHERE co2.credit_id = c.id
        AND e2.imdb_episode_id IS NOT NULL
      ORDER BY
        sea.season_number NULLS LAST,
        e2.episode_number NULLS LAST,
        e2.air_date NULLS LAST,
        e2.imdb_episode_id
    ),
    '{}'::text[]
  ) AS imdb_episode_title_ids,

  -- Aggregate tmdb_episode_ids (sorted by season/episode/air_date)
  COALESCE(
    ARRAY(
      SELECT e2.tmdb_episode_id
      FROM core.credit_occurrences co2
      JOIN core.episodes e2 ON e2.id = co2.episode_id
      LEFT JOIN core.seasons sea ON sea.id = e2.season_id
      WHERE co2.credit_id = c.id
        AND e2.tmdb_episode_id IS NOT NULL
      ORDER BY
        sea.season_number NULLS LAST,
        e2.episode_number NULLS LAST,
        e2.air_date NULLS LAST,
        e2.tmdb_episode_id
    ),
    '{}'::integer[]
  ) AS tmdb_episode_ids,

  -- Computed total (matches generated column in episode_appearances)
  (
    SELECT COUNT(DISTINCT co2.episode_id)
    FROM core.credit_occurrences co2
    WHERE co2.credit_id = c.id
  )::integer AS total_episodes,

  -- Identity columns
  c.show_id,
  c.person_id,
  c.id,
  c.created_at,
  c.updated_at

FROM core.credits c
JOIN core.shows sh ON sh.id = c.show_id
JOIN core.people p ON p.id = c.person_id
-- Only include credits that have at least one episode occurrence
WHERE EXISTS (
  SELECT 1
  FROM core.credit_occurrences co
  WHERE co.credit_id = c.id
);

COMMENT ON VIEW core.v_episode_appearances_from_credits IS
'Validation view: matches core.episode_appearances shape but aggregates from
core.credits + core.credit_occurrences. Used to verify parity before cutover.';

GRANT SELECT ON core.v_episode_appearances_from_credits TO anon, authenticated, service_role;


-- ---------------------------------------------------------------------------
-- Utility view for "who is in episode X?" queries
-- This is the primary use case the new model enables.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.v_episode_credits AS
SELECT
  co.episode_id,
  co.credit_id,
  co.appearance_type,
  c.show_id,
  c.person_id,
  c.credit_category,
  c.role,
  c.billing_order,
  c.source_type,
  p.full_name AS person_name,
  sh.name AS show_name,
  e.episode_number,
  sea.season_number,
  e.title AS episode_name
FROM core.credit_occurrences co
JOIN core.credits c ON c.id = co.credit_id
JOIN core.people p ON p.id = c.person_id
JOIN core.shows sh ON sh.id = c.show_id
JOIN core.episodes e ON e.id = co.episode_id
LEFT JOIN core.seasons sea ON sea.id = e.season_id;

COMMENT ON VIEW core.v_episode_credits IS
'Utility view for "who is in episode X?" queries.
Joins credit_occurrences → credits → people/shows/episodes.';

GRANT SELECT ON core.v_episode_credits TO anon, authenticated, service_role;


-- ---------------------------------------------------------------------------
-- Utility view for "seasons per person per show" queries
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW core.v_person_show_seasons AS
SELECT
  c.person_id,
  c.show_id,
  p.full_name AS person_name,
  sh.name AS show_name,
  ARRAY(
    SELECT DISTINCT sea.season_number
    FROM core.credit_occurrences co
    JOIN core.episodes e ON e.id = co.episode_id
    JOIN core.seasons sea ON sea.id = e.season_id
    WHERE co.credit_id = c.id
      AND sea.season_number IS NOT NULL
    ORDER BY sea.season_number
  ) AS seasons_appeared,
  (
    SELECT COUNT(DISTINCT co.episode_id)
    FROM core.credit_occurrences co
    WHERE co.credit_id = c.id
  )::integer AS total_episodes
FROM core.credits c
JOIN core.people p ON p.id = c.person_id
JOIN core.shows sh ON sh.id = c.show_id
WHERE EXISTS (
  SELECT 1
  FROM core.credit_occurrences co
  WHERE co.credit_id = c.id
);

COMMENT ON VIEW core.v_person_show_seasons IS
'Utility view for "which seasons does person P appear in for show S?"
Returns distinct seasons and total episode count per person/show.';

GRANT SELECT ON core.v_person_show_seasons TO anon, authenticated, service_role;

COMMIT;
