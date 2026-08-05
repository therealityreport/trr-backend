begin;

-- Keep exact admin slug ownership deterministic under case-insensitive reads.
-- The preflight fails before index creation if legacy case/whitespace variants
-- exist; operators must resolve those rows explicitly rather than choosing a
-- winner in migration SQL.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE core.shows IN SHARE MODE;

DO $show_slug_duplicate_guard$
DECLARE
  duplicate_group_count integer;
BEGIN
  SELECT count(*)::integer
  INTO duplicate_group_count
  FROM (
    SELECT lower(btrim(slug)) AS normalized_slug
    FROM core.shows
    WHERE slug IS NOT NULL
      AND btrim(slug) <> ''
    GROUP BY lower(btrim(slug))
    HAVING count(*) > 1
  ) AS duplicate_groups;

  IF duplicate_group_count > 0 THEN
    RAISE EXCEPTION
      'core.shows contains % duplicate normalized slug group(s); resolve them before applying the unique index',
      duplicate_group_count
      USING ERRCODE = '23505';
  END IF;
END
$show_slug_duplicate_guard$;

CREATE UNIQUE INDEX IF NOT EXISTS core_shows_slug_normalized_unique
  ON core.shows (lower(btrim(slug)))
  WHERE slug IS NOT NULL
    AND btrim(slug) <> '';

commit;
