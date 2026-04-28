\set ON_ERROR_STOP on
\pset pager off

\if :{?explain_analyze}
\else
\set explain_analyze false
\endif
\if :{?statement_timeout}
\else
\set statement_timeout 8s
\endif
\if :{?lock_timeout}
\else
\set lock_timeout 1s
\endif
\if :{?safe_limit}
\else
\set safe_limit 50
\endif
\if :{?safe_offset}
\else
\set safe_offset 0
\endif
\if :{?show_id}
\else
\set show_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?season_id}
\else
\set season_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?season_number}
\else
\set season_number 1
\endif
\if :{?week_start}
\else
\set week_start 2026-01-01T00:00:00+00:00
\endif
\if :{?week_end}
\else
\set week_end 2026-01-08T00:00:00+00:00
\endif
\if :{?community_id}
\else
\set community_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?reddit_post_id}
\else
\set reddit_post_id example_post_id
\endif
\if :{?reddit_period_key}
\else
\set reddit_period_key episode-1
\endif
\if :{?account_platform}
\else
\set account_platform instagram
\endif
\if :{?account_platforms_csv}
\else
\set account_platforms_csv instagram,youtube,facebook
\endif
\if :{?account_handle}
\else
\set account_handle bravotv
\endif
\if :{?person_id}
\else
\set person_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?source_scope}
\else
\set source_scope bravo
\endif
\if :{?review_status}
\else
\set review_status open
\endif
\if :{?survey_slug}
\else
\set survey_slug example-survey
\endif
\if :{?survey_id}
\else
\set survey_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?survey_run_id}
\else
\set survey_run_id 00000000-0000-0000-0000-000000000000
\endif
\if :{?brand_target_type}
\else
\set brand_target_type franchise
\endif
\if :{?brand_q}
\else
\set brand_q housewives
\endif
\if :{?media_entity_type}
\else
\set media_entity_type show
\endif
\if :{?media_kind}
\else
\set media_kind gallery
\endif
\if :{?search_q}
\else
\set search_q traitors
\endif

\i scripts/db/guard_core_schema.sql

BEGIN READ ONLY;
SET LOCAL statement_timeout = :'statement_timeout';
SET LOCAL lock_timeout = :'lock_timeout';
SET LOCAL idle_in_transaction_session_timeout = '15s';
SET LOCAL row_security = on;
SET LOCAL application_name = 'trr-hot-path-explain';

\echo 'hot_path=social_landing label=covered_shows route=/api/v1/admin/socials/landing-summary'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
WITH covered AS (
  SELECT
    cs.id::text AS id,
    cs.trr_show_id::text AS trr_show_id,
    cs.show_name,
    s.name AS core_show_name,
    s.slug,
    COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
    s.show_total_episodes,
    si.hosted_url AS poster_url,
    lower(
      trim(
        both '-' FROM regexp_replace(
          regexp_replace(COALESCE(s.name, ''), '&', ' and ', 'gi'),
          '[^a-z0-9]+',
          '-',
          'gi'
        )
      )
    ) AS computed_slug
  FROM admin.covered_shows AS cs
  LEFT JOIN core.shows AS s
    ON s.id = cs.trr_show_id
  LEFT JOIN core.show_images AS si
    ON si.id = s.primary_poster_image_id
),
ranked AS (
  SELECT
    id,
    trr_show_id,
    show_name,
    alternative_names,
    show_total_episodes,
    poster_url,
    CASE
      WHEN COALESCE(NULLIF(trim(slug), ''), NULLIF(computed_slug, '')) IS NULL
        THEN NULL
      WHEN COUNT(*) OVER (PARTITION BY computed_slug) > 1
        THEN COALESCE(NULLIF(trim(slug), ''), computed_slug) || '--' || lower(left(trr_show_id, 8))
      ELSE COALESCE(NULLIF(trim(slug), ''), computed_slug)
    END AS canonical_slug
  FROM covered
)
SELECT
  id,
  trr_show_id,
  show_name,
  canonical_slug,
  alternative_names,
  show_total_episodes,
  poster_url
FROM ranked
ORDER BY show_name ASC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=social_landing label=reddit_dashboard route=/api/v1/admin/socials/landing-summary'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  COUNT(*) FILTER (WHERE c.is_active)::int AS active_community_count,
  COUNT(*) FILTER (WHERE NOT c.is_active)::int AS archived_community_count,
  COUNT(DISTINCT c.trr_show_id)::int AS show_count
FROM admin.reddit_communities AS c;

\echo 'hot_path=social_landing label=socialblade_rows route=/api/v1/admin/socials/landing-socialblade-rows'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  id::text AS id,
  person_id::text AS person_id,
  platform,
  account_handle,
  scraped_at,
  updated_at,
  created_at,
  stats_refreshed,
  raw_response->>'socialblade_url' AS socialblade_url
FROM pipeline.socialblade_growth_data
WHERE platform = ANY(string_to_array(:'account_platforms_csv', ',')::text[])
  AND (
    person_id = :'person_id'::uuid
    OR account_handle = :'account_handle'
  )
ORDER BY
  platform ASC,
  account_handle ASC,
  person_id ASC NULLS LAST,
  updated_at DESC NULLS LAST,
  scraped_at DESC NULLS LAST,
  created_at DESC NULLS LAST,
  id ASC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=profile_dashboard label=shared_account_source route=/api/v1/admin/socials/profiles/:platform/:handle/dashboard'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  id::text,
  platform,
  source_scope,
  account_handle,
  is_active,
  scrape_priority,
  last_scrape_status,
  last_scrape_at,
  updated_at
FROM social.shared_account_sources
WHERE platform = :'account_platform'
  AND lower(account_handle) = lower(:'account_handle')
ORDER BY is_active DESC, scrape_priority ASC, updated_at DESC NULLS LAST
LIMIT :safe_limit;

\echo 'hot_path=profile_dashboard label=recent_catalog_jobs route=/api/v1/admin/socials/profiles/:platform/:handle/dashboard'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  j.id::text AS job_id,
  j.run_id::text AS run_id,
  j.platform,
  j.job_type,
  j.status,
  j.items_found,
  j.created_at,
  r.status AS run_status,
  r.created_at AS run_created_at
FROM social.scrape_jobs AS j
LEFT JOIN social.scrape_runs AS r
  ON r.id = j.run_id
WHERE j.platform = :'account_platform'
  AND (
    lower(coalesce(j.config->>'account_handle', j.config->>'handle', j.config->>'username', j.metadata->>'account_handle', '')) =
      lower(:'account_handle')
    OR lower(coalesce(j.config->>'source_account', '')) = lower(:'account_handle')
  )
ORDER BY j.created_at DESC NULLS LAST
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=season_analytics label=season_targets route=/api/v1/admin/socials/shows/:show_id/seasons/:season_number/social/analytics'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  season_id::text,
  show_id::text,
  platform,
  source_scope,
  timezone,
  is_active,
  updated_at
FROM social.season_targets
WHERE season_id = :'season_id'::uuid
  AND source_scope = :'source_scope'
ORDER BY platform ASC;

\echo 'hot_path=week_live_health label=instagram_week_bucket route=/api/v1/admin/socials/shows/:show_id/seasons/:season_number/social/analytics/week/:week_index/live-health'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  date_trunc('day', p.posted_at) AS day_utc,
  ltrim(lower(coalesce(p.source_account, p.username)), '@') AS account_handle,
  COUNT(*)::int AS posts,
  COALESCE(SUM(p.comments_count), 0)::bigint AS comments,
  COALESCE(SUM(p.likes), 0)::bigint AS likes
FROM social.instagram_posts AS p
WHERE p.season_id = :'season_id'::uuid
  AND p.posted_at >= :'week_start'::timestamptz
  AND p.posted_at < :'week_end'::timestamptz
  AND (
    :'account_handle' = ''
    OR ltrim(lower(coalesce(p.source_account, p.username)), '@') = ltrim(lower(:'account_handle'), '@')
  )
GROUP BY day_utc, account_handle
ORDER BY day_utc ASC, account_handle ASC;

\echo 'hot_path=shared_ingest label=recent_runs route=/api/v1/admin/socials/runs and /shared/runs'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  r.id::text,
  r.season_id::text,
  r.show_id::text,
  r.source_scope,
  r.status,
  r.config->>'pipeline_ingest_mode' AS pipeline_ingest_mode,
  r.created_at,
  r.started_at,
  r.completed_at
FROM social.scrape_runs AS r
WHERE r.source_scope = :'source_scope'
  AND (
    r.season_id = :'season_id'::uuid
    OR coalesce(r.config->>'pipeline_ingest_mode', '') IN (
      'shared_account_async',
      'shared_account_catalog_backfill'
    )
  )
ORDER BY r.created_at DESC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=shared_review_queue label=open_items route=/api/v1/admin/socials/shared/review-queue'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  q.id::text,
  q.platform,
  q.source_scope,
  q.source_id,
  q.source_account,
  q.review_status,
  q.review_reason,
  q.resolved_show_id::text,
  q.resolved_season_id::text,
  q.created_at,
  q.updated_at
FROM social.shared_post_review_queue AS q
WHERE q.source_scope = :'source_scope'
  AND q.review_status = :'review_status'
ORDER BY q.created_at DESC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=reddit_sources label=communities_with_threads route=/api/v1/admin/reddit/communities'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  c.id::text,
  c.trr_show_id::text,
  c.trr_show_name,
  c.subreddit,
  c.is_active,
  COUNT(t.id)::int AS assigned_thread_count,
  MAX(t.posted_at) AS latest_thread_posted_at
FROM admin.reddit_communities AS c
LEFT JOIN admin.reddit_threads AS t
  ON t.community_id = c.id
 AND (
    t.trr_season_id = :'season_id'::uuid
    OR t.trr_season_id IS NULL
 )
WHERE c.trr_show_id = :'show_id'::uuid
   OR c.id = :'community_id'::uuid
GROUP BY
  c.id,
  c.trr_show_id,
  c.trr_show_name,
  c.subreddit,
  c.is_active
ORDER BY c.trr_show_name ASC, lower(c.subreddit) ASC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=reddit_window label=stored_window_posts route=/api/v1/admin/reddit/communities/:community_id/seasons/:season_id/window-posts'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
WITH scoped AS (
  SELECT DISTINCT ON (m.reddit_post_id)
    p.reddit_post_id,
    p.title,
    p.author,
    p.score,
    p.num_comments,
    p.posted_at,
    COALESCE(NULLIF(TRIM(m.link_flair_text), ''), NULLIF(TRIM(p.link_flair_text), '')) AS link_flair_text,
    m.is_show_match,
    m.match_score
  FROM social.reddit_period_post_matches AS m
  JOIN social.reddit_posts AS p
    ON p.reddit_post_id = m.reddit_post_id
  WHERE m.community_id = :'community_id'::uuid
    AND m.season_id = :'season_id'::uuid
    AND m.passes_flair_filter = true
    AND m.period_key = :'reddit_period_key'
  ORDER BY m.reddit_post_id, m.updated_at DESC, p.posted_at DESC NULLS LAST
)
SELECT
  reddit_post_id,
  title,
  author,
  score,
  num_comments,
  posted_at,
  link_flair_text,
  is_show_match,
  match_score
FROM scoped
ORDER BY posted_at DESC NULLS LAST, num_comments DESC NULLS LAST, score DESC NULLS LAST
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=reddit_detail label=post_comments route=/api/v1/admin/reddit/communities/:community_id/seasons/:season_id/posts/:reddit_post_id'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  c.reddit_comment_id,
  c.parent_comment_id,
  c.author,
  c.score,
  c.depth,
  c.created_at_utc
FROM social.reddit_comments AS c
WHERE c.reddit_post_id = :'reddit_post_id'
ORDER BY c.created_at_utc ASC NULLS LAST, c.depth ASC, c.score DESC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=survey_admin label=survey_definition route=/api/admin/normalized-surveys/:surveySlug'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  s.id::text,
  s.slug,
  s.title,
  s.is_active,
  s.created_at,
  s.updated_at,
  question_counts.question_count,
  run_counts.run_count
FROM firebase_surveys.surveys AS s
LEFT JOIN LATERAL (
  SELECT COUNT(*)::int AS question_count
  FROM firebase_surveys.questions AS q
  WHERE q.survey_id = s.id
) AS question_counts ON true
LEFT JOIN LATERAL (
  SELECT COUNT(*)::int AS run_count
  FROM firebase_surveys.survey_runs AS sr
  WHERE sr.survey_id = s.id
) AS run_counts ON true
WHERE s.slug = :'survey_slug'
   OR s.id = :'survey_id'::uuid
LIMIT 1;

\echo 'hot_path=survey_admin label=survey_responses route=/api/admin/normalized-surveys/:surveySlug/runs/:runId/responses'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  r.id::text,
  r.survey_run_id::text,
  r.user_id,
  r.submission_number,
  r.completed_at,
  r.created_at,
  answer_counts.answer_count
FROM firebase_surveys.responses AS r
LEFT JOIN LATERAL (
  SELECT COUNT(*)::int AS answer_count
  FROM firebase_surveys.answers AS a
  WHERE a.response_id = r.id
) AS answer_counts ON true
WHERE r.survey_run_id = :'survey_run_id'::uuid
ORDER BY r.created_at DESC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=survey_admin label=survey_show_palette route=/admin/surveys show palette reads'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  p.id::text,
  p.trr_show_id::text,
  p.season_number,
  p.name,
  p.source_type,
  p.created_at,
  p.updated_at
FROM public.survey_show_palette_library AS p
WHERE p.trr_show_id = :'show_id'::uuid
  AND (p.season_number = :season_number OR p.season_number IS NULL)
ORDER BY p.season_number NULLS FIRST, lower(p.name) ASC
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=brand_profile label=brand_logo_assets route=/api/v1/admin/brands/logos'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  id::text,
  target_type,
  target_key,
  target_label,
  source_url,
  source_domain,
  hosted_logo_url,
  is_primary,
  mirror_status,
  failure_reason,
  created_at,
  updated_at
FROM admin.brand_logo_assets
WHERE target_type = :'brand_target_type'
  AND (
    :'brand_q' = ''
    OR target_key ILIKE '%' || :'brand_q' || '%'
    OR target_label ILIKE '%' || :'brand_q' || '%'
    OR coalesce(source_domain, '') ILIKE '%' || :'brand_q' || '%'
    OR coalesce(source_url, '') ILIKE '%' || :'brand_q' || '%'
  )
ORDER BY is_primary DESC, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=brand_profile label=brand_family_rules route=/api/v1/admin/brands/franchises'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  f.id::text AS family_id,
  f.family_key,
  f.display_name,
  f.is_active,
  r.id::text AS rule_id,
  r.link_kind,
  r.coverage_type,
  r.priority,
  r.updated_at
FROM admin.brand_families AS f
LEFT JOIN admin.brand_family_link_rules AS r
  ON r.family_id = f.id
 AND r.is_active = true
WHERE f.is_active = true
  AND (
    :'brand_q' = ''
    OR f.family_key ILIKE '%' || :'brand_q' || '%'
    OR f.display_name ILIKE '%' || :'brand_q' || '%'
  )
ORDER BY f.updated_at DESC NULLS LAST, r.priority ASC NULLS LAST
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=media_gallery label=entity_media_links route=/api/v1/admin/trr-api/shows/:show_id/assets'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
SELECT
  ml.id::text AS link_id,
  ml.entity_type,
  ml.entity_id::text,
  ml.kind,
  ml.position,
  ml.is_primary,
  ma.id::text AS media_asset_id,
  ma.source,
  ma.source_asset_id,
  ma.hosted_url,
  ma.created_at,
  ma.updated_at
FROM core.media_links AS ml
JOIN core.media_assets AS ma
  ON ma.id = ml.media_asset_id
WHERE ml.entity_type = :'media_entity_type'
  AND ml.entity_id = :'show_id'::uuid
  AND (:'media_kind' = '' OR ml.kind = :'media_kind')
ORDER BY ml.is_primary DESC, ml.position ASC NULLS LAST, ma.updated_at DESC NULLS LAST
LIMIT :safe_limit OFFSET :safe_offset;

\echo 'hot_path=admin_show_reads label=recent_show_search route=/api/v1/admin/trr-api/shows and /search'
EXPLAIN (ANALYZE :explain_analyze, BUFFERS :explain_analyze, VERBOSE true, COSTS true, SETTINGS true, FORMAT TEXT)
WITH shows_with_slug AS (
  SELECT
    s.id,
    s.name,
    s.slug,
    COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
    s.show_total_seasons,
    s.show_total_episodes,
    s.primary_poster_image_id,
    lower(
      trim(
        both '-' FROM regexp_replace(
          regexp_replace(COALESCE(s.name, ''), '&', ' and ', 'gi'),
          '[^a-z0-9]+',
          '-',
          'gi'
        )
      )
    ) AS computed_slug,
    COUNT(*) OVER (
      PARTITION BY lower(
        trim(
          both '-' FROM regexp_replace(
            regexp_replace(COALESCE(s.name, ''), '&', ' and ', 'gi'),
            '[^a-z0-9]+',
            '-',
            'gi'
          )
        )
      )
    ) AS slug_collision_count
  FROM core.shows AS s
)
SELECT
  s.id::text,
  s.name,
  s.slug,
  s.alternative_names,
  s.show_total_seasons,
  s.show_total_episodes,
  CASE
    WHEN s.slug_collision_count > 1
      THEN COALESCE(NULLIF(s.slug, ''), s.computed_slug) || '--' || lower(left(s.id::text, 8))
    ELSE COALESCE(NULLIF(s.slug, ''), s.computed_slug)
  END AS canonical_slug,
  poster.hosted_url AS poster_url
FROM shows_with_slug AS s
LEFT JOIN core.show_images AS poster
  ON poster.id = s.primary_poster_image_id
WHERE :'search_q' = ''
   OR s.name ILIKE '%' || :'search_q' || '%'
   OR s.slug ILIKE '%' || :'search_q' || '%'
   OR EXISTS (
      SELECT 1
      FROM unnest(s.alternative_names) AS alternative_name(name)
      WHERE alternative_name.name ILIKE '%' || :'search_q' || '%'
   )
ORDER BY s.name ASC
LIMIT :safe_limit OFFSET :safe_offset;

ROLLBACK;
