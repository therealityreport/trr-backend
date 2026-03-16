-- SocialBlade growth data storage
-- Replaces file-based storage (data/socialblade/{handle}.json) with database-backed rows.

CREATE TABLE IF NOT EXISTS pipeline.socialblade_growth_data (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  person_id       uuid        NOT NULL REFERENCES core.people(id) ON DELETE CASCADE,
  instagram_handle text       NOT NULL,
  scraped_at      timestamptz NOT NULL DEFAULT now(),
  stats_refreshed boolean     DEFAULT false,
  profile_stats   jsonb       NOT NULL DEFAULT '{}',
  rankings        jsonb       NOT NULL DEFAULT '{}',
  daily_channel_metrics_60day jsonb NOT NULL DEFAULT '{}',
  daily_total_followers_chart  jsonb,
  raw_response    jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (person_id, instagram_handle)
);

COMMENT ON TABLE pipeline.socialblade_growth_data
  IS 'Stores SocialBlade scraped growth data per person/handle. One row per person+handle, upserted on each scrape.';
