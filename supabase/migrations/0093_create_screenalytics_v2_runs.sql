-- Migration: Screenalytics v2 runs schema + tables

BEGIN;

CREATE SCHEMA IF NOT EXISTS screenalytics;

-- Video assets for screenalytics runs (episode/season/show assets)
CREATE TABLE screenalytics.video_assets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id uuid NULL REFERENCES core.episodes(id) ON DELETE CASCADE,
    season_id uuid NULL REFERENCES core.seasons(id) ON DELETE CASCADE,
    show_id uuid NULL REFERENCES core.shows(id) ON DELETE CASCADE,
    media_asset_id uuid NULL REFERENCES core.media_assets(id) ON DELETE SET NULL,
    source_url text NULL,
    duration_seconds numeric NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (episode_id IS NOT NULL OR season_id IS NOT NULL OR show_id IS NOT NULL),
    CHECK (media_asset_id IS NOT NULL OR source_url IS NOT NULL)
);

CREATE INDEX screenalytics_video_assets_episode_idx ON screenalytics.video_assets (episode_id);
CREATE INDEX screenalytics_video_assets_season_idx ON screenalytics.video_assets (season_id);
CREATE INDEX screenalytics_video_assets_show_idx ON screenalytics.video_assets (show_id);

-- Run tracking for v2 visual pipeline
CREATE TABLE screenalytics.runs_v2 (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    video_asset_id uuid NOT NULL REFERENCES screenalytics.video_assets(id) ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'success', 'failed', 'cancelled')),
    run_config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_hash text NULL,
    candidate_cast_snapshot_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    manifest_key text NULL,
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    error_message text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX screenalytics_runs_v2_status_idx ON screenalytics.runs_v2 (status);
CREATE INDEX screenalytics_runs_v2_video_asset_idx ON screenalytics.runs_v2 (video_asset_id);

-- Artifact references for run traceability
CREATE TABLE screenalytics.run_artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    artifact_key text NOT NULL,
    artifact_kind text NOT NULL,
    s3_key text NOT NULL,
    schema_version text NULL,
    content_type text NULL,
    checksum_sha256 text NULL,
    row_count bigint NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, artifact_key)
);

CREATE INDEX screenalytics_run_artifacts_run_idx ON screenalytics.run_artifacts (run_id);

-- Per-person metrics for a run (leaderboard, etc.)
CREATE TABLE screenalytics.run_person_metrics (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    person_id uuid NOT NULL REFERENCES core.people(id) ON DELETE RESTRICT,
    screen_time_seconds numeric NOT NULL DEFAULT 0,
    frame_count integer NOT NULL DEFAULT 0,
    confidence_avg numeric NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, person_id)
);

CREATE INDEX screenalytics_run_person_metrics_run_idx ON screenalytics.run_person_metrics (run_id);
CREATE INDEX screenalytics_run_person_metrics_person_idx ON screenalytics.run_person_metrics (person_id);

-- Unknown clusters (UI + assignment)
CREATE TABLE screenalytics.unknown_clusters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    cluster_id text NOT NULL,
    track_count integer NOT NULL DEFAULT 0,
    preview_s3_key text NULL,
    assigned_person_id uuid NULL REFERENCES core.people(id) ON DELETE RESTRICT,
    assigned_by text NULL,
    assigned_at timestamptz NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, cluster_id)
);

CREATE INDEX screenalytics_unknown_clusters_run_idx ON screenalytics.unknown_clusters (run_id);
CREATE INDEX screenalytics_unknown_clusters_unassigned_idx
    ON screenalytics.unknown_clusters (run_id)
    WHERE assigned_person_id IS NULL;

-- Triggers (using existing core.set_updated_at)
CREATE TRIGGER screenalytics_video_assets_set_updated_at
BEFORE UPDATE ON screenalytics.video_assets
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_runs_v2_set_updated_at
BEFORE UPDATE ON screenalytics.runs_v2
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_run_artifacts_set_updated_at
BEFORE UPDATE ON screenalytics.run_artifacts
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_run_person_metrics_set_updated_at
BEFORE UPDATE ON screenalytics.run_person_metrics
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_unknown_clusters_set_updated_at
BEFORE UPDATE ON screenalytics.unknown_clusters
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

-- Grants (service_role only)
GRANT USAGE ON SCHEMA screenalytics TO service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA screenalytics TO service_role;

-- RLS with explicit service_role policies
ALTER TABLE screenalytics.video_assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.runs_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.run_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.run_person_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.unknown_clusters ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all_video_assets"
ON screenalytics.video_assets FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_runs_v2"
ON screenalytics.runs_v2 FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_run_artifacts"
ON screenalytics.run_artifacts FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_run_person_metrics"
ON screenalytics.run_person_metrics FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_unknown_clusters"
ON screenalytics.unknown_clusters FOR ALL TO service_role
USING (true) WITH CHECK (true);

COMMIT;
