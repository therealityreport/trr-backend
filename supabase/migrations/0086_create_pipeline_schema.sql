-- Migration: Create pipeline schema for run tracking and orchestration
-- This creates pipeline.runs + pipeline.run_stages for tracking pipeline execution
-- with resume support via input hash comparison and S3 manifest storage.

BEGIN;

CREATE SCHEMA IF NOT EXISTS pipeline;

-- Pipeline run tracking
CREATE TABLE pipeline.runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'success', 'failed', 'cancelled')),
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    error_message text NULL,
    error_stage text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX pipeline_runs_status_idx ON pipeline.runs (status);
CREATE INDEX pipeline_runs_created_at_idx ON pipeline.runs (created_at DESC);

-- Per-stage tracking (manifest keys stored here, not on runs)
CREATE TABLE pipeline.run_stages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES pipeline.runs(id) ON DELETE CASCADE,
    stage_name text NOT NULL,
    stage_order integer NOT NULL,  -- Global stage index (1-6), not relative

    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'skipped', 'success', 'failed')),

    -- Resume support via hash comparison
    input_hash text NULL,   -- SHA256 of (config + show_ids)
    output_hash text NULL,

    -- Manifest storage (S3 keys)
    manifest_key text NULL,  -- pipeline_runs/{run_id}/{stage}/manifest.json

    -- Timing
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    duration_ms integer NULL,

    -- Stats
    items_processed integer NULL,
    items_skipped integer NULL,
    items_failed integer NULL,

    -- Error tracking
    error_message text NULL,
    error_details jsonb NULL,

    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    UNIQUE (run_id, stage_name)
);

CREATE INDEX pipeline_run_stages_run_id_idx ON pipeline.run_stages (run_id);
CREATE INDEX pipeline_run_stages_status_idx ON pipeline.run_stages (status);

-- Triggers (using existing core.set_updated_at)
CREATE TRIGGER pipeline_runs_set_updated_at
BEFORE UPDATE ON pipeline.runs
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER pipeline_run_stages_set_updated_at
BEFORE UPDATE ON pipeline.run_stages
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

-- Grants (service_role only)
GRANT USAGE ON SCHEMA pipeline TO service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pipeline TO service_role;

-- RLS with explicit service_role policies
ALTER TABLE pipeline.runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline.run_stages ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all_runs"
ON pipeline.runs FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_run_stages"
ON pipeline.run_stages FOR ALL TO service_role
USING (true) WITH CHECK (true);

COMMENT ON TABLE pipeline.runs IS
'Pipeline run tracking. One row per orchestrator invocation.
Status: pending -> running -> (success|failed|cancelled)';

COMMENT ON TABLE pipeline.run_stages IS
'Per-stage tracking within a pipeline run.
Stores input_hash for resume logic (skip if hash matches prior success).
manifest_key points to S3 manifest JSON for auditing.';

COMMIT;
