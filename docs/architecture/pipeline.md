# Pipeline Architecture

The TRR Backend pipeline orchestrator replaces the previous collection of independent scripts with a unified, resumable workflow.

## Overview

```
python -m trr_backend.cli pipeline run --all --verbose
```

The pipeline runs 6 sequential stages (internal stage names are prefixed, e.g. `01_collect`):

| Stage | Name | Description |
|-------|------|-------------|
| 1 | collect (`01_collect`) | Fetch show records based on filters |
| 2 | resolve (`02_resolve`) | Resolve missing TMDb/IMDb IDs |
| 3 | enrich (`03_enrich`) | Fetch metadata from external sources |
| 4 | mirror (`04_mirror`) | Upload media assets to S3 |
| 5 | deploy (`05_deploy`) | Update sync state, finalize |
| 6 | sync_screenalytics (`06_sync_screenalytics`) | Ingest Screenalytics results (stub) |

## CLI Commands

### Run Pipeline

```bash
# Run all stages for all shows
python -m trr_backend.cli pipeline run --all

# Run specific stages
python -m trr_backend.cli pipeline run --all --from 1 --to 3

# Run for specific shows
python -m trr_backend.cli pipeline run --tmdb-id 1396 --tmdb-id 1399
python -m trr_backend.cli pipeline run --imdb-id tt0903747

# Resume a failed run
python -m trr_backend.cli pipeline run --resume <run-id>

# Dry run (no DB writes)
python -m trr_backend.cli pipeline run --all --dry-run

# Skip S3 operations
python -m trr_backend.cli pipeline run --all --skip-s3

# Verbose output
python -m trr_backend.cli pipeline run --all --verbose
```

### Check Status

```bash
# Show run details
python -m trr_backend.cli pipeline status <run-id>

# List recent runs
python -m trr_backend.cli pipeline list
python -m trr_backend.cli pipeline list --limit 20
```

### Makefile Targets

```bash
make pipeline-run ARGS="--all --verbose"
make pipeline-run-all
make pipeline-status RUN_ID=<uuid>
make pipeline-list
```

## Resume Logic

The pipeline supports resuming interrupted runs via stage-specific input hash comparison:

### Stage-Specific Hash Computation

Each stage computes its own `input_hash` based on what it actually depends on:

| Stage | Hash Inputs |
|-------|-------------|
| Stage 1 (collect) | `show_filters`, `dry_run` |
| Stage 2-6 | `show_filters`, `show_ids`, `dry_run` |

**Excluded from hash:** `force`, `from_stage`, `to_stage`, `verbose`, `skip_s3`

**Why stage-specific?** Stage 1 doesn't know `show_ids` yet (it discovers them). Including `dry_run` prevents resuming a dry-run into a real run.

### Skip Conditions

When resuming (`--resume <run-id>`), a stage is skipped if ALL conditions are met:

1. Prior status = `success`
2. Prior `input_hash` matches current computed hash
3. `--force` flag is NOT set
4. Stage 1 ALWAYS requires `manifest_key` to exist (needed for hydration)
5. Other stages require `manifest_key` when `skip_s3=False`

### Context Hydration

When Stage 1 is skipped, `context.show_ids` is populated from the Stage 1 manifest. This ensures subsequent stages know which shows to process.

### Show Snapshot Behavior

**When resuming a run, Stage 1 uses the prior show snapshot from its manifest rather than re-expanding `--all`.** This ensures reproducible runs - the same set of shows is processed regardless of database changes since the original run.

To re-collect shows with current database contents, start a fresh run (omit `--resume`).

### Local Development with `--skip-s3`

Runs executed with `--skip-s3` do not persist manifests to S3. This affects resume behavior:

- **Stage 1 will always re-run on resume** because `manifest_key` is required for hydration (skip condition #4)
- **Other stages may skip on resume** if `skip_s3=True` was also used during the resume (skip condition #5 only applies when `skip_s3=False`)

This is intentional: local development runs can safely re-run Stage 1 to re-materialize `show_ids`. Production runs should always write manifests.

## Manifests

Each completed stage writes a manifest to S3:

**Location:** `s3://{bucket}/pipeline_runs/{run_id}/{stage_name}/manifest.json`

**Contents:**
```json
{
  "run_id": "uuid",
  "stage_name": "01_collect",
  "timestamp": "2024-01-28T12:00:00Z",
  "input_hash": "sha256...",
  "output_hash": "sha256...",
  "show_ids": ["uuid1", "uuid2"],
  "items_processed": 10,
  "items_skipped": 0,
  "items_failed": 0,
  "config": {
    "from_stage": 1,
    "to_stage": 6,
    "show_filters": {"all": true}
  }
}
```

## Database Schema

### pipeline.runs

Tracks overall pipeline execution:

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Primary key |
| name | text | Human-readable name |
| status | text | pending/running/success/failed/cancelled |
| config | jsonb | Run configuration |
| started_at | timestamptz | When run started |
| completed_at | timestamptz | When run completed |
| error_message | text | Error if failed |
| error_stage | text | Which stage failed |

### pipeline.run_stages

Tracks per-stage execution:

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Primary key |
| run_id | uuid | FK to runs |
| stage_name | text | e.g., "01_collect" |
| stage_order | int | Global ordering (1-6) |
| status | text | pending/running/skipped/success/failed |
| input_hash | text | SHA256 for resume logic |
| output_hash | text | Output content hash |
| manifest_key | text | S3 key to manifest |
| duration_ms | int | Execution time |
| items_processed | int | Count processed |
| items_skipped | int | Count skipped |
| items_failed | int | Count failed |
| error_message | text | Error if failed |

## Adding New Stages

1. Create `trr_backend/pipeline/stages/your_stage.py`:

```python
from datetime import UTC, datetime
from trr_backend.pipeline.models import RunContext, StageResult, StageStatus

def run(context: RunContext) -> StageResult:
    started_at = datetime.now(UTC)

    try:
        # Your logic here
        processed = 0

        completed_at = datetime.now(UTC)
        return StageResult(
            stage_name="07_your_stage",
            status=StageStatus.SUCCESS,
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=int((completed_at - started_at).total_seconds() * 1000),
            items_processed=processed,
        )
    except Exception as e:
        return StageResult(
            stage_name="07_your_stage",
            status=StageStatus.FAILED,
            started_at=started_at,
            error_message=str(e),
        )
```

2. Add to `trr_backend/pipeline/registry.py`:

```python
from trr_backend.pipeline.stages import your_stage

STAGES = [
    ...
    ("07_your_stage", your_stage.run),
]
```

3. Update `trr_backend/pipeline/repository.py` STAGE_ORDER:

```python
STAGE_ORDER = {
    ...
    "07_your_stage": 7,
}
```

## Related Migrations

- `0086_create_pipeline_schema.sql` (pipeline.runs, pipeline.run_stages)
- `0087_screenalytics_cast_views.sql` (core.v_episode_cast, core.v_season_cast)
- `0088_person_images_view.sql` (core.v_person_images)

## Screenalytics Integration

### Cast Views

The database provides views for Screenalytics candidate selection:

**core.v_episode_cast** - Who appears in each episode:
```sql
SELECT * FROM core.v_episode_cast
WHERE episode_id = '<uuid>'
```

**core.v_season_cast** - Distinct people in a season with episode counts:
```sql
SELECT * FROM core.v_season_cast
WHERE season_id = '<uuid>'
```

**core.v_person_images** - Person images for facebank seeding:
```sql
SELECT * FROM core.v_person_images
WHERE person_id = '<uuid>'
  AND is_primary = true
```

All views are granted to `service_role` only.

### Stage 6: Screenalytics Sync

Stage 6 is currently a stub. When Screenalytics manifest format is defined:

1. Read from Screenalytics outbox (S3 or API)
2. Find completed runs with manifests
3. Parse summary artifacts
4. Upsert results into TRR database

## Error Handling

- Stages catch exceptions and return `StageResult` with `FAILED` status
- Error details are stored in `pipeline.run_stages.error_details` as JSON
- The orchestrator stops on first failure
- Resume (`--resume`) skips successful stages and retries failed ones

## Best Practices

1. **Always use `--verbose`** during development to see stage progress
2. **Use `--dry-run --skip-s3`** to test without side effects
3. **Resume failed runs** instead of starting fresh (preserves audit trail)
4. **Monitor via `pipeline list`** to track recent runs
5. **Check manifests in S3** for debugging and auditing
