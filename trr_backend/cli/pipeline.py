"""Pipeline CLI commands."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

import typer

app = typer.Typer(help="Pipeline orchestration commands")


@app.command()
def run(  # noqa: PLR0913
    from_stage: Annotated[int, typer.Option("--from", help="Starting stage (1-6)")] = 1,
    to_stage: Annotated[int, typer.Option("--to", help="Ending stage (1-6)")] = 6,
    resume: Annotated[UUID | None, typer.Option("--resume", help="Resume from run ID")] = None,
    show_id: Annotated[list[str] | None, typer.Option("--show-id", help="Filter by show ID")] = None,
    tmdb_id: Annotated[list[int] | None, typer.Option("--tmdb-id", help="Filter by TMDb ID")] = None,
    imdb_id: Annotated[list[str] | None, typer.Option("--imdb-id", help="Filter by IMDb ID")] = None,
    all_shows: Annotated[bool, typer.Option("--all", help="Process all shows")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="No database writes")] = False,
    force: Annotated[bool, typer.Option("--force", help="Ignore sync state and hash checks")] = False,
    skip_s3: Annotated[bool, typer.Option("--skip-s3", help="Skip S3 operations")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Verbose output")] = False,
) -> None:
    """Run pipeline stages sequentially."""
    from trr_backend.pipeline.models import RunConfig
    from trr_backend.pipeline.orchestrator import PipelineOrchestrator
    from trr_backend.pipeline.registry import STAGES
    from trr_backend.utils.env import load_env

    load_env()

    config = RunConfig(
        from_stage=from_stage,
        to_stage=to_stage,
        show_filters={
            "show_ids": show_id or [],
            "tmdb_ids": tmdb_id or [],
            "imdb_ids": imdb_id or [],
            "all": all_shows,
        },
        dry_run=dry_run,
        force=force,
        skip_s3=skip_s3,
        verbose=verbose,
    )

    orchestrator = PipelineOrchestrator(STAGES)
    run_id, results = orchestrator.run(config, resume_run_id=resume)

    typer.echo(f"\nRun ID: {run_id}")
    for result in results:
        if result.status.value == "success":
            icon = "\u2713"
        elif result.status.value == "failed":
            icon = "\u2717"
        else:
            icon = "\u25cb"
        typer.echo(f"  {icon} {result.stage_name}: {result.status.value}")


@app.command()
def status(run_id: UUID) -> None:
    """Show status of a pipeline run."""
    from trr_backend.db.supabase import create_supabase_admin_client
    from trr_backend.pipeline.repository import fetch_run_with_stages
    from trr_backend.utils.env import load_env

    load_env()

    db = create_supabase_admin_client()
    run = fetch_run_with_stages(db, run_id)
    if not run:
        typer.echo(f"Run {run_id} not found")
        raise typer.Exit(1)

    typer.echo(f"Run: {run['name']} ({run['status']})")
    typer.echo(f"  Created: {run.get('created_at', 'N/A')}")
    if run.get("started_at"):
        typer.echo(f"  Started: {run['started_at']}")
    if run.get("completed_at"):
        typer.echo(f"  Completed: {run['completed_at']}")
    if run.get("error_message"):
        typer.echo(f"  Error: {run['error_message']}")

    typer.echo("\nStages:")
    for stage in run.get("stages", []):
        status_str = stage["status"]
        if status_str == "success":
            icon = "\u2713"
        elif status_str == "failed":
            icon = "\u2717"
        elif status_str == "running":
            icon = "\u25b6"
        else:
            icon = "\u25cb"

        extra = ""
        if stage.get("items_processed"):
            extra = f" (processed={stage['items_processed']})"
        if stage.get("duration_ms"):
            extra += f" [{stage['duration_ms']}ms]"

        typer.echo(f"  {icon} {stage['stage_order']}. {stage['stage_name']}: {status_str}{extra}")


@app.command("list")
def list_runs(
    limit: Annotated[int, typer.Option(help="Number of runs to show")] = 10,
) -> None:
    """List recent pipeline runs."""
    from trr_backend.db.supabase import create_supabase_admin_client
    from trr_backend.pipeline.repository import list_runs as db_list_runs
    from trr_backend.utils.env import load_env

    load_env()

    db = create_supabase_admin_client()
    runs = db_list_runs(db, limit=limit)

    if not runs:
        typer.echo("No pipeline runs found")
        return

    typer.echo(f"{'ID':<10} {'Status':<12} {'Name':<30} {'Created'}")
    typer.echo("-" * 70)
    for run in runs:
        run_id_short = run["id"][:8]
        created = run.get("created_at", "")[:19] if run.get("created_at") else ""
        typer.echo(f"{run_id_short:<10} {run['status']:<12} {run['name']:<30} {created}")
