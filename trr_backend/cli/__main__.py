"""CLI entrypoint for TRR Backend.

Usage:
    python -m trr_backend.cli pipeline run --help
    python -m trr_backend.cli pipeline run --all --verbose
    python -m trr_backend.cli pipeline list
"""

from __future__ import annotations

import typer

from trr_backend.cli.pipeline import app as pipeline_app

app = typer.Typer(
    name="trr",
    help="TRR Backend CLI - Pipeline orchestration and utilities",
)

app.add_typer(pipeline_app, name="pipeline")

if __name__ == "__main__":
    app()
