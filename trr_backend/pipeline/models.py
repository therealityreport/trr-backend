"""Pipeline data models for run tracking and orchestration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID


class RunStatus(StrEnum):
    """Status of a pipeline run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageStatus(StrEnum):
    """Status of a pipeline stage."""

    PENDING = "pending"
    RUNNING = "running"
    SKIPPED = "skipped"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class RunConfig:
    """Configuration for a pipeline run."""

    from_stage: int = 1
    to_stage: int = 6
    show_filters: dict[str, Any] = field(default_factory=dict)
    dry_run: bool = False
    force: bool = False
    skip_s3: bool = False
    verbose: bool = False


@dataclass
class RunContext:
    """Runtime context passed between stages."""

    run_id: UUID
    config: RunConfig
    db: Any  # Supabase client
    s3_client: Any | None = None

    # Accumulated state
    show_ids: list[str] = field(default_factory=list)
    artifacts: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def compute_stage_input_hash(self, stage_name: str) -> str:
        """
        Compute stage-specific input hash for resume logic.

        Stage 1: hash(show_filters, dry_run) - show_ids not yet known
        Stage 2+: hash(show_filters, show_ids, dry_run)

        Excludes: force, from_stage, to_stage, verbose, skip_s3
        Includes: dry_run (prevents resuming dry-run → real run)
        """
        if stage_name == "01_collect":
            data = {
                "show_filters": self.config.show_filters,
                "dry_run": self.config.dry_run,
            }
        else:
            data = {
                "show_filters": self.config.show_filters,
                "show_ids": sorted(self.show_ids),
                "dry_run": self.config.dry_run,
            }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


@dataclass
class StageResult:
    """Result from a single stage execution."""

    stage_name: str
    status: StageStatus

    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: int | None = None

    items_processed: int = 0
    items_skipped: int = 0
    items_failed: int = 0

    input_hash: str | None = None
    output_hash: str | None = None
    manifest_key: str | None = None

    error_message: str | None = None
    error_details: dict[str, Any] | None = None


@dataclass
class StageManifest:
    """Manifest describing stage inputs/outputs for resume and auditing."""

    run_id: str
    stage_name: str
    timestamp: str

    input_hash: str
    output_hash: str | None = None

    show_ids: list[str] = field(default_factory=list)
    items_processed: int = 0
    items_skipped: int = 0
    items_failed: int = 0

    config: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize manifest to dictionary for JSON storage."""
        return {
            "run_id": self.run_id,
            "stage_name": self.stage_name,
            "timestamp": self.timestamp,
            "input_hash": self.input_hash,
            "output_hash": self.output_hash,
            "show_ids": self.show_ids,
            "items_processed": self.items_processed,
            "items_skipped": self.items_skipped,
            "items_failed": self.items_failed,
            "config": self.config,
        }
