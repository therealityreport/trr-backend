"""Orchestrates parallel show refresh across Modal workers.

Builds execution waves from the target dependency graph, creates one
sub-operation per target, and dispatches independent targets concurrently.
"""

from __future__ import annotations

import logging
from typing import Any

from trr_backend.job_plane import is_remote_job_plane_enabled
from trr_backend.modal_dispatch import dispatch_admin_operation, supports_admin_operation
from trr_backend.pipeline.admin_operations import ensure_operation_execution
from trr_backend.repositories import admin_operations

logger = logging.getLogger(__name__)

# Dependency graph: target -> list of targets that must complete first.
# A target with an empty list can run in the first wave.
TARGET_DEPENDENCY_GRAPH: dict[str, list[str]] = {
    "show_core": [],
    "links": ["show_core"],
    "bravo": ["show_core"],
    "cast_profiles": ["show_core"],
    "cast_media": ["cast_profiles"],
    "official_images": ["bravo", "cast_profiles"],
}


def build_show_refresh_sub_operation_request_payload(
    *,
    parent_request_payload: dict[str, Any] | None,
    refresh_target: str,
    show_id: int | str | None = None,
    initiated_by: str | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build the canonical single-target payload for a show refresh sub-operation."""
    base_payload = dict(parent_request_payload) if isinstance(parent_request_payload, dict) else {}
    nested_payload = dict(base_payload.get("payload")) if isinstance(base_payload.get("payload"), dict) else {}

    nested_payload["targets"] = [refresh_target]
    for flag in ("skip_s3", "verbose", "reload_schema_cache", "force_new_operation"):
        nested_payload[flag] = bool(nested_payload.get(flag, False))

    child_payload = dict(base_payload)
    child_payload.pop("targets", None)
    child_payload["payload"] = nested_payload

    resolved_show_id = base_payload.get("show_id", show_id)
    if resolved_show_id is not None:
        child_payload["show_id"] = str(resolved_show_id)

    resolved_request_id = request_id if request_id is not None else base_payload.get("request_id")
    if resolved_request_id is not None:
        child_payload["request_id"] = str(resolved_request_id)

    resolved_initiated_by = initiated_by if initiated_by is not None else base_payload.get("initiated_by")
    if resolved_initiated_by is not None:
        child_payload["initiated_by"] = str(resolved_initiated_by)

    return child_payload


def execution_waves(targets: list[str]) -> list[list[str]]:
    """Sort targets into sequential waves respecting the dependency graph.

    Targets whose dependencies are satisfied (or not in the requested set)
    go into the earliest possible wave. Returns a list of waves, each
    containing targets that can execute concurrently.
    """
    if not targets:
        return []

    target_set = set(targets)
    remaining = set(targets)
    completed: set[str] = set()
    waves: list[list[str]] = []

    while remaining:
        wave = []
        for target in sorted(remaining):  # sorted for deterministic ordering
            deps = TARGET_DEPENDENCY_GRAPH.get(target, [])
            # A dependency is satisfied if it's completed OR not in the requested set
            if all(d in completed or d not in target_set for d in deps):
                wave.append(target)
        if not wave:
            # Safety valve: remaining targets have unsatisfiable deps — force them
            wave = sorted(remaining)
        for target in wave:
            remaining.discard(target)
        completed.update(wave)
        waves.append(wave)

    return waves


class ShowRefreshOrchestrator:
    """Creates and dispatches sub-operations for a show refresh."""

    def __init__(
        self,
        *,
        show_id: int | str,
        targets: list[str],
        initiated_by: str | None = None,
        request_payload: dict[str, Any] | None = None,
        request_id: str | None = None,
        client_session_id: str | None = None,
        client_workflow_id: str | None = None,
    ) -> None:
        self.show_id = show_id
        self.targets = targets
        self.initiated_by = initiated_by
        self.request_payload = request_payload or {}
        self.request_id = request_id
        self.client_session_id = client_session_id
        self.client_workflow_id = client_workflow_id
        self._parent_id: str | None = None
        self._sub_ops: dict[str, dict[str, Any]] = {}

    def create_operations(self) -> tuple[str, list[dict[str, Any]]]:
        """Create a parent operation and one sub-operation per target."""
        parent, _attached = admin_operations.create_or_attach_operation(
            operation_type="admin_show_refresh",
            request_payload=self.request_payload,
            initiated_by=self.initiated_by,
            request_id=self.request_id,
            client_session_id=self.client_session_id,
            client_workflow_id=self.client_workflow_id,
            allow_attach=True,
        )
        self._parent_id = str(parent["id"])

        sub_ops = []
        try:
            for target in self.targets:
                child = admin_operations.create_sub_operation(
                    parent_operation_id=self._parent_id,
                    operation_type="admin_show_refresh",
                    refresh_target=target,
                    request_payload=build_show_refresh_sub_operation_request_payload(
                        parent_request_payload=self.request_payload,
                        refresh_target=target,
                        show_id=self.show_id,
                        initiated_by=self.initiated_by,
                        request_id=self.request_id,
                    ),
                    initiated_by=self.initiated_by,
                    request_id=self.request_id,
                    client_session_id=self.client_session_id,
                    client_workflow_id=self.client_workflow_id,
                )
                self._sub_ops[target] = child
                sub_ops.append(child)
        except Exception:
            logger.exception(
                "Failed to create sub-operations: parent_id=%s created=%d/%d",
                self._parent_id,
                len(sub_ops),
                len(self.targets),
            )
            admin_operations.update_operation_status(self._parent_id, "failed")
            raise

        return self._parent_id, sub_ops

    def dispatch_wave(
        self,
        sub_ops: list[dict[str, Any]],
        *,
        producer_factory: Any | None = None,
    ) -> int:
        """Dispatch a wave of sub-operations.

        Returns the count dispatched to Modal. Local fallback requires a
        producer_factory; otherwise this raises to avoid leaving
        sub-operations pending with no executor.
        """
        modal_dispatched = 0
        op_type = "admin_show_refresh"
        modal_supported = supports_admin_operation(op_type)
        remote_enabled = is_remote_job_plane_enabled()

        for sub_op in sub_ops:
            op_id = str(sub_op["id"])
            target = str(sub_op.get("refresh_target") or "")

            if modal_supported and remote_enabled:
                dispatched = dispatch_admin_operation(
                    operation_id=op_id,
                    operation_type=op_type,
                )
                if dispatched:
                    modal_dispatched += 1
                    logger.info(
                        "Dispatched sub-operation to Modal: target=%s operation_id=%s parent=%s",
                        target,
                        op_id,
                        self._parent_id,
                    )
                    continue

            # Fallback: local execution
            if producer_factory is not None:
                producer = producer_factory(sub_op)
                ensure_operation_execution(op_id, producer=producer, request_id=self.request_id)
                logger.info(
                    "Local execution for sub-operation: target=%s operation_id=%s parent=%s",
                    target,
                    op_id,
                    self._parent_id,
                )
            else:
                raise RuntimeError(
                    f"Cannot execute sub-operation: Modal unavailable and no producer_factory provided. "
                    f"target={target} operation_id={op_id} parent={self._parent_id}"
                )

        return modal_dispatched

    def get_waves(self) -> list[list[dict[str, Any]]]:
        """Return sub-operations grouped into execution waves."""
        waves = execution_waves(self.targets)
        result = []
        for wave_targets in waves:
            wave_ops = [self._sub_ops[t] for t in wave_targets if t in self._sub_ops]
            if wave_ops:
                result.append(wave_ops)
        return result

    def update_parent_status(self) -> str:
        """Recompute and persist the parent's aggregated status."""
        if not self._parent_id:
            raise RuntimeError("No parent operation created yet")
        status = admin_operations.aggregate_parent_status(self._parent_id)
        if status in ("completed", "failed", "cancelled"):
            admin_operations.update_operation_status(self._parent_id, status)
        return status
