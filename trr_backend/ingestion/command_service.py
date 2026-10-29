"""Explicit Instagram command service; all infrastructure adapters are injected.

No provider request or workflow dispatch occurs here. Store.start atomically
creates business state and an outbox item. Replay requires an injected command
adapter that owns durable idempotency for previews as well as apply operations.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import copy
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID

from api.schemas.v2.instagram_ingestion import (
    InstagramIngestionCapabilitiesV2,
    InstagramIngestionCommandV2,
    InstagramIngestionDiagnosticsV2,
    InstagramIngestionLaunchV2,
    InstagramIngestionProgressV2,
    InstagramRecentRunsQueryV2,
    InstagramRecentRunsV2,
    InstagramReplayResultV2,
    InstagramReplayV2,
    InstagramRetryTargetsV2,
    SourceScope,
)
from trr_backend.ingestion.domain import (
    Conflict,
    IngestionError,
    RunIntent,
    SourceProfile,
    Stage,
    fingerprint,
    utc_timestamp,
)
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.queries import Queries


class ServiceUnavailableError(IngestionError):
    code = "INGESTION_SERVICE_UNAVAILABLE"
    status = 503


class ReplayCommand(Protocol):
    """Persist request/result identity, including dry runs.

    Bind evidence to the account and requested run before any apply write;
    preserve capture timestamps and perform no source requests.
    """

    def __call__(
        self, handle: str, run_id: UUID, payload: InstagramReplayV2, *, idempotency_key: str, initiated_by: str
    ) -> InstagramReplayResultV2 | dict[str, Any]: ...


def _now() -> datetime:
    return datetime.now(UTC)


def _account(handle: str) -> str:
    return RunIntent(account=handle).account


def _key(value: str) -> str:
    normalized = value.strip()
    if not normalized or len(value) > 200:
        raise ValueError("Idempotency key must contain 1 to 200 characters")
    return normalized


def _command(value: dict[str, Any]) -> InstagramIngestionCommandV2:
    return InstagramIngestionCommandV2.model_validate(
        {key: value[key] for key in InstagramIngestionCommandV2.model_fields if key in value}
    )


def _api_run(value: dict[str, Any]) -> dict[str, Any]:
    """Keep internal transport selection out of the existing operator contract.

    Validate the complete frozen intent first. The stored hash continues to
    identify that complete intent; this projection never changes stored state.
    """
    intent = RunIntent.from_payload(value["intent"]).payload()
    intent.pop("source_profile", None)
    return {**value, "intent": intent}


class CommandService:
    def __init__(
        self,
        store: Store,
        queries: Queries,
        *,
        engine: Literal["dbos", "temporal"],
        workflow_version: str,
        enabled: bool = False,
        capacity: Callable[[str, InstagramIngestionLaunchV2], None] | None = None,
        frontier: Callable[[str, str], datetime | None] | None = None,
        clock: Callable[[], datetime] = _now,
        listing_max_attempts: int = 12,
        comments_max_attempts: int = 12,
        worker_compatible: Callable[[dict[str, Any]], bool] | None = None,
        replay_command: ReplayCommand | None = None,
        replay_apply_enabled: bool = False,
        source_profile: SourceProfile | None = None,
        local_worker_status: Callable[[], bool] | None = None,
    ):
        if engine not in {"dbos", "temporal"} or not workflow_version.strip():
            raise ValueError("An explicit engine and workflow version are required")
        self.store = store
        self.queries = queries
        self.engine: Literal["dbos", "temporal"] = engine
        self.workflow_version = workflow_version
        self.enabled = enabled
        self.capacity = capacity
        self.frontier = frontier
        self.clock = clock
        # Composition reads the established env contracts. Freeze the resulting
        # budgets into intent so later environment changes cannot alter a run.
        self.listing_max_attempts = max(1, listing_max_attempts)
        self.comments_max_attempts = min(12, max(1, comments_max_attempts))
        self.worker_compatible = worker_compatible
        self.replay_command = replay_command
        self.replay_apply_enabled = replay_apply_enabled
        if source_profile is not None and not isinstance(source_profile, SourceProfile):
            raise ValueError("Source profile must be an immutable SourceProfile")
        self.source_profile = source_profile
        self.local_worker_status = local_worker_status

    def _local_worker_online(self) -> bool | None:
        if self.local_worker_status is None:
            return None
        try:
            return bool(self.local_worker_status())
        except Exception:  # noqa: BLE001 - an unreadable status is reported as unknown
            return None

    def capabilities(self, handle: str) -> InstagramIngestionCapabilitiesV2:
        ready = self.enabled and self.capacity is not None
        return InstagramIngestionCapabilitiesV2(
            local_worker_online=self._local_worker_online() if ready else None,
            enabled=ready,
            account=_account(handle),
            engine=self.engine,
            workflow_version=self.workflow_version,
            supported_actions=(
                [
                    action
                    for action in ("backfill", "sync_recent", "sync_newer", "resume_tail")
                    if (action != "sync_newer" or self.frontier is not None)
                    and (action != "resume_tail" or _account(handle) != "bravotv")
                ]
                if ready
                else []
            ),
            replay_apply_enabled=self.replay_apply_enabled and self.replay_command is not None,
            disabled_reason=None if ready else "New-run routing or required capacity validation is disabled.",
        )

    def launch(
        self, handle: str, payload: InstagramIngestionLaunchV2, *, idempotency_key: str, initiated_by: str
    ) -> InstagramIngestionCommandV2:
        account = _account(handle)
        key = _key(idempotency_key)
        request_payload = payload.model_dump(mode="json")
        placement = request_payload.pop("execution_placement", "modal")
        if placement != "modal":
            # Default Modal launches keep their historical command fingerprint.
            request_payload["execution_placement"] = placement
        command_hash = fingerprint({"command": "start", "intent": request_payload})
        previous = self.store.command_result(account, key, command_hash)
        if previous is not None:
            return _command(previous)
        if not self.capabilities(account).enabled or self.capacity is None:
            raise ServiceUnavailableError("New-run routing requires an enabled service and capacity validator")
        if payload.apply_run_id is not None:
            raise Conflict("apply_run_id is not supported by the rebuilt launch command")
        if placement == "local" and not (
            self.source_profile is not None
            and (
                self.source_profile.access_mode == "public"
                or self.source_profile == SourceProfile.for_authenticated_relay()
            )
        ):
            raise Conflict("Mac (local) runs require the explicitly public or authenticated WWW Relay source profile")
        # The capacity adapter validates the existing worker-count and canary
        # confirmation rules. It must raise on rejection; no defaults bypass it.
        self.capacity(account, payload.model_copy(deep=True))
        now = datetime.fromisoformat(utc_timestamp(self.clock()))
        start, end = payload.date_start, payload.date_end
        tasks = tuple(Stage(task) for task in payload.selected_tasks)
        scope = payload.backfill_scope
        if payload.action == "resume_tail":
            scope, start, end = "full_history", None, None
            tasks = (Stage.DETAILS, Stage.COMMENTS, Stage.MEDIA)
        elif payload.action == "sync_recent":
            scope, start, end = "recent_window", now - timedelta(days=payload.lookback_days), now
        elif payload.action == "sync_newer":
            if self.frontier is None:
                raise ServiceUnavailableError("Stored-frontier adapter is not configured")
            start = self.frontier(account, payload.source_scope)
            if start is None:
                raise Conflict("Initial backfill is required before sync_newer")
            start = datetime.fromisoformat(utc_timestamp(start))
            if start >= now:
                raise Conflict("Stored frontier must precede the current time")
            scope, end = "head_gap", now
        elif scope == "full_history":
            start, end = None, None
        intent = RunIntent(
            account=account,
            action=payload.action,
            source_scope=payload.source_scope,
            date_start=start,
            date_end=end,
            selected_tasks=tasks,
            scope=scope,
            detail_worker_count=payload.detail_worker_count,
            comments_worker_count=payload.comments_worker_count,
            comments_enable_media_followups=(
                True if payload.comments_enable_media_followups is None else payload.comments_enable_media_followups
            ),
            force=payload.force_catalog_rediscovery,
            enable_cap4_canary=payload.enable_cap4_canary,
            listing_max_attempts=self.listing_max_attempts,
            comments_max_attempts=self.comments_max_attempts,
            media_max_attempts=3,
            source_profile=self.source_profile,
        )

        def initialize(conn, run_id):
            with conn.cursor() as cur:
                cur.execute(
                    """insert into social.instagram_ingestion_targets
                    (run_id,stage,target_key,max_attempts,checkpoint)
                    values(%s,'listing','page:root',%s,'{"cursor":null}'::jsonb)""",
                    (run_id, intent.listing_max_attempts),
                )
                if placement == "local":
                    # Same transaction as creation: no Modal executor ever sees
                    # this run as Modal-placed, so the placement fence holds.
                    cur.execute(
                        """update social.instagram_ingestion_runs set execution_placement='local'
                        where run_id=%s""",
                        (run_id,),
                    )

        return _command(
            self.store.start(
                intent,
                idempotency_key=key,
                engine=self.engine,
                workflow_version=self.workflow_version,
                initiated_by=initiated_by,
                request_payload=request_payload,
                initialize=initialize,
            )
        )

    def progress(self, handle: str, run_id: UUID) -> InstagramIngestionProgressV2:
        value = _api_run(self.queries.progress(str(run_id), _account(handle)))
        return InstagramIngestionProgressV2.model_validate(
            {key: value[key] for key in InstagramIngestionProgressV2.model_fields if key in value}
        )

    def recent(self, handle: str, *, source_scope: SourceScope, limit: int) -> InstagramRecentRunsV2:
        query = InstagramRecentRunsQueryV2(source_scope=source_scope, limit=limit)
        return InstagramRecentRunsV2.model_validate(
            {
                "runs": [
                    _api_run(run)
                    for run in self.queries.recent(_account(handle), source_scope=query.source_scope, limit=query.limit)
                ]
            }
        )

    def _compatible(self, handle: str, run_id: UUID) -> dict[str, Any]:
        run = self.queries.progress(str(run_id), handle)
        if run.get("execution_placement", "modal") == "local" and self._local_worker_online() is not True:
            raise ServiceUnavailableError("The compatible Mac worker must be online to resume or retry")
        compatible = (
            self.worker_compatible(run)
            if self.worker_compatible
            else (run["engine"] == self.engine and run["workflow_version"] == self.workflow_version)
        )
        if not compatible:
            raise Conflict("No compatible worker is configured for this pinned run")
        return run

    def _control_store(self, run: dict[str, Any]) -> Store:
        # Preserve injected validation/capacity subclasses without mutating the
        # shared service store used concurrently by other commands.
        store = copy(self.store)
        store.execution_placement = run.get("execution_placement", "modal")
        return store

    def control(
        self,
        handle: str,
        run_id: UUID,
        *,
        action: Literal["cancel", "resume"],
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramIngestionCommandV2:
        account, key = _account(handle), _key(idempotency_key)
        store = self.store
        if action == "resume":
            run = self._compatible(account, run_id)
            store = self._control_store(run)
        return _command(store.control(str(run_id), account, action=action, idempotency_key=key))

    def retry(
        self,
        handle: str,
        run_id: UUID,
        payload: InstagramRetryTargetsV2,
        *,
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramIngestionCommandV2:
        account, key = _account(handle), _key(idempotency_key)
        run = self._compatible(account, run_id)
        store = self._control_store(run)
        return _command(
            store.control(
                str(run_id),
                account,
                action="retry_targets",
                idempotency_key=key,
                selection=payload.model_dump(mode="json"),
            )
        )

    def diagnostics(self, handle: str, run_id: UUID, *, limit: int) -> InstagramIngestionDiagnosticsV2:
        return InstagramIngestionDiagnosticsV2.model_validate(
            self.queries.diagnostics(str(run_id), _account(handle), limit=limit)
        )

    def replay(
        self,
        handle: str,
        run_id: UUID,
        payload: InstagramReplayV2,
        *,
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramReplayResultV2:
        account, key = _account(handle), _key(idempotency_key)
        self.queries.progress(str(run_id), account)
        if self.replay_command is None or (payload.apply and not self.replay_apply_enabled):
            raise ServiceUnavailableError("A durable replay command adapter is not enabled")
        result = InstagramReplayResultV2.model_validate(
            self.replay_command(account, run_id, payload, idempotency_key=key, initiated_by=initiated_by)
        )
        if result.run_id != run_id or result.evidence_id != payload.evidence_id:
            raise Conflict("Replay evidence does not belong to the requested run")
        if result.mode != ("apply" if payload.apply else "dry_run") or result.parser_version != payload.parser_version:
            raise Conflict("Replay result does not match the requested mode or parser")
        return result
