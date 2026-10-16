"""Explicit API composition boundary; this module does not install a runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

if TYPE_CHECKING:
    from api.schemas.v2.instagram_ingestion import (
        InstagramIngestionCapabilitiesV2,
        InstagramIngestionCommandV2,
        InstagramIngestionDiagnosticsV2,
        InstagramIngestionLaunchV2,
        InstagramIngestionProgressV2,
        InstagramRecentRunsV2,
        InstagramReplayResultV2,
        InstagramReplayV2,
        InstagramRetryTargetsV2,
    )


class IngestionAPI(Protocol):
    """Account-scoped operations installed as app.state.instagram_ingestion.

    Implementations must scope run/evidence lookup to handle, keep all GET
    operations read-only, and enforce durable idempotency on every command.
    Full-history bound clearing and sync-newer frontier selection belong to
    the command service. The HTTP boundary never retries commands.
    """

    def capabilities(self, handle: str) -> InstagramIngestionCapabilitiesV2: ...

    def launch(
        self, handle: str, payload: InstagramIngestionLaunchV2, *, idempotency_key: str, initiated_by: str
    ) -> InstagramIngestionCommandV2: ...

    def progress(self, handle: str, run_id: UUID) -> InstagramIngestionProgressV2: ...

    def recent(self, handle: str, *, source_scope: str, limit: int) -> InstagramRecentRunsV2: ...

    def control(
        self,
        handle: str,
        run_id: UUID,
        *,
        action: Literal["cancel", "resume"],
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramIngestionCommandV2: ...

    def retry(
        self,
        handle: str,
        run_id: UUID,
        payload: InstagramRetryTargetsV2,
        *,
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramIngestionCommandV2: ...

    def diagnostics(self, handle: str, run_id: UUID, *, limit: int) -> InstagramIngestionDiagnosticsV2: ...

    def replay(
        self,
        handle: str,
        run_id: UUID,
        payload: InstagramReplayV2,
        *,
        idempotency_key: str,
        initiated_by: str,
    ) -> InstagramReplayResultV2: ...
