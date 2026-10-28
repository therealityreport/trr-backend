#!/usr/bin/env python3
"""Prepare or launch the bounded January 2026 BravoTV run on the local worker.

The default is source-free planning. --apply creates a local-placed run through
the production command service; it does not start a worker or transfer sessions.
Use the verified installed release and its local credential environment.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from scripts.ingestion.validate_fixed_posts import _save
from trr_backend.ingestion.domain import Conflict, SourceProfile, fingerprint


def january_payload():
    return InstagramIngestionLaunchV2(
        action="backfill",
        execution_placement="local",
        source_scope="network",
        backfill_scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
        selected_tasks=["post_details", "comments", "media"],
        detail_worker_count=1,
        comments_worker_count=1,
        comments_enable_media_followups=False,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--apply", action="store_true")
    options = parser.parse_args(argv)
    payload = january_payload()
    profile = SourceProfile.for_authenticated_relay()
    request_hash = fingerprint(
        {
            "payload": payload.model_dump(mode="json"),
            "profile": profile.__dict__,
            "source_revision": options.source_revision,
        }
    )
    assert payload.date_start is not None
    assert payload.date_end is not None
    report = {
        "account": "bravotv",
        "execution_placement": "local",
        "request_hash": request_hash,
        "source_revision": options.source_revision,
        "comments_family": profile.comments_family,
        "date_start": payload.date_start.isoformat(),
        "date_end_exclusive": payload.date_end.isoformat(),
    }
    if not options.apply:
        print(json.dumps({**report, "status": "plan", "source_requests": 0, "runs_created": 0}))
        return 0
    try:
        from scripts.ingestion.local_driver import _project_dsn
        from trr_backend.db.connection import resolve_database_url_candidate_details
        from trr_backend.ingestion.composition import build_api
        from trr_backend.ingestion.credential_snapshot import CredentialSnapshot
        from trr_backend.ingestion.domain import RunIntent
        from trr_backend.ingestion.fixed_validation import require_exclusive_execution
        from trr_backend.ingestion.validation_gate import require_allowed_intent

        candidates = resolve_database_url_candidate_details()
        if not candidates or any(not _project_dsn(item["url"]) for item in candidates):
            raise Conflict("Business database must target the approved TRR project")
        CredentialSnapshot.from_environ()  # Pinned dedicated identity; never retained in state/output.
        service = build_api()
        if service is None or service.source_profile != profile:
            raise Conflict("Explicit authenticated WWW Relay launch configuration required")
        require_allowed_intent(
            RunIntent(
                account="bravotv",
                scope="bounded_window",
                date_start=payload.date_start,
                date_end=payload.date_end,
                source_profile=profile,
            )
        )
        state = json.loads(options.state.read_text()) if options.state.exists() else None
        if state is not None and state.get("request_hash") != request_hash:
            raise Conflict("Operator state belongs to a different frozen request")
        if state is None:
            state = {**report, "idempotency_key": "authenticated-local:" + uuid4().hex}
            _save(options.state, state)
        command_hash = fingerprint({"command": "start", "intent": payload.model_dump(mode="json")})
        previous = service.store.command_result("bravotv", state["idempotency_key"], command_hash)
        # Resolve an ambiguous create before the exclusive-run check. Otherwise
        # our own persisted run would block recovery of the original command.
        own_run_id = (previous or {}).get("run_id") or state.get("run_id")
        require_exclusive_execution(service.store, own_run_id or "00000000-0000-0000-0000-000000000000")
        result = service.launch(
            "bravotv", payload, idempotency_key=state["idempotency_key"], initiated_by="local-operator"
        )
        state.update(run_id=str(result.run_id), generation=result.generation)
        _save(options.state, state)
        print(
            json.dumps({**report, "status": "created", "run_id": str(result.run_id), "generation": result.generation})
        )
        return 0
    except Exception as error:
        # Exception details can include connection strings and credentials.
        print(json.dumps({**report, "status": "blocked", "error_type": type(error).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
