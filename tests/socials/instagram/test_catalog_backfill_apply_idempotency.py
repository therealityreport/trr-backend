"""Regression tests for catalog-backfill Live-APPLY reservation idempotency.

A starved or retried submit that replays the same launch identity
(``launch_group_id`` carried on the placeholder config) must reuse the run it
already reserved under the catalog-start advisory lock instead of inserting a
duplicate ``social.scrape_runs`` row.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as social_repo


def _install_fake_pg(
    monkeypatch: pytest.MonkeyPatch,
    *,
    insert_calls: list[dict[str, Any]],
) -> None:
    """Stub the pg layer so the advisory lock acquires and INSERT is observable.

    Any ``insert into social.scrape_runs`` query is recorded in ``insert_calls``
    so the test can assert whether a second row would have been created.
    """

    fake_conn = object()

    @contextmanager
    def _fake_db_connection(*, pool_name: str = "default", **_kwargs):
        del pool_name
        yield fake_conn

    @contextmanager
    def _fake_db_cursor(*, conn: Any | None = None, label: str = "write-cursor"):
        del conn, label
        yield object()

    def _fake_fetch_one_with_cursor(cur: Any, query: str, params: list[Any] | None = None) -> dict[str, Any]:
        del cur
        normalized = " ".join(query.lower().split())
        if "pg_try_advisory_lock" in normalized:
            return {"locked": True}
        if "insert into social.scrape_runs" in normalized:
            insert_calls.append({"query": normalized, "params": params})
            return {"id": "freshly-inserted-run"}
        if "pg_advisory_unlock" in normalized:
            return {"unlocked": True}
        raise AssertionError(f"Unexpected query: {normalized}")

    monkeypatch.setattr(social_repo.pg, "db_connection", _fake_db_connection)
    monkeypatch.setattr(social_repo.pg, "db_cursor", _fake_db_cursor)
    monkeypatch.setattr(social_repo.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)


def test_reservation_dedupes_on_matching_launch_group_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replaying the same launch_group_id returns the existing run, inserts nothing."""

    launch_group_id = "launch-group-abc-123"
    existing_run_id = "catalog-run-existing-001"
    insert_calls: list[dict[str, Any]] = []
    _install_fake_pg(monkeypatch, insert_calls=insert_calls)

    # A prior reservation for this same launch identity already exists and is
    # still non-terminal (queued). The dedupe read should surface it.
    recent_rows_calls: list[dict[str, Any]] = []

    def _fake_catalog_recent_runs(platform: str, account_handle: str, *, limit: int = 10, conn: Any | None = None):
        recent_rows_calls.append({"platform": platform, "account_handle": account_handle, "conn": conn})
        return [
            {
                "run_id": existing_run_id,
                "status": "queued",
                "run_config": {"launch_group_id": launch_group_id},
            }
        ]

    monkeypatch.setattr(social_repo, "_catalog_recent_runs", _fake_catalog_recent_runs)

    def _unexpected_active(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("get_active_social_account_catalog_run should not run once dedupe fires")

    monkeypatch.setattr(social_repo, "get_active_social_account_catalog_run", _unexpected_active)

    payload = social_repo._reserve_social_account_catalog_launch(
        platform="instagram",
        account_handle="bravotv",
        source_scope="network",
        initiated_by="codex@thereality.report",
        placeholder_config={"launch_state": "pending", "launch_group_id": launch_group_id},
        initial_status="queued",
    )

    assert payload["run_id"] == existing_run_id
    assert payload["deduped"] is True
    # No second social.scrape_runs row was created.
    assert insert_calls == []
    # The dedupe read was scoped to the same platform/account under the held lock.
    assert recent_rows_calls
    assert recent_rows_calls[0]["platform"] == "instagram"
    assert recent_rows_calls[0]["account_handle"] == "bravotv"


def test_reservation_inserts_when_no_matching_launch_group_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fresh launch identity (no prior run) inserts exactly one row, deduped=False."""

    insert_calls: list[dict[str, Any]] = []
    _install_fake_pg(monkeypatch, insert_calls=insert_calls)

    # Recent runs exist but none share the incoming launch_group_id, and an
    # unrelated terminal run must not be treated as a dedupe hit.
    def _fake_catalog_recent_runs(platform: str, account_handle: str, *, limit: int = 10, conn: Any | None = None):
        del platform, account_handle, limit, conn
        return [
            {
                "run_id": "some-old-completed-run",
                "status": "completed",
                "run_config": {"launch_group_id": "launch-group-abc-123"},
            }
        ]

    monkeypatch.setattr(social_repo, "_catalog_recent_runs", _fake_catalog_recent_runs)
    monkeypatch.setattr(social_repo, "get_active_social_account_catalog_run", lambda *_a, **_k: None)

    payload = social_repo._reserve_social_account_catalog_launch(
        platform="instagram",
        account_handle="bravotv",
        source_scope="network",
        initiated_by="codex@thereality.report",
        placeholder_config={"launch_state": "pending", "launch_group_id": "launch-group-xyz-999"},
        initial_status="queued",
    )

    assert payload["run_id"] == "freshly-inserted-run"
    assert payload["deduped"] is False
    assert len(insert_calls) == 1


def test_reservation_skips_dedupe_when_no_launch_group_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a launch_group_id there is no stable identity, so the insert path runs."""

    insert_calls: list[dict[str, Any]] = []
    _install_fake_pg(monkeypatch, insert_calls=insert_calls)

    def _unexpected_recent(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("dedupe read must be skipped when launch_group_id is absent")

    monkeypatch.setattr(social_repo, "_catalog_recent_runs", _unexpected_recent)
    monkeypatch.setattr(social_repo, "get_active_social_account_catalog_run", lambda *_a, **_k: None)

    payload = social_repo._reserve_social_account_catalog_launch(
        platform="instagram",
        account_handle="bravotv",
        source_scope="network",
        initiated_by="codex@thereality.report",
        placeholder_config={"launch_state": "pending"},
        initial_status="queued",
    )

    assert payload["run_id"] == "freshly-inserted-run"
    assert payload["deduped"] is False
    assert len(insert_calls) == 1
