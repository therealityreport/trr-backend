from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from scripts.db import backfill_instagram_payload_sidecars as backfill


def test_backfill_sql_is_bounded_keyset_and_resumable() -> None:
    sql = " ".join(backfill.build_batch_sql("posts", dry_run=False).lower().split())
    assert "left join social.instagram_post_payloads s on s.post_id = p.id" in sql
    assert "s.post_id is null or not" in sql
    assert "p.id > %(after_id)s::uuid" in sql
    assert "order by p.id limit %(batch_size)s" in sql
    assert "for update of p" in sql
    assert "skip locked" not in sql
    assert "on conflict (post_id) do update" in sql
    assert "is distinct from" in sql
    assert "else social.instagram_post_payloads.payload_updated_at" in sql


def test_backfill_catalog_seeds_all_exact_payload_values_without_emptying_legacy() -> None:
    sql = " ".join(backfill.build_batch_sql("catalog", dry_run=False).lower().split())
    assert "p.raw_data" in sql
    assert "p.child_posts_data" in sql
    assert "coalesce(p.updated_at, p.last_seen_at, p.created_at, now())" in sql
    assert "update social.instagram_account_catalog_posts" not in sql


def test_dry_run_is_read_only_and_status_checks_full_parity() -> None:
    dry_run_sql = " ".join(backfill.build_batch_sql("posts", dry_run=True).lower().split())
    assert dry_run_sql.startswith("select p.id::text")
    assert "insert into" not in dry_run_sql
    status_sql = " ".join(backfill.build_status_sql("posts").lower().split())
    assert "missing_count" in status_sql
    assert "orphan_count" in status_sql
    assert "mismatch_count" in status_sql
    assert "pending_count" in status_sql
    assert "is not distinct from" in status_sql
    assert "min(" not in status_sql
    assert (
        "(select p.id::text from social.instagram_posts p "
        "left join social.instagram_post_payloads s on s.post_id = p.id "
        "where s.post_id is null or not "
        "(s.raw_data is not distinct from p.raw_data and "
        "s.asset_manifest is not distinct from p.asset_manifest and "
        "s.child_posts_data is not distinct from p.child_posts_data) "
        "order by p.id limit 1) as next_pending_id"
    ) in status_sql
    assert "max(post_id)" not in status_sql


def test_status_maps_ordered_next_pending_id_and_null_when_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    next_pending_id = "11111111-1111-4111-8111-111111111111"
    reports = {
        "social.instagram_posts": {
            "base_count": 2,
            "sidecar_count": 1,
            "missing_count": 1,
            "orphan_count": 0,
            "mismatch_count": 0,
            "pending_count": 1,
            "next_pending_id": next_pending_id,
        },
        "social.instagram_account_catalog_posts": {
            "base_count": 2,
            "sidecar_count": 2,
            "missing_count": 0,
            "orphan_count": 0,
            "mismatch_count": 0,
            "pending_count": 0,
            "next_pending_id": None,
        },
    }

    class FakeCursor:
        row: dict[str, Any]

        def execute(self, sql: str, params: Any = None) -> None:
            assert params is None
            self.row = next(report for table, report in reports.items() if table in sql)

        def fetchone(self) -> dict[str, Any]:
            return self.row

    @contextmanager
    def fake_connection(*_args: Any, **_kwargs: Any):
        yield object()

    @contextmanager
    def fake_cursor(*_args: Any, **_kwargs: Any):
        yield FakeCursor()

    monkeypatch.setattr(backfill.pg, "db_connection", fake_connection)
    monkeypatch.setattr(backfill.pg, "db_cursor", fake_cursor)
    monkeypatch.setattr(backfill, "_set_timeouts", lambda *_args, **_kwargs: None)

    result = backfill.status(
        ("posts", "catalog"),
        lock_timeout_ms=100,
        statement_timeout_ms=1000,
    )

    assert result["targets"]["posts"]["next_pending_id"] == next_pending_id
    assert result["targets"]["posts"]["converged"] is False
    assert result["targets"]["catalog"]["next_pending_id"] is None
    assert result["targets"]["catalog"]["converged"] is True


def test_sparse_sidecars_do_not_skip_lower_missing_rows_and_repeated_runs_converge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eligible_ids = [
        "11111111-1111-4111-8111-111111111111",
        "33333333-3333-4333-8333-333333333333",
    ]

    class FakeCursor:
        rows: list[dict[str, str]]

        def execute(self, sql: str, params: Any = None) -> None:
            if params is None or not isinstance(params, dict):
                self.rows = []
                return
            after_id = params["after_id"]
            candidates = [row_id for row_id in eligible_ids if after_id is None or row_id > after_id]
            selected = candidates[: params["batch_size"]]
            self.rows = [{"id": row_id} for row_id in selected]
            if "insert into" in sql.lower():
                for row_id in selected:
                    eligible_ids.remove(row_id)

        def fetchall(self) -> list[dict[str, str]]:
            return self.rows

    @contextmanager
    def fake_connection(*_args: Any, **_kwargs: Any):
        yield object()

    @contextmanager
    def fake_cursor(*_args: Any, **_kwargs: Any):
        yield FakeCursor()

    monkeypatch.setattr(backfill.pg, "db_connection", fake_connection)
    monkeypatch.setattr(backfill.pg, "db_cursor", fake_cursor)

    first = backfill.run_target(
        "posts",
        after_id=None,
        batch_size=1,
        max_rows=1,
        dry_run=False,
        lock_timeout_ms=100,
        statement_timeout_ms=1000,
    )
    second = backfill.run_target(
        "posts",
        after_id=None,
        batch_size=1,
        max_rows=1,
        dry_run=False,
        lock_timeout_ms=100,
        statement_timeout_ms=1000,
    )
    final = backfill.run_target(
        "posts",
        after_id=None,
        batch_size=1,
        max_rows=1,
        dry_run=False,
        lock_timeout_ms=100,
        statement_timeout_ms=1000,
    )

    assert first["last_processed_id"] == "11111111-1111-4111-8111-111111111111"
    assert second["last_processed_id"] == "33333333-3333-4333-8333-333333333333"
    assert final["rows_processed"] == 0
    assert eligible_ids == []


def test_resume_cursor_wraps_once_and_cannot_skip_lower_sparse_uuids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eligible_ids = [
        "11111111-1111-4111-8111-111111111111",
        "33333333-3333-4333-8333-333333333333",
    ]

    class FakeCursor:
        rows: list[dict[str, str]]

        def execute(self, sql: str, params: Any = None) -> None:
            if params is None or not isinstance(params, dict):
                self.rows = []
                return
            after_id = params["after_id"]
            candidates = [row_id for row_id in eligible_ids if after_id is None or row_id > after_id]
            selected = candidates[: params["batch_size"]]
            self.rows = [{"id": row_id} for row_id in selected]
            if "insert into" in sql.lower():
                for row_id in selected:
                    eligible_ids.remove(row_id)

        def fetchall(self) -> list[dict[str, str]]:
            return self.rows

    @contextmanager
    def fake_connection(*_args: Any, **_kwargs: Any):
        yield object()

    @contextmanager
    def fake_cursor(*_args: Any, **_kwargs: Any):
        yield FakeCursor()

    monkeypatch.setattr(backfill.pg, "db_connection", fake_connection)
    monkeypatch.setattr(backfill.pg, "db_cursor", fake_cursor)

    result = backfill.run_target(
        "posts",
        after_id="22222222-2222-4222-8222-222222222222",
        batch_size=1,
        max_rows=3,
        dry_run=False,
        lock_timeout_ms=100,
        statement_timeout_ms=1000,
    )

    assert result["rows_processed"] == 2
    assert result["wrapped_to_start"] is True
    assert result["complete_for_bound"] is True
    assert result["bound_exhausted"] is False
    assert eligible_ids == []


def test_cli_exposes_status_dry_run_resume_and_timeout_bounds() -> None:
    args = backfill.build_parser().parse_args(
        [
            "run",
            "--target",
            "catalog",
            "--dry-run",
            "--after-id",
            "11111111-1111-4111-8111-111111111111",
            "--batch-size",
            "25",
            "--max-rows",
            "100",
            "--lock-timeout-ms",
            "500",
            "--statement-timeout-ms",
            "5000",
        ]
    )
    assert args.dry_run is True
    assert args.after_id.startswith("11111111")
    assert args.batch_size == 25
    assert args.max_rows == 100
