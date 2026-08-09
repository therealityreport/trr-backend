from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from trr_backend.repositories import media_watchers

WATCH_ID = "11111111-1111-1111-1111-111111111111"
SHOW_ID = "22222222-2222-2222-2222-222222222222"
SEASON_ID = "33333333-3333-3333-3333-333333333333"
BRAVO_ID = "44444444-4444-4444-4444-444444444444"
RUN_ID = "55555555-5555-5555-5555-555555555555"
ASSET_ID = "66666666-6666-6666-6666-666666666666"


def _migration_sql() -> str:
    return (
        Path(__file__).parents[2]
        / "supabase/migrations/20260806120000_show_season_media_watchers.sql"
    ).read_text()


def test_migration_is_additive_and_enforces_watcher_integrity_contract() -> None:
    sql = _migration_sql().lower()

    assert "create table if not exists core.show_season_media_watches" in sql
    assert "create table if not exists core.show_season_media_watch_baseline_generations" in sql
    assert "create table if not exists core.show_season_media_watch_runs" in sql
    assert "create table if not exists core.show_season_media_watch_observations" in sql
    assert "create table if not exists core.media_source_revisions" in sql
    assert "foreign key (season_id, show_id, target_season_number)" in sql
    assert "references core.seasons (id, show_id, season_number)" in sql
    assert "show_season_media_watches_active_identity_uq" in sql
    assert "where status = 'active'" in sql
    assert "lease_fence bigint not null default 0" in sql
    assert "for update skip locked" not in sql  # Repository owns the runtime claim.
    assert "drop table" not in sql
    assert "add column if not exists watch_id" in sql


def test_migration_keeps_the_60_second_default_valid_with_bounded_overlap() -> None:
    sql = _migration_sql().lower()

    assert "poll_interval_seconds integer not null default 60" in sql
    assert "overlap_seconds integer not null default 300" in sql
    assert "check (overlap_seconds between 0 and 3600)" in sql
    assert "overlap_seconds < poll_interval_seconds" not in sql


def test_migration_makes_rules_and_revision_content_immutable_and_service_role_only() -> None:
    sql = _migration_sql().lower()

    assert "enforce_show_season_media_watch_rules_immutable" in sql
    assert "watch qualification and source-season rules are immutable" in sql
    assert "enforce_media_source_revision_immutability" in sql
    assert "invalid immutable media source revision acquisition-state transition" in sql
    assert "alter table core.media_source_revisions enable row level security" in sql
    assert "media_source_revisions_service_role_all" in sql
    assert "revoke all on table core.show_season_media_watches" in sql


def test_create_watch_serializes_config_and_lets_composite_fk_enforce_show_season(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_execute(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured.update(sql=sql, params=params)
        return [{"id": WATCH_ID, "status": "active"}]

    monkeypatch.setattr(media_watchers.pg, "execute_returning", fake_execute)

    row = media_watchers.create_watch(
        show_id=SHOW_ID,
        season_id=SEASON_ID,
        target_season_number=7,
        nbcumv_show_id="490e731c-d85f-474f-945b-b9681dc1931b",
        bravo_show_uuid=BRAVO_ID,
        source_season_rules={"source": "season_number"},
        qualification_rules_version="v1",
        r2_prefix="shows/rhoslc/season-7",
        desktop_folder_name="RHOSLC-S7",
    )

    assert row == {"id": WATCH_ID, "status": "active"}
    assert "INSERT INTO core.show_season_media_watches" in str(captured["sql"])
    assert "%s::uuid, %s::uuid, %s::int" in str(captured["sql"])
    assert captured["params"][:5] == [
        SHOW_ID,
        SEASON_ID,
        7,
        "490e731c-d85f-474f-945b-b9681dc1931b",
        BRAVO_ID,
    ]
    assert '"source": "season_number"' in str(captured["params"][5])


def test_claim_and_heartbeat_use_skip_locked_and_the_current_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch(sql: str, params: list[object]) -> dict[str, object] | None:
        calls.append((sql, params))
        if "WITH candidate" in sql:
            return {"id": WATCH_ID, "lease_fence": 8}
        return {"id": WATCH_ID}

    monkeypatch.setattr(media_watchers.pg, "fetch_one", fake_fetch)

    claimed = media_watchers.claim_due_watch(lease_owner="modal-worker", lease_seconds=90)
    assert "watch.id::text AS id" in calls[0][0]
    heartbeated = media_watchers.heartbeat_lease(
        watch_id=WATCH_ID,
        lease_owner="modal-worker",
        lease_fence=8,
        lease_seconds=90,
    )

    assert claimed == {"id": WATCH_ID, "lease_fence": 8}
    assert heartbeated is True
    claim_sql, claim_params = calls[0]
    assert "FOR UPDATE SKIP LOCKED" in claim_sql
    assert "lease_fence = watch.lease_fence + 1" in claim_sql
    assert claim_params == ["modal-worker", 90]
    heartbeat_sql, heartbeat_params = calls[1]
    assert "lease_owner = %s" in heartbeat_sql
    assert "lease_fence = %s::bigint" in heartbeat_sql
    assert "lease_expires_at > now()" in heartbeat_sql
    assert heartbeat_params == [90, WATCH_ID, "modal-worker", 8]


def test_journal_and_revision_writes_require_the_current_fence_and_are_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch(sql: str, params: list[object]) -> dict[str, object] | None:
        calls.append((sql, params))
        return {"id": RUN_ID}

    monkeypatch.setattr(media_watchers.pg, "fetch_one", fake_fetch)

    journal = media_watchers.update_run_journal(
        run_id=RUN_ID,
        watch_id=WATCH_ID,
        lease_owner="worker-a",
        lease_fence=12,
        cursor_journal={"bravo": {"page": 3}},
    )
    assert "run.id::text AS id" in calls[0][0]
    revision = media_watchers.insert_source_revision(
        watch_id=WATCH_ID,
        lease_owner="worker-a",
        lease_fence=12,
        media_asset_id=ASSET_ID,
        source="bravo",
        source_asset_id="source-1",
        sha256="a" * 64,
        hosted_bucket="media",
        hosted_key="shows/rhoslc/revision.jpg",
    )

    assert journal == {"id": RUN_ID}
    assert revision == {"id": RUN_ID}
    for sql, _params in calls:
        assert "watch.lease_owner = %s" in sql
        assert "watch.lease_fence = %s::bigint" in sql
        assert "watch.lease_expires_at > now()" in sql
    assert "ON CONFLICT (media_asset_id, sha256) DO NOTHING" in calls[1][0]
    assert "core.media_source_revisions" in calls[1][0]


def test_baseline_generation_snapshots_rules_under_the_current_fence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_fetch(sql: str, params: list[object]) -> dict[str, object] | None:
        captured.update(sql=sql, params=params)
        return {"id": "77777777-7777-7777-7777-777777777777", "generation": 1}

    monkeypatch.setattr(media_watchers.pg, "fetch_one", fake_fetch)

    generation = media_watchers.start_baseline_generation(
        watch_id=WATCH_ID,
        lease_owner="worker-a",
        lease_fence=3,
        created_by="admin",
    )

    assert generation == {"id": "77777777-7777-7777-7777-777777777777", "generation": 1}
    assert "watch.qualification_rules_version" in str(captured["sql"])
    assert "watch.source_season_rules" in str(captured["sql"])
    assert "watch.lease_fence = %s::bigint" in str(captured["sql"])
    assert captured["params"] == ["admin", WATCH_ID, "worker-a", 3]


@contextmanager
def _connection() -> Iterator[object]:
    yield object()


def test_finish_run_only_advances_source_state_after_a_fenced_journal_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object], object]] = []

    monkeypatch.setattr(media_watchers.pg, "db_connection", lambda *, label: _connection())

    def fake_execute(sql: str, params: list[object], *, conn: object) -> list[dict[str, object]]:
        calls.append((sql, params, conn))
        return [{"id": RUN_ID}] if len(calls) == 1 else [{"id": WATCH_ID}]

    monkeypatch.setattr(media_watchers.pg, "execute_returning", fake_execute)

    result = media_watchers.finish_run(
        run_id=RUN_ID,
        watch_id=WATCH_ID,
        lease_owner="worker-a",
        lease_fence=3,
        status="completed",
        source_state_after={"bravo": {"cursor": "next"}},
        next_check_seconds=60,
    )

    assert result == {"id": RUN_ID}
    assert len(calls) == 2
    assert "UPDATE core.show_season_media_watch_runs AS run" in calls[0][0]
    assert "watch.lease_fence = %s::bigint" in calls[0][0]
    assert "UPDATE core.show_season_media_watches" in calls[1][0]
    assert "CASE WHEN %s = 'completed' THEN %s::jsonb ELSE source_state END" in calls[1][0]
    assert calls[0][2] is calls[1][2]


def test_pause_and_resume_fence_stale_workers_without_deleting_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch(sql: str, params: list[object]) -> dict[str, object]:
        calls.append((sql, params))
        return {"id": WATCH_ID}

    monkeypatch.setattr(media_watchers.pg, "fetch_one", fake_fetch)

    assert media_watchers.pause_watch(watch_id=WATCH_ID) == {"id": WATCH_ID}
    assert media_watchers.resume_watch(watch_id=WATCH_ID) == {"id": WATCH_ID}
    assert all("lease_fence = lease_fence + 1" in sql for sql, _params in calls)
    assert "DELETE" not in calls[0][0]


def test_rejects_missing_identity_before_database_work() -> None:
    with pytest.raises(ValueError, match="watch_id is required"):
        media_watchers.heartbeat_lease(
            watch_id="",
            lease_owner="worker-a",
            lease_fence=1,
        )
