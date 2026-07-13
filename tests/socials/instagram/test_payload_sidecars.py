from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from trr_backend.socials.instagram import payload_sidecars


def test_owned_transaction_propagates_failure_to_connection_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"rolled_back": False}

    @contextmanager
    def _fake_connection(*, label: str):
        assert label == "payload-test"
        try:
            yield "transaction"
        except RuntimeError:
            state["rolled_back"] = True
            raise

    monkeypatch.setattr(payload_sidecars.pg, "db_connection", _fake_connection)
    with pytest.raises(RuntimeError, match="sidecar failed"):
        with payload_sidecars.payload_write_transaction(None, label="payload-test") as conn:
            assert conn == "transaction"
            raise RuntimeError("sidecar failed")
    assert state["rolled_back"] is True


def test_read_mode_defaults_to_legacy_and_rejects_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(payload_sidecars.PAYLOAD_READ_MODE_ENV, raising=False)
    assert payload_sidecars.payload_read_mode() == "legacy"
    monkeypatch.setenv(payload_sidecars.PAYLOAD_READ_MODE_ENV, "compare")
    assert payload_sidecars.payload_read_mode() == "compare"
    monkeypatch.setenv(payload_sidecars.PAYLOAD_READ_MODE_ENV, "surprise")
    assert payload_sidecars.payload_read_mode() == "legacy"


def test_read_mode_resolution_serves_legacy_until_sidecar_cutover() -> None:
    legacy = {"source": "legacy"}
    sidecar = {"source": "sidecar"}
    assert payload_sidecars.payload_for_read_mode(legacy=legacy, sidecar=sidecar, mode="legacy") is legacy
    assert payload_sidecars.payload_for_read_mode(legacy=legacy, sidecar=sidecar, mode="compare") is legacy
    assert payload_sidecars.payload_for_read_mode(legacy=legacy, sidecar=sidecar, mode="sidecar") is sidecar
    assert payload_sidecars.payload_for_read_mode(legacy=legacy, sidecar=None, mode="sidecar") is legacy


def test_post_sidecar_preserves_nullable_raw_data_and_seeds_best_timestamp() -> None:
    observed_at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    payload = payload_sidecars.post_sidecar_payload(
        legacy_row={"id": "11111111-1111-4111-8111-111111111111"},
        payload={"raw_data": None, "metadata_scraped_at": observed_at},
    )
    assert payload == {
        "post_id": "11111111-1111-4111-8111-111111111111",
        "raw_data": None,
        "asset_manifest": {},
        "child_posts_data": [],
        "payload_updated_at": observed_at,
    }


def test_seed_timestamp_normalizes_naive_and_offset_datetimes_to_utc() -> None:
    naive = datetime(2026, 7, 13, 12, 0)
    offset = datetime(2026, 7, 13, 12, 0, tzinfo=timezone(timedelta(hours=-4)))

    assert payload_sidecars._seed_timestamp(naive) == datetime(2026, 7, 13, 12, 0, tzinfo=UTC)  # noqa: SLF001
    assert payload_sidecars._seed_timestamp(offset) == datetime(2026, 7, 13, 16, 0, tzinfo=UTC)  # noqa: SLF001


def test_bulk_post_upsert_changes_timestamp_only_when_payload_is_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_execute(sql: str, values: list[tuple[Any, ...]], *, conn: Any) -> list[dict[str, Any]]:
        captured.update(sql=sql, values=values, conn=conn)
        return []

    monkeypatch.setattr(payload_sidecars.pg, "execute_values_returning", _fake_execute)
    payload_sidecars.upsert_post_payloads(
        [
            {
                "post_id": "11111111-1111-4111-8111-111111111111",
                "raw_data": {"rich": True},
                "asset_manifest": {},
                "child_posts_data": [],
                "payload_updated_at": None,
            },
            {
                "post_id": "22222222-2222-4222-8222-222222222222",
                "raw_data": {"rich": True},
                "asset_manifest": {},
                "child_posts_data": [],
                "payload_updated_at": None,
            },
        ],
        conn="transaction",
    )
    normalized = " ".join(captured["sql"].lower().split())
    assert len(captured["values"]) == 2
    assert "is distinct from" in normalized
    assert "then now()" in normalized
    assert "else social.instagram_post_payloads.payload_updated_at" in normalized
    assert captured["conn"] == "transaction"


def test_bulk_post_upsert_sends_rich_payload_values_exactly(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    rich_raw = {"carousel_media": [{"pk": "slide-1", "width": 1080}]}
    rich_manifest = {"original": {"sha256": "abc123", "bytes": 12345}}
    rich_children = [{"slide_index": 0, "image": {"url": "https://cdn.test/slide.jpg"}}]
    monkeypatch.setattr(
        payload_sidecars.pg,
        "execute_values_returning",
        lambda sql, values, *, conn: captured.update(sql=sql, values=values, conn=conn) or [],
    )

    payload_sidecars.upsert_post_payloads(
        [
            {
                "post_id": "11111111-1111-4111-8111-111111111111",
                "raw_data": rich_raw,
                "asset_manifest": rich_manifest,
                "child_posts_data": rich_children,
                "payload_updated_at": datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
            }
        ],
        conn="transaction",
    )

    values = captured["values"][0]
    assert values[1].adapted == rich_raw
    assert values[2].adapted == rich_manifest
    assert values[3].adapted == rich_children
    normalized = " ".join(captured["sql"].lower().split())
    assert "else social.instagram_post_payloads.payload_updated_at" in normalized


def test_bulk_catalog_upsert_uses_one_statement_without_legacy_emptying(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        payload_sidecars.pg,
        "execute_values_returning",
        lambda sql, values, *, conn: captured.update(sql=sql, values=values, conn=conn) or [],
    )
    payload_sidecars.upsert_catalog_payloads(
        [
            {
                "catalog_post_id": "11111111-1111-4111-8111-111111111111",
                "raw_data": {},
                "child_posts_data": [],
                "payload_updated_at": None,
            }
        ],
        conn=object(),
    )
    normalized = " ".join(captured["sql"].lower().split())
    assert "insert into social.instagram_account_catalog_post_payloads" in normalized
    assert "is distinct from" in normalized
    assert "update social.instagram_account_catalog_posts" not in normalized

