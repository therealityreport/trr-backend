from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from trr_backend.socials.control_plane import shared_source_config


def test_get_shared_account_sources_returns_persisted_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured.update(sql=sql, params=params)
        return [
            {
                "id": "source-1",
                "platform": "Instagram",
                "source_scope": "network",
                "account_handle": "@BravoTV",
                "is_active": True,
                "scrape_priority": 10,
                "metadata": {"display_name": "Bravo TV"},
                "last_scrape_status": None,
                "last_scrape_run_id": None,
                "last_scrape_job_id": None,
                "last_scrape_at": None,
                "last_classified_at": None,
                "updated_by": None,
                "created_at": None,
                "updated_at": None,
            }
        ]

    monkeypatch.setattr(shared_source_config.pg, "fetch_all", fetch_all)

    payload = shared_source_config.get_shared_account_sources(
        source_scope="bravo",
        include_inactive=False,
        platforms=["ig"],
    )

    assert captured["params"] == ["network", ["instagram"]]
    assert "is_active = true" in captured["sql"]
    assert payload["using_defaults"] is False
    assert payload["sources"][0]["platform"] == "instagram"
    assert payload["sources"][0]["account_handle"] == "bravotv"
    assert payload["sources"][0]["profile_kind"] == "network_streaming"


def test_get_shared_account_sources_uses_network_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shared_source_config.pg, "fetch_all", lambda *_args, **_kwargs: [])

    payload = shared_source_config.get_shared_account_sources(
        source_scope="network",
        platforms=["instagram"],
    )

    assert payload["using_defaults"] is True
    assert [source["account_handle"] for source in payload["sources"]] == [
        "bravotv",
        "bravodailydish",
        "bravowwhl",
    ]


def test_get_shared_account_sources_rejects_invalid_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shared_source_config.pg, "fetch_all", lambda *_args, **_kwargs: [])

    with pytest.raises(ValueError, match="INVALID_PLATFORM_FILTER"):
        shared_source_config.get_shared_account_sources(platforms=["linkedin"])


def test_put_shared_account_sources_upserts_and_deactivates_omitted_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[tuple[str, list[Any]]] = []
    inserted: list[list[Any]] = []
    connection = object()

    class Cursor:
        def execute(self, sql: str, params: list[Any]) -> None:
            executed.append((sql, params))

    @contextmanager
    def db_connection():
        yield connection

    @contextmanager
    def db_cursor(*, conn: object):
        assert conn is connection
        yield Cursor()

    monkeypatch.setattr(shared_source_config.pg, "db_connection", db_connection)
    monkeypatch.setattr(shared_source_config.pg, "db_cursor", db_cursor)
    monkeypatch.setattr(
        shared_source_config.pg,
        "fetch_one_with_cursor",
        lambda _cur, _sql, params: inserted.append(params) or {"id": "source-1"},
    )
    monkeypatch.setattr(
        shared_source_config,
        "get_shared_account_sources",
        lambda **_kwargs: {"source_scope": "network", "sources": [], "using_defaults": False},
    )

    payload = shared_source_config.put_shared_account_sources(
        source_scope="network",
        sources=[
            {
                "platform": "instagram",
                "account_handle": "@BravoTV",
                "is_active": True,
                "scrape_priority": 10,
                "metadata": {"display_name": "Bravo TV"},
            }
        ],
        updated_by="admin@example.com",
    )

    assert payload["source_scope"] == "network"
    assert inserted[0][0:5] == ["instagram", "network", "bravotv", True, 10]
    assert inserted[0][6] == "admin@example.com"
    assert len(executed) == 1
    assert "not ((platform = %s and account_handle = %s))" in executed[0][0]
    assert executed[0][1] == ["admin@example.com", "network", "instagram", "bravotv"]
