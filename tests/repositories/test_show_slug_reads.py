from __future__ import annotations

import pytest

from trr_backend.repositories import show_slug_reads

SHOW_ID = "11111111-1111-1111-1111-111111111111"


def test_get_show_by_exact_slug_is_one_exact_normalized_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_one(sql: str, params: list[object]) -> dict[str, object]:
        calls.append((sql, params))
        return {
            "id": SHOW_ID,
            "name": "The Real Housewives of Beverly Hills",
            "slug": "rhobh",
        }

    monkeypatch.setattr(show_slug_reads.pg, "fetch_one", fake_fetch_one)

    show, query_count = show_slug_reads.get_show_by_exact_slug("  RHOBH  ")

    assert query_count == 1
    assert len(calls) == 1
    sql, params = calls[0]
    normalized_sql = " ".join(sql.lower().split())
    assert "from core.shows" in normalized_sql
    assert "where core.shows.slug is not null" in normalized_sql
    assert "and btrim(core.shows.slug) <> ''" in normalized_sql
    assert "and lower(btrim(core.shows.slug)) = %s" in normalized_sql
    assert "limit 1" in normalized_sql
    assert "alias" not in normalized_sql
    assert "canonical" not in normalized_sql
    assert "alternative" not in normalized_sql
    assert params == ["rhobh"]
    assert show == {
        "id": SHOW_ID,
        "name": "The Real Housewives of Beverly Hills",
        "slug": "rhobh",
    }


def test_get_show_by_exact_slug_missing_still_uses_one_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_fetch_one(_sql: str, _params: list[object]) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(show_slug_reads.pg, "fetch_one", fake_fetch_one)

    show, query_count = show_slug_reads.get_show_by_exact_slug("missing-show")

    assert show is None
    assert query_count == 1
    assert calls == 1
