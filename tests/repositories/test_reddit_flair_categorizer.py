from __future__ import annotations

from contextlib import contextmanager

from trr_backend.repositories import reddit_flair_categorizer


class _FakeCursor:
    def __init__(self, results: dict[str, list[tuple]]) -> None:
        self._results = results
        self._rows: list[tuple] = []
        self.description: list[tuple] | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | list | None = None) -> None:
        if "core.show_cast" in sql:
            self._rows = self._results.get("cast", [])
        elif "core.seasons" in sql:
            self._rows = self._results.get("seasons", [])
        elif "trr_show_id" in sql:
            self._rows = self._results.get("communities", [])
            self.description = [("id",), ("subreddit",), ("post_flairs",)]
        elif "admin.reddit_communities" in sql:
            self._rows = self._results.get("flairs", [])
        else:  # pragma: no cover - unexpected query shape
            raise AssertionError(f"unexpected query: {sql}")

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class _FakeConnection:
    def __init__(self, results: dict[str, list[tuple]]) -> None:
        self._results = results

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._results)


def _patch_db(monkeypatch, results: dict[str, list[tuple]]) -> None:
    @contextmanager
    def fake_db_connection(**_: object):
        yield _FakeConnection(results)

    monkeypatch.setattr(reddit_flair_categorizer.pg, "db_connection", fake_db_connection)


def test_module_uses_real_pg_helpers() -> None:
    # Regression guard: the module previously called pg.connection(), which does
    # not exist on trr_backend.db.pg and made every categorizer route 500.
    assert hasattr(reddit_flair_categorizer.pg, "db_connection")
    import inspect

    source = inspect.getsource(reddit_flair_categorizer)
    assert "pg.connection(" not in source


def test_auto_categorize_flairs_matches_cast_and_seasons(monkeypatch) -> None:
    _patch_db(
        monkeypatch,
        {
            "cast": [("Lisa Barlow",), ("Heather Gay",)],
            "seasons": [(5,), (6,)],
            "flairs": [(["Lisa Barlow", "Season 6", "Unrelated Flair"],)],
        },
    )

    result = reddit_flair_categorizer.auto_categorize_flairs(
        community_id="community-1", show_id="show-1"
    )

    assert result["total"] == 3
    assert result["matched"] == 2
    assert result["categories"]["lisa barlow"] == "cast"
    assert result["categories"]["season 6"] == "season"


def test_auto_categorize_flairs_batch_aggregates_communities(monkeypatch) -> None:
    _patch_db(
        monkeypatch,
        {
            "cast": [("Lisa Barlow",)],
            "seasons": [(6,)],
            "communities": [
                ("community-1", "rhoslc", ["Lisa Barlow", "S6"]),
                ("community-2", "BravoRealHousewives", []),
            ],
        },
    )

    result = reddit_flair_categorizer.auto_categorize_flairs_batch(show_id="show-1")

    assert result["total_communities"] == 2
    assert result["total_matched"] == 2
    assert result["total_flairs"] == 2
    by_id = {c["community_id"]: c for c in result["communities"]}
    assert by_id["community-1"]["categories"] == {
        "lisa barlow": "cast",
        "s6": "season",
    }
    assert by_id["community-2"]["matched"] == 0
