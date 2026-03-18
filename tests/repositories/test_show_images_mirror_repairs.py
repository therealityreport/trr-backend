from __future__ import annotations

from types import SimpleNamespace

from trr_backend.repositories import show_images as show_images_repo


class _FakeQuery:
    def __init__(self, table_name: str, responses: dict[str, list[dict[str, object]]], log: dict[str, object]) -> None:
        self.table_name = table_name
        self.responses = responses
        self.log = log

    def select(self, columns: str):
        self.log.setdefault("selects", {})[self.table_name] = columns
        return self

    def eq(self, column: str, value: object):
        self.log.setdefault("eq", []).append((self.table_name, column, value))
        return self

    def in_(self, column: str, values: list[str]):
        self.log.setdefault("in", []).append((self.table_name, column, list(values)))
        return self

    def is_(self, column: str, value: object):
        self.log.setdefault("is", []).append((self.table_name, column, value))
        return self

    @property
    def not_(self):
        return self

    def ilike(self, column: str, value: str):
        self.log.setdefault("ilike", []).append((self.table_name, column, value))
        return self

    def limit(self, value: int):
        self.log.setdefault("limits", []).append((self.table_name, value))
        return self

    def execute(self):
        return SimpleNamespace(data=self.responses.get(self.table_name, []), error=None)


class _FakeSchema:
    def __init__(self, responses: dict[str, list[dict[str, object]]], log: dict[str, object]) -> None:
        self.responses = responses
        self.log = log

    def table(self, table_name: str) -> _FakeQuery:
        return _FakeQuery(table_name, self.responses, self.log)


class _FakeDb:
    def __init__(self, responses: dict[str, list[dict[str, object]]], log: dict[str, object]) -> None:
        self.responses = responses
        self.log = log

    def schema(self, _schema_name: str) -> _FakeSchema:
        return _FakeSchema(self.responses, self.log)


def test_fetch_show_images_missing_hosted_applies_imdb_filter_and_selects_joined_show_metadata() -> None:
    log: dict[str, object] = {}
    db = _FakeDb(
        {
            "shows": [{"id": "show-1"}],
            "show_images": [
                {
                    "id": "img-1",
                    "show_id": "show-1",
                    "shows": {"imdb_id": "tt1234567"},
                }
            ],
        },
        log,
    )

    rows = show_images_repo.fetch_show_images_missing_hosted(db, source="tmdb", imdb_id="tt1234567")

    assert rows == [{"id": "img-1", "show_id": "show-1", "shows": {"imdb_id": "tt1234567"}}]
    assert log["selects"]["show_images"].count("shows(imdb_id)") == 1
    assert ("shows", "imdb_id", "tt1234567") in log["eq"]
    assert ("show_images", "show_id", ["show-1"]) in log["in"]
