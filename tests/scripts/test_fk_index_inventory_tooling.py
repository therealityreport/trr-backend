from __future__ import annotations

from pathlib import Path

import yaml

import scripts.db.run_fk_index_inventory as inventory_mod
import scripts.db.run_fk_index_observer as observer_mod


class _FakeCursor:
    def __init__(self, result_sets: list[list[dict]]):
        self._result_sets = result_sets
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql: str, params=None) -> None:  # noqa: ANN001
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict]:
        return self._result_sets.pop(0)

    def fetchone(self) -> dict | None:
        rows = self._result_sets.pop(0)
        return rows[0] if rows else None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class _FakeConnection:
    def __init__(self, cursors: list[_FakeCursor]):
        self._cursors = cursors
        self.autocommit = False

    def cursor(self) -> _FakeCursor:
        return self._cursors.pop(0)

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def test_generate_inventory_writes_yaml(tmp_path: Path, monkeypatch) -> None:
    owned_path = tmp_path / "owned.yml"
    hot_path = tmp_path / "hot.yml"
    out_path = tmp_path / "wave.yml"
    owned_path.write_text("owned_schemas:\n  - core\n", encoding="utf-8")
    hot_path.write_text("hot_tables:\n  - schema: core\n    table: shows\n", encoding="utf-8")

    monkeypatch.setattr(
        inventory_mod,
        "resolve_direct_db_url",
        lambda: inventory_mod.ResolvedDbUrl(value="postgresql://db.example.com/postgres", source="TRR_DB_URL"),
    )
    monkeypatch.setattr(inventory_mod, "analyze_owned_tables", lambda cur, owned_schemas: ["core.shows"])
    fake_conn = _FakeConnection(
        [
            _FakeCursor([]),
            _FakeCursor(
                [
                    [
                        {
                            "schema_name": "core",
                            "table_name": "episodes",
                            "constraint_name": "episodes_show_id_fkey",
                            "referenced_schema": "core",
                            "referenced_table": "shows",
                            "fk_columns_in_order": ["show_id"],
                            "nullable_columns": [],
                            "estimated_row_count": 2500,
                            "hot_table": False,
                            "covered_by_existing_index": False,
                            "proposed_index_name": "core_episodes_show_id_idx",
                            "proposed_index_columns": ["show_id"],
                            "proposed_partial_predicate": None,
                            "statement_timeout_tier": "30min",
                            "decision": "add",
                        }
                    ]
                ]
            ),
        ]
    )
    monkeypatch.setattr(inventory_mod, "_connect", lambda resolved: fake_conn)

    payload = inventory_mod.generate_inventory(
        owned_schemas_path=owned_path,
        hot_tables_path=hot_path,
        output_path=out_path,
    )

    assert payload["metadata"]["analyzed_tables"] == ["core.shows"]
    assert payload["entries"][0]["proposed_index_name"] == "core_episodes_show_id_idx"
    persisted = yaml.safe_load(out_path.read_text(encoding="utf-8"))
    assert persisted["entries"][0]["decision"] == "add"


def test_generate_inventory_filters_wave_schemas(tmp_path: Path, monkeypatch) -> None:
    owned_path = tmp_path / "owned.yml"
    hot_path = tmp_path / "hot.yml"
    out_path = tmp_path / "wave.yml"
    owned_path.write_text("owned_schemas:\n  - core\n  - admin\n  - social\n  - ml\n", encoding="utf-8")
    hot_path.write_text("hot_tables: []\n", encoding="utf-8")

    monkeypatch.setattr(
        inventory_mod,
        "resolve_direct_db_url",
        lambda: inventory_mod.ResolvedDbUrl(value="postgresql://db.example.com/postgres", source="TRR_DB_URL"),
    )
    monkeypatch.setattr(inventory_mod, "analyze_owned_tables", lambda cur, owned_schemas: owned_schemas)
    fake_conn = _FakeConnection([_FakeCursor([]), _FakeCursor([[]])])
    monkeypatch.setattr(inventory_mod, "_connect", lambda resolved: fake_conn)

    payload = inventory_mod.generate_inventory(
        owned_schemas_path=owned_path,
        hot_tables_path=hot_path,
        output_path=out_path,
        wave_name="wave-1",
    )

    assert payload["metadata"]["wave"] == "wave-1"
    assert payload["metadata"]["owned_schemas"] == ["core", "admin", "social"]


def test_generate_inventory_supports_runtime_connection_mode(tmp_path: Path, monkeypatch) -> None:
    owned_path = tmp_path / "owned.yml"
    hot_path = tmp_path / "hot.yml"
    out_path = tmp_path / "wave.yml"
    owned_path.write_text("owned_schemas:\n  - core\n", encoding="utf-8")
    hot_path.write_text("hot_tables: []\n", encoding="utf-8")

    monkeypatch.setattr(
        inventory_mod,
        "resolve_db_url",
        lambda **_kwargs: inventory_mod.ResolvedDbUrl(
            value="postgresql://pooler.example.com/postgres",
            source="TRR_DB_URL",
        ),
    )
    monkeypatch.setattr(inventory_mod, "analyze_owned_tables", lambda cur, owned_schemas: [])
    fake_conn = _FakeConnection([_FakeCursor([]), _FakeCursor([[]])])
    monkeypatch.setattr(inventory_mod, "_connect", lambda resolved: fake_conn)

    payload = inventory_mod.generate_inventory(
        owned_schemas_path=owned_path,
        hot_tables_path=hot_path,
        output_path=out_path,
        connection_mode="runtime",
    )

    assert payload["metadata"]["connection_mode"] == "runtime"
    assert payload["metadata"]["resolved_db_host"] == "pooler.example.com"


def test_planned_indexes_from_inventory_filters_add_decisions() -> None:
    inventory = {
        "entries": [
            {
                "schema": "core",
                "table": "episodes",
                "proposed_index_name": "core_episodes_show_id_idx",
                "proposed_index_columns": ["show_id"],
                "proposed_partial_predicate": None,
                "decision": "add",
            },
            {
                "schema": "core",
                "table": "episodes",
                "proposed_index_name": "core_episodes_season_id_idx",
                "proposed_index_columns": ["season_id"],
                "proposed_partial_predicate": None,
                "decision": "skip-covered",
            },
        ]
    }

    planned = observer_mod._planned_indexes_from_inventory(inventory)

    assert planned == [
        {
            "schema": "core",
            "table": "episodes",
            "index_name": "core_episodes_show_id_idx",
            "columns": ["show_id"],
            "predicate": None,
        }
    ]
