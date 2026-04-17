from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import scripts.db.run_fk_index_checks as checks_mod


class _FakeCursor:
    def __init__(self, result_sets: list[list[dict]]):
        self._result_sets = result_sets
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql: str, params=None) -> None:  # noqa: ANN001
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict]:
        if not self._result_sets:
            return []
        return self._result_sets.pop(0)

    def fetchone(self) -> dict | None:
        if not self._result_sets:
            return None
        rows = self._result_sets.pop(0)
        return rows[0] if rows else None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.autocommit = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def _write_inventory(path: Path, entries: list[dict]) -> None:
    path.write_text(yaml.safe_dump({"entries": entries}), encoding="utf-8")


def _install_fake_connection(monkeypatch, cursor: _FakeCursor) -> None:
    conn = _FakeConnection(cursor)
    monkeypatch.setattr(checks_mod, "_connect", lambda: conn)


def _patch_inventory_path(monkeypatch, inventory_path: Path) -> None:
    monkeypatch.setattr(checks_mod, "_inventory_path_for_wave", lambda wave_name: inventory_path)


def test_argparse_accepts_presence_with_wave_and_output() -> None:
    parser = checks_mod.build_parser()
    args = parser.parse_args(["presence", "--wave", "wave-1", "--output", "/tmp/out.csv"])
    assert args.command == "presence"
    assert args.wave == "wave-1"
    assert args.output == "/tmp/out.csv"


def test_argparse_rejects_presence_without_wave() -> None:
    parser = checks_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["presence", "--output", "/tmp/out.csv"])


def test_argparse_rejects_presence_without_output() -> None:
    parser = checks_mod.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["presence", "--wave", "wave-1"])


def test_argparse_accepts_compare_with_all_three_args() -> None:
    parser = checks_mod.build_parser()
    args = parser.parse_args(
        [
            "compare",
            "--baseline-csv",
            "/tmp/b.csv",
            "--post-csv",
            "/tmp/p.csv",
            "--output",
            "/tmp/o.csv",
        ]
    )
    assert args.command == "compare"
    assert args.baseline_csv == "/tmp/b.csv"
    assert args.post_csv == "/tmp/p.csv"
    assert args.output == "/tmp/o.csv"


def test_run_check_loads_inventory_and_passes_filtered_add_entries_as_json(tmp_path: Path, monkeypatch) -> None:
    inventory_path = tmp_path / "wave-1-inventory.yml"
    _write_inventory(
        inventory_path,
        [
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
                "decision": "add",
            },
            {
                "schema": "core",
                "table": "episodes",
                "proposed_index_name": "core_episodes_network_id_idx",
                "proposed_index_columns": ["network_id"],
                "proposed_partial_predicate": None,
                "decision": "skip-covered",
            },
        ],
    )
    _patch_inventory_path(monkeypatch, inventory_path)
    cursor = _FakeCursor([[]])
    _install_fake_connection(monkeypatch, cursor)

    output_path = tmp_path / "presence.csv"
    checks_mod.run_check(kind="presence", wave_name="wave-1", output_path=output_path)

    assert len(cursor.executed) == 1
    _, params = cursor.executed[0]
    assert set(params.keys()) == {"expected_indexes_json"}
    payload = json.loads(params["expected_indexes_json"])
    assert len(payload) == 2
    assert [entry["index_name"] for entry in payload] == [
        "core_episodes_show_id_idx",
        "core_episodes_season_id_idx",
    ]
    # write_csv writes empty file when there are no rows
    assert output_path.exists()


@pytest.mark.parametrize(
    "kind,expected_sql_filename,expected_param",
    [
        ("presence", "fk_index_presence_check.sql", "expected_indexes_json"),
        ("invalid", "fk_index_invalid_check.sql", "planned_indexes_json"),
        ("duplicate", "fk_index_duplicate_check.sql", "expected_indexes_json"),
    ],
)
def test_run_check_uses_correct_sql_file_and_param_name_per_kind(
    tmp_path: Path,
    monkeypatch,
    kind: str,
    expected_sql_filename: str,
    expected_param: str,
) -> None:
    inventory_path = tmp_path / "wave-1-inventory.yml"
    _write_inventory(
        inventory_path,
        [
            {
                "schema": "core",
                "table": "episodes",
                "proposed_index_name": "core_episodes_show_id_idx",
                "proposed_index_columns": ["show_id"],
                "proposed_partial_predicate": None,
                "decision": "add",
            }
        ],
    )
    _patch_inventory_path(monkeypatch, inventory_path)
    cursor = _FakeCursor([[]])
    _install_fake_connection(monkeypatch, cursor)

    captured: dict[str, Path] = {}
    real_read_sql = checks_mod._read_sql

    def _spy_read_sql(path: Path) -> str:
        captured["path"] = path
        return real_read_sql(path)

    monkeypatch.setattr(checks_mod, "_read_sql", _spy_read_sql)

    output_path = tmp_path / f"{kind}.csv"
    checks_mod.run_check(kind=kind, wave_name="wave-1", output_path=output_path)

    assert captured["path"].name == expected_sql_filename
    assert captured["path"] == checks_mod.SCRIPTS_DB / expected_sql_filename
    assert len(cursor.executed) == 1
    _, params = cursor.executed[0]
    assert set(params.keys()) == {expected_param}


def test_compare_subcommand_delegates_to_observer_compare_baseline(tmp_path: Path, monkeypatch) -> None:
    called: dict[str, Path] = {}

    def _fake_compare_baseline(*, baseline_csv: Path, post_csv: Path, output_path: Path) -> Path:
        called["baseline_csv"] = baseline_csv
        called["post_csv"] = post_csv
        called["output_path"] = output_path
        return output_path

    monkeypatch.setattr(checks_mod, "compare_baseline", _fake_compare_baseline)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_fk_index_checks.py",
            "compare",
            "--baseline-csv",
            str(tmp_path / "b.csv"),
            "--post-csv",
            str(tmp_path / "p.csv"),
            "--output",
            str(tmp_path / "out.csv"),
        ],
    )
    rc = checks_mod.main()

    assert rc == 0
    assert called["baseline_csv"] == tmp_path / "b.csv"
    assert called["post_csv"] == tmp_path / "p.csv"
    assert called["output_path"] == tmp_path / "out.csv"
