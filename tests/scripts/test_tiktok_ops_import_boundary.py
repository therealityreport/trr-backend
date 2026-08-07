from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

SOURCE_PATH = Path(__file__).resolve().parents[2] / "trr_backend" / "socials" / "tiktok" / "ops.py"


def _run_smoke(monkeypatch):
    import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
    import trr_backend.socials.tiktok.ops as ops

    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        dispatch_runtime.legacy,
        "_create_run",
        lambda _context, **kwargs: calls.append(("run", kwargs)) or "run-id",
    )
    monkeypatch.setattr(
        dispatch_runtime.legacy,
        "_create_job",
        lambda _context, **kwargs: calls.append(("job", kwargs)) or "job-id",
    )
    monkeypatch.setattr(dispatch_runtime.legacy.pg, "fetch_one", lambda *_args: None)

    result = ops.run_posts_scrapling_smoke(account="@BravoTV", max_pages=2)
    return calls, result


def test_tiktok_ops_source_uses_lazy_dispatch_runtime_legacy_boundary() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_posts_scrapling_smoke"
    )
    imports = [node for node in ast.walk(function) if isinstance(node, ast.ImportFrom)]

    assert any(
        node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        and [(alias.name, alias.asname) for alias in node.names] == [("legacy", "repo")]
        for node in imports
    )
    assert "trr_backend.repositories" not in source
    assert "social_season_analytics_impl" not in source


def test_tiktok_ops_dispatch_runtime_legacy_is_exact_repository_alias() -> None:
    import trr_backend.repositories.social_season_analytics as repository_alias
    import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime

    assert dispatch_runtime.legacy is repository_alias
    assert dispatch_runtime.legacy.__dict__ is repository_alias.__dict__


def test_tiktok_ops_resolves_live_monkeypatches_at_execution_time(monkeypatch) -> None:
    calls, result = _run_smoke(monkeypatch)

    assert [name for name, _payload in calls] == ["run", "job"]
    assert calls[0][1]["config"]["stage"] == calls[1][1]["stage"]
    assert result == {
        "run_id": "run-id",
        "job_id": "job-id",
        "status": "missing_job",
        "items_found": None,
        "error_message": "job job-id not found in social.scrape_jobs",
    }
