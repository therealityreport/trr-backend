from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROUTER_PATH = Path(__file__).parents[3] / "api/routers/socials/__init__.py"
CANONICAL_ANALYTICS_MODULE = "trr_backend.socials.analytics"
LEGACY_ANALYTICS_MODULE = "trr_backend.repositories.social_season_analytics"


@pytest.mark.parametrize(
    ("function_name", "expected_names"),
    [
        ("get_season_analytics_week_live_health", {"get_week_live_health_snapshot"}),
        ("get_season_analytics", {"get_analytics"}),
        (
            "get_season_analytics_week_summary",
            {"get_week_detail_summary", "get_week_detail_summary_fast"},
        ),
        ("get_season_analytics_week_detail", {"get_week_detail"}),
    ],
)
def test_season_analytics_endpoints_use_canonical_import_boundary(
    function_name: str,
    expected_names: set[str],
) -> None:
    module = ast.parse(ROUTER_PATH.read_text(encoding="utf-8"))
    function = next(
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name
    )
    imports = [node for node in ast.walk(function) if isinstance(node, ast.ImportFrom)]

    assert not any(node.module == LEGACY_ANALYTICS_MODULE for node in imports)
    canonical_names = {
        alias.name for node in imports if node.module == CANONICAL_ANALYTICS_MODULE for alias in node.names
    }
    assert canonical_names == expected_names
