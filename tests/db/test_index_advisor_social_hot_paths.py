from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/db/index_advisor_social_hot_paths.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("index_advisor_social_hot_paths", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_resolve_db_url_prefers_session_url(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setenv("TRR_DB_SESSION_URL", "postgres://session.example/db")
    monkeypatch.setenv("TRR_DB_URL", "postgres://runtime.example/db")
    monkeypatch.setenv("TRR_DB_FALLBACK_URL", "postgres://fallback.example/db")

    resolved = module.resolve_db_url()

    assert resolved.source == "TRR_DB_SESSION_URL"
    assert resolved.value == "postgres://session.example/db"


def test_dry_run_does_not_load_env_or_connect(monkeypatch, capsys) -> None:
    module = _load_module()
    monkeypatch.setattr(module, "load_env", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("loaded env")))
    monkeypatch.setattr(
        module.psycopg2,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("connected")),
    )

    assert module.main(["--dry-run", "--labels", "profile_dashboard/shared_account_source"]) == 0

    output = capsys.readouterr().out
    assert "profile_dashboard/shared_account_source" in output
    assert "/api/v1/admin/socials/profiles/:platform/:handle/dashboard" in output


def test_query_set_covers_required_social_hot_paths() -> None:
    module = _load_module()

    labels = {spec.label for spec in module.QUERY_SPECS}

    assert {
        "profile_dashboard/shared_account_source",
        "profile_dashboard/recent_catalog_jobs",
        "shared_ingest/recent_runs",
        "shared_review_queue/open_items",
        "social_landing/socialblade_rows",
        "season_analytics/season_targets",
        "week_live_health/instagram_week_bucket",
    }.issubset(labels)


def test_report_redacts_db_url_and_marks_review_required(tmp_path: Path) -> None:
    module = _load_module()
    report = {
        "metadata": {
            "generated_at": "2026-04-28T12:00:00+00:00",
            "output_date": "2026-04-28",
            "database": {"source": "TRR_DB_URL", "value": "redacted"},
            "extension_schema": "extensions",
            "extension_version": "0.2.0",
            "read_only": True,
        },
        "queries": [
            {
                "label": "profile_dashboard/shared_account_source",
                "route": "/api/v1/admin/socials/profiles/:platform/:handle/dashboard",
                "parameters": {"platform": "instagram"},
                "status": "ok",
                "recommendations": [{"index_statement": "create index example_idx on social.example(id)"}],
                "errors": [],
                "review_required": True,
            }
        ],
    }

    json_path, md_path = module.write_reports(report, tmp_path, "2026-04-28")

    combined = json_path.read_text(encoding="utf-8") + md_path.read_text(encoding="utf-8")
    assert "postgres://" not in combined
    assert "TRR_DB_URL" in combined
    assert '"review_required": true' in combined
    assert "Do not execute returned DDL" in combined


def test_report_surfaces_nested_recommendation_errors(tmp_path: Path) -> None:
    module = _load_module()
    report = {
        "metadata": {
            "generated_at": "2026-07-02T12:00:00+00:00",
            "output_date": "2026-07-02",
            "database": {"source": "TRR_DB_URL", "value": "redacted"},
            "extension_schema": "extensions",
            "extension_version": "0.2.0",
            "read_only": True,
        },
        "queries": [
            {
                "label": "profile_dashboard/shared_account_source",
                "route": "/api/v1/admin/socials/profiles/:platform/:handle/dashboard",
                "parameters": {"platform": "instagram"},
                "status": "advisor_warning",
                "recommendations": [{"errors": ["hypopg: not more oid available"], "index_statements": []}],
                "errors": ["hypopg: not more oid available"],
                "review_required": True,
            }
        ],
    }

    _, md_path = module.write_reports(report, tmp_path, "2026-07-02")

    markdown = md_path.read_text(encoding="utf-8")
    assert "| profile_dashboard/shared_account_source | advisor_warning | 1 | 1 |" in markdown
    assert "`hypopg: not more oid available`" in markdown


def test_script_does_not_execute_returned_index_statements() -> None:
    script = SCRIPT.read_text(encoding="utf-8").lower()

    assert "create index" not in script
    assert "cur.execute(statement" not in script
    assert "cur.execute(recommendation" not in script
    assert "select * from extensions.index_advisor" in script
