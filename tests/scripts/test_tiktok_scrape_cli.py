from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.socials.tiktok import scrape as tiktok_cli
from scripts.socials.tiktok import smoke_posts_scrapling as tiktok_smoke_cli
from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.tiktok import ops as tiktok_ops
from trr_backend.socials.tiktok.ops import proxy_label


def test_smoke_posts_scrapling_direct_file_help_works() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "scripts/socials/tiktok/smoke_posts_scrapling.py", "--help"],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--account" in result.stdout
    assert "--max-pages" in result.stdout


def test_smoke_posts_scrapling_loads_env_before_running(monkeypatch, capsys) -> None:
    calls: list[str] = []

    def fake_load_env() -> None:
        calls.append("load_env")

    def fake_run_posts_scrapling_smoke(*, account: str, max_pages: int):
        calls.append("run")
        assert account == "bravotv"
        assert max_pages == 1
        return {"run_id": "run-id", "job_id": "job-id", "status": "completed", "items_found": 3}

    monkeypatch.setattr(tiktok_smoke_cli, "load_env", fake_load_env)
    monkeypatch.setattr(tiktok_smoke_cli, "run_posts_scrapling_smoke", fake_run_posts_scrapling_smoke)
    monkeypatch.setattr(sys, "argv", ["smoke_posts_scrapling.py", "--account", "bravotv", "--max-pages", "1"])

    assert tiktok_smoke_cli.main() == 0
    assert calls == ["load_env", "run"]
    assert "Items found: 3" in capsys.readouterr().out


def test_run_posts_scrapling_smoke_uses_current_source_scope(monkeypatch) -> None:
    captured: dict[str, dict] = {}

    def fake_create_run(_context, **kwargs):
        captured["run"] = kwargs
        return "run-id"

    def fake_create_job(_context, **kwargs):
        captured["job"] = kwargs
        return "job-id"

    def fake_fetch_one(_query, _params):
        return {"id": "job-id", "config": {}}

    def fake_runner(job):
        captured["runner_job"] = job
        return {"status": "completed", "items_found": 1}

    monkeypatch.setattr(repo, "_create_run", fake_create_run)
    monkeypatch.setattr(repo, "_create_job", fake_create_job)
    monkeypatch.setattr(pg, "fetch_one", fake_fetch_one)

    from trr_backend.socials.tiktok.posts_scrapling import job_runner

    monkeypatch.setattr(job_runner, "run_tiktok_posts_scrapling_job", fake_runner)

    assert tiktok_ops.run_posts_scrapling_smoke(account="bravotv", max_pages=1)["status"] == "completed"
    assert captured["run"]["source_scope"] == "network"
    assert captured["job"]["source_scope"] == "network"


def test_proxy_label_redacts_schemeless_proxy_credentials() -> None:
    assert proxy_label("user:pass@proxy.example:9000") == "proxy.example"
    assert proxy_label("http://user:pass@proxy.example:9000") == "proxy.example"
    assert proxy_label("user:pass@") == "redacted-proxy"


def test_emit_diagnostics_summary_prints_risk_and_operator_fields(capsys) -> None:
    tiktok_cli._emit_diagnostics_summary(  # noqa: SLF001
        target_label="bravotv",
        scrape_mode="ytdlp",
        diagnostics={
            "http_client": "requests",
            "risk_state": "critical",
            "operator_summary": "TikTok posts path degraded; browser intercept fallback recommended.",
            "operator_action": "Inspect fallback chain and retry with browser intercept.",
            "triage_bucket": "manual_review",
        },
    )

    output = capsys.readouterr().out

    assert "Risk state: critical" in output
    assert "Operator summary: TikTok posts path degraded;" in output
    assert "Operator action: Inspect fallback chain and retry with browser intercept." in output
    assert "Triage bucket: manual_review" in output


def test_emit_diagnostics_summary_redacts_proxy_credentials(capsys) -> None:
    tiktok_cli._emit_diagnostics_summary(  # noqa: SLF001
        target_label="bravotv",
        scrape_mode="api",
        diagnostics={"proxy_label": "user:pass@proxy.example:9000"},
    )

    output = capsys.readouterr().out
    assert "proxy.example" in output
    assert "user:pass" not in output
