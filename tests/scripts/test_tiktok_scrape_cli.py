from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.socials.tiktok import scrape as tiktok_cli
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
