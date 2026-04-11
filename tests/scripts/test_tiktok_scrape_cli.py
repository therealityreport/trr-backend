from __future__ import annotations

from scripts.socials.tiktok import scrape as tiktok_cli


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
