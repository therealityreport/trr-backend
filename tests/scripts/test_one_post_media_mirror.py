from __future__ import annotations

import json
from typing import Any

from scripts.socials.instagram import one_post_media_mirror as cli


def test_resolve_media_job_by_shortcode_filters_claimable_media_job(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def _fake_fetch_one(query: str, params: list[Any]) -> dict[str, Any]:
        captured["query"] = query
        captured["params"] = params
        return {
            "job_id": "job-1",
            "post_id": "post-1",
            "shortcode": "DGk_hLXhy56",
            "job_status": "queued",
        }

    monkeypatch.setattr(cli.pg, "fetch_one", _fake_fetch_one)
    args = cli._parse_args(["--source-id", "DGk_hLXhy56", "--account", "@BravoTV", "--dry-run"])

    row = cli.resolve_media_job(args)

    assert row is not None
    assert row["job_id"] == "job-1"
    assert "j.status = any(%s::text[])" in captured["query"]
    assert captured["params"] == [
        "DGk_hLXhy56",
        "media_mirror",
        ["queued", "pending", "retrying"],
        "bravotv",
    ]


def test_main_dry_run_prints_resolved_job(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli.pg, "close_pool", lambda: None)
    monkeypatch.setattr(
        cli,
        "resolve_media_job",
        lambda _args: {
            "job_id": "job-1",
            "shortcode": "DGk_hLXhy56",
            "job_status": "queued",
        },
    )
    monkeypatch.setattr(
        cli,
        "claim_and_process_social_job",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("dry run must not claim")),
    )

    assert cli.main(["--source-id", "DGk_hLXhy56", "--dry-run", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["job_id"] == "job-1"
