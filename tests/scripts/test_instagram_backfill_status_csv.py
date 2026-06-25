from __future__ import annotations

import csv
from pathlib import Path


def test_backfill_status_csv_uses_rollups_and_writes_rows(monkeypatch, tmp_path: Path) -> None:
    from scripts.socials.instagram import backfill_status_csv

    calls: list[dict[str, object]] = []

    def _fetch_all(query: str, params: list[object]) -> list[dict[str, object]]:
        calls.append({"query": query, "params": params})
        return [
            {
                "shortcode": "DVMdEy8AbLL",
                "post_id": "post-1",
                "reported_comment_count": 6274,
                "saved_comment_count": 1501,
                "detail_refresh_incomplete": False,
                "comments_incomplete": True,
                "media_mirror_incomplete": False,
                "missing_materialized": False,
                "hard_media_error": True,
                "media_mirror_status": "unrecoverable",
                "media_mirror_error": "media[0]:download_failed:asset_too_large",
                "hosted_thumbnail_present": True,
                "hosted_media_url_count": 0,
            }
        ]

    monkeypatch.setattr(backfill_status_csv.pg, "fetch_all", _fetch_all)

    rows = backfill_status_csv.fetch_rows(
        account_handle="BravoTV",
        run_id="11111111-1111-1111-1111-111111111111",
    )
    output = backfill_status_csv.write_csv(rows, tmp_path / "status.csv")

    query = str(calls[0]["query"])
    assert "social.instagram_post_comment_rollups" in query
    assert "active_comment_count" in query
    assert "asset_too_large" in query
    assert calls[0]["params"] == ["bravotv", "11111111-1111-1111-1111-111111111111"]

    with output.open(encoding="utf-8", newline="") as fh:
        written = list(csv.DictReader(fh))

    assert written[0]["shortcode"] == "DVMdEy8AbLL"
    assert written[0]["reported_comment_count"] == "6274"
    assert written[0]["saved_comment_count"] == "1501"
    assert written[0]["hard_media_error"] == "True"
