from __future__ import annotations

import importlib.util
import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "socials" / "repair_instagram_single_media_urls.py"
SPEC = importlib.util.spec_from_file_location("repair_instagram_single_media_urls", SCRIPT_PATH)
assert SPEC and SPEC.loader
repair_script = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = repair_script
SPEC.loader.exec_module(repair_script)


def test_select_primary_media_url_prefers_image_for_non_video_posts() -> None:
    primary = repair_script._select_primary_media_url(  # noqa: SLF001
        media_type="image",
        post_format="post",
        media_urls=[
            "https://example.com/cover.jpg",
            "https://example.com/video.mp4",
        ],
    )

    assert primary == "https://example.com/cover.jpg"


def test_select_primary_media_url_prefers_video_for_reels() -> None:
    primary = repair_script._select_primary_media_url(  # noqa: SLF001
        media_type="video",
        post_format="reel",
        media_urls=[
            "https://example.com/cover.jpg",
            "https://example.com/video.mp4",
        ],
    )

    assert primary == "https://example.com/video.mp4"


def test_repair_candidate_row_skips_rows_already_normalized() -> None:
    repair = repair_script._repair_candidate_row(  # noqa: SLF001
        {
            "id": "1",
            "shortcode": "DObyKQfkz4v",
            "media_type": "image",
            "post_format": "post",
            "media_urls": ["https://example.com/only-one.jpg"],
        }
    )

    assert repair is None


def test_main_dry_run_reports_repairs_without_updating(monkeypatch) -> None:
    candidate_rows = [
        {
            "id": "row-1",
            "shortcode": "repair-me",
            "media_type": "video",
            "post_format": "reel",
            "media_urls": [
                "https://example.com/cover.jpg",
                "https://example.com/video.mp4",
            ],
        },
        {
            "id": "row-2",
            "shortcode": "DObyKQfkz4v",
            "media_type": "image",
            "post_format": "post",
            "media_urls": ["https://example.com/only-one.jpg"],
        },
    ]

    monkeypatch.setattr(repair_script, "load_env", lambda: None)
    monkeypatch.setattr(repair_script, "_fetch_candidate_rows", lambda **_kwargs: candidate_rows)
    monkeypatch.setattr(
        repair_script,
        "_apply_repairs",
        lambda repairs: (_ for _ in ()).throw(AssertionError("apply should not run during dry-run")),
    )

    stdout = io.StringIO()
    with redirect_stdout(stdout):
        exit_code = repair_script.main(["--dry-run", "--limit", "25"])

    payload = json.loads(stdout.getvalue().strip())
    assert exit_code == 0
    assert payload["dry_run"] is True
    assert payload["totals"] == {
        "matched_rows": 2,
        "rows_needing_repair": 1,
        "rows_updated": 0,
    }
    assert payload["preview"] == [
        {
            "shortcode": "repair-me",
            "old_count": 2,
            "new_media_urls": ["https://example.com/video.mp4"],
        }
    ]


def test_main_apply_updates_only_rows_needing_repair(monkeypatch) -> None:
    candidate_rows = [
        {
            "id": "row-1",
            "shortcode": "repair-me",
            "media_type": "image",
            "post_format": "post",
            "media_urls": [
                "https://example.com/primary.jpg",
                "https://example.com/secondary.jpg",
            ],
        },
        {
            "id": "row-2",
            "shortcode": "already-good",
            "media_type": "image",
            "post_format": "post",
            "media_urls": ["https://example.com/only-one.jpg"],
        },
    ]
    applied_repairs: list[dict[str, object]] = []

    monkeypatch.setattr(repair_script, "load_env", lambda: None)
    monkeypatch.setattr(repair_script, "_fetch_candidate_rows", lambda **_kwargs: candidate_rows)

    def _fake_apply(repairs: list[dict[str, object]]) -> list[dict[str, object]]:
        applied_repairs.extend(repairs)
        return [{"id": "row-1", "shortcode": "repair-me"}]

    monkeypatch.setattr(repair_script, "_apply_repairs", _fake_apply)

    stdout = io.StringIO()
    with redirect_stdout(stdout):
        exit_code = repair_script.main(["--apply"])

    payload = json.loads(stdout.getvalue().strip())
    assert exit_code == 0
    assert payload["dry_run"] is False
    assert payload["totals"] == {
        "matched_rows": 2,
        "rows_needing_repair": 1,
        "rows_updated": 1,
    }
    assert applied_repairs == [
        {
            "id": "row-1",
            "shortcode": "repair-me",
            "old_media_urls": [
                "https://example.com/primary.jpg",
                "https://example.com/secondary.jpg",
            ],
            "new_media_urls": ["https://example.com/primary.jpg"],
        }
    ]
