from __future__ import annotations

import pytest

from trr_backend import modal_dispatch
from trr_backend.services import retained_cast_screentime_subtitle_dispatch


def test_extract_video_asset_subtitles_returns_modal_dispatch_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    expected = {
        "dispatched": True,
        "call_id": "fc-subtitles",
        "reason": None,
        "reason_code": None,
    }

    def _dispatch(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return expected

    monkeypatch.setattr(modal_dispatch, "dispatch_cast_screentime_subtitle_extraction", _dispatch)

    result = retained_cast_screentime_subtitle_dispatch.extract_video_asset_subtitles(
        "asset-123",
        force=True,
    )

    assert result is expected
    assert captured == {"video_asset_id": "asset-123", "force": True}
