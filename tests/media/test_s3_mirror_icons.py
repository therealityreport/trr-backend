from __future__ import annotations

from trr_backend.media import s3_mirror


def test_build_icon_s3_key_sanitizes_show_key_and_filename() -> None:
    key = s3_mirror.build_icon_s3_key("RHOSLC S6", "Brack Star.PNG")
    assert key == "icons/rhoslc-s6/brack-star.png"


def test_build_icon_s3_key_handles_empty_filename() -> None:
    key = s3_mirror.build_icon_s3_key("rhobh", "")
    assert key == "icons/rhobh/icon.bin"


def test_get_show_icon_s3_prefix() -> None:
    prefix = s3_mirror.get_show_icon_s3_prefix("The Real Housewives of Potomac")
    assert prefix == "icons/the-real-housewives-of-potomac/"
