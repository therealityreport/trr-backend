from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.fandom import parse_fandom_infobox_html


def test_parse_fandom_infobox_html_lisa_barlow() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "fandom" / "lisa_barlow_infobox.html").read_text(encoding="utf-8")

    result = parse_fandom_infobox_html(
        html,
        url="https://real-housewives.fandom.com/wiki/Lisa_Barlow",
    )

    assert result.full_name == "Lisa Brandine Barlow"
    assert result.gender == "Female"
    assert result.birth_date == "1974-12-14"
    assert result.resides_in == "Salt Lake City, Utah"
