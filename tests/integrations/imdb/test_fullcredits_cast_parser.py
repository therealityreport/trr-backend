from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    filter_self_cast_rows,
    parse_fullcredits_cast_html,
)


def test_parse_fullcredits_cast_html_extracts_cast_rows() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(
        encoding="utf-8"
    )

    rows = parse_fullcredits_cast_html(html, series_id="tt1234567")
    assert len(rows) == 3

    first = rows[0]
    assert first.name_id == "nm0000001"
    assert first.name == "Jane Doe"
    assert first.billing_order == 1
    assert first.raw_role_text == "Self (as Jane)"
    assert first.job_category_id == "amzn1.imdb.concept.name_credit_group.cast123"

    second = rows[1]
    assert second.raw_role_text == "Limo Driver"


def test_filter_self_cast_rows_only_keeps_self_roles() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "fullcredits_cast_sample.html").read_text(
        encoding="utf-8"
    )

    rows = parse_fullcredits_cast_html(html, series_id="tt1234567")
    self_rows = filter_self_cast_rows(rows)

    assert [row.name_id for row in self_rows] == ["nm0000001", "nm0000003"]
    assert self_rows[1].raw_role_text == "Self (archive footage)"
