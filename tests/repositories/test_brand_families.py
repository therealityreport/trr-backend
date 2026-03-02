from __future__ import annotations

from unittest.mock import patch

import trr_backend.repositories.brand_families as mod


def test_extract_wikipedia_show_urls_prefers_programming_sections() -> None:
    html = """
    <html><body>
      <h2>Programming</h2>
      <ul>
        <li><a href="/wiki/The_Traitors_(American_TV_series)">The Traitors</a></li>
        <li><a href="/wiki/Category:Television_networks">Category page</a></li>
      </ul>
      <h2>History</h2>
      <p><a href="/wiki/NBC">NBC</a></p>
    </body></html>
    """
    rows = mod._extract_wikipedia_show_urls_from_html(
        html=html,
        page_url="https://en.wikipedia.org/wiki/Peacock_(streaming_service)",
    )

    urls = {row["show_url"] for row in rows}
    assert "https://en.wikipedia.org/wiki/The_Traitors_(American_TV_series)" in urls
    assert all("Category:" not in row["show_title"] for row in rows)


def test_apply_family_links_skips_manual_and_updates_derived() -> None:
    rule_row = {
        "id": "rule-1",
        "family_id": "fam-1",
        "link_group": "knowledge",
        "link_kind": "wikipedia",
        "label": "Wikipedia",
        "url": "https://en.wikipedia.org/wiki/Test",
        "coverage_type": "family_all_shows",
        "coverage_value": None,
        "source": "manual",
        "priority": 10,
        "auto_apply": True,
        "is_active": True,
        "metadata": {},
    }

    with patch.object(mod, "_fetch_family_row", return_value={"id": "fam-1"}), patch.object(
        mod.pg,
        "fetch_all",
        return_value=[rule_row],
    ), patch.object(mod, "_resolve_rule_show_ids", return_value={"show-1", "show-2"}), patch.object(
        mod,
        "_show_has_non_family_link_kind",
        side_effect=[True, False],
    ), patch.object(mod, "_update_existing_family_rule_link", return_value=1), patch.object(
        mod,
        "_upsert_family_rule_link",
        return_value=0,
    ):
        result = mod.apply_family_links(
            family_id="fam-1",
            dry_run=False,
            actor="admin",
            rule_ids=None,
        )

    assert result["matched_show_count"] == 2
    assert result["skipped_existing_manual"] == 1
    assert result["updated_derived_count"] == 1
    assert result["applied_show_count"] == 1


def test_import_family_wikipedia_show_links_requires_scope_pair() -> None:
    with patch.object(mod, "_fetch_family_row", return_value={"id": "fam-1"}):
        try:
            mod.import_family_wikipedia_show_links(
                family_id="fam-1",
                actor="admin",
                entity_type="network",
                entity_key=None,
                apply_matched=False,
            )
        except ValueError as exc:
            assert "must be provided together" in str(exc)
        else:
            raise AssertionError("expected ValueError")


def test_show_has_non_family_link_kind_escapes_like_placeholder() -> None:
    def _fake_fetch_one(query: str, params: list[object]) -> dict[str, object] | None:
        assert "brand_family_rule:%%" in query
        assert params == ["show-1", "show-1", "wikipedia"]
        return None

    with patch.object(mod.pg, "fetch_one", side_effect=_fake_fetch_one):
        assert mod._show_has_non_family_link_kind("show-1", link_kind="wikipedia") is False
