from __future__ import annotations

from pathlib import Path

from trr_backend.ingestion.show_cast_matrix_scraper import (
    build_person_fandom_url,
    build_person_wikipedia_url,
    extract_relationship_data_from_fandom_html,
    extract_relationship_data_from_wikipedia_html,
    is_missing_fandom_page,
    is_missing_wikipedia_page,
    merge_cast_matrices,
    parse_fandom_cast_matrix_html,
    parse_wikipedia_cast_matrix_html,
)


def _read_fixture(group: str, name: str) -> str:
    base = Path(__file__).resolve().parents[1] / "fixtures" / group
    return (base / name).read_text(encoding="utf-8")


def test_parse_wikipedia_cast_matrix_html_handles_colspans_and_tba() -> None:
    html = _read_fixture("wikipedia", "rhoslc_cast_table_sample.html")
    matrix = parse_wikipedia_cast_matrix_html(html)

    assert matrix["Lisa Barlow"][1] == "Housewife"
    assert matrix["Lisa Barlow"][6] == "Housewife"
    assert matrix["Mary Cosby"][1] == "Housewife"
    assert matrix["Mary Cosby"][3] == "Friend"
    assert matrix["Mary Cosby"][4] == "Housewife"
    assert 5 not in matrix["Mary Cosby"]
    assert matrix["Angie Harrington"][1] == "Guest"
    assert matrix["Angie Harrington"][2] == "Friend"


def test_parse_fandom_cast_matrix_html_maps_active_cells_to_roles() -> None:
    html = _read_fixture("fandom", "rhoslc_cast_table_sample.html")
    matrix = parse_fandom_cast_matrix_html(html)

    assert matrix["Lisa Barlow"][1] == "Housewife"
    assert matrix["Angie Katsanevas"][2] == "Guest"
    assert matrix["Angie Katsanevas"][3] == "Friend"
    assert matrix["Angie Katsanevas"][4] == "Housewife"
    assert matrix["Britani Bateman"][3] == "Friend"


def test_merge_cast_matrices_prefers_wikipedia_and_fills_missing() -> None:
    wiki = {
        "Lisa Barlow": {1: "Housewife", 2: "Housewife"},
        "Mary Cosby": {1: "Housewife"},
    }
    fandom = {
        "Lisa Barlow": {3: "Housewife"},
        "Mary Cosby": {1: "Friend", 3: "Friend"},
        "Angie Harrington": {2: "Friend"},
    }

    merged = merge_cast_matrices(wiki, fandom)

    assert merged["Lisa Barlow"][1] == "Housewife"
    assert merged["Lisa Barlow"][3] == "Housewife"
    assert merged["Mary Cosby"][1] == "Housewife"
    assert merged["Mary Cosby"][3] == "Friend"
    assert merged["Angie Harrington"][2] == "Friend"


def test_build_person_knowledge_urls_use_person_pages() -> None:
    assert build_person_fandom_url("Lisa Barlow") == "https://real-housewives.fandom.com/wiki/Lisa_Barlow"
    assert build_person_wikipedia_url("Lisa Barlow") == "https://en.wikipedia.org/wiki/Lisa_Barlow"


def test_extract_relationship_data_from_fandom_html_returns_season_partner_roles_and_kids() -> None:
    html = """
    <div class='mw-parser-output'>
      <aside class='portable-infobox'>
        <div class='pi-item pi-data'><h3 class='pi-data-label'>Family</h3>
          <div class='pi-data-value'>Mia Doe<br /><small>(Daughter)</small></div>
        </div>
      </aside>
      <table class='wikitable'>
        <tr><th>Season</th><th>Partner</th><th>Status</th></tr>
        <tr><td>2</td><td><a href='/wiki/John_Barlow'>John Barlow</a></td><td>Husband</td></tr>
        <tr><td>3</td><td><a href='/wiki/John_Barlow'>John Barlow</a></td><td>Ex-Husband</td></tr>
      </table>
    </div>
    """
    data = extract_relationship_data_from_fandom_html(html)

    assert data["kid_names"] == ["Mia Doe"]
    assert data["global_partner_roles"] == []
    assert {(item["season"], item["name"], item["role"]) for item in data["season_partner_roles"]} == {
        (2, "John Barlow", "Husband"),
        (3, "John Barlow", "Ex-Husband"),
    }


def test_extract_relationship_data_from_wikipedia_html_parses_spouse_and_children() -> None:
    html = """
    <table class="infobox biography vcard">
      <tr>
        <th>Spouse</th>
        <td><a href="/wiki/John_Barlow">John Barlow</a> (m. 2003)</td>
      </tr>
      <tr>
        <th>Children</th>
        <td>
          <ul>
            <li><a href="/wiki/Jack_Barlow">Jack Barlow</a></li>
            <li><a href="/wiki/Henry_Barlow">Henry Barlow</a></li>
          </ul>
        </td>
      </tr>
      <tr>
        <th>Relatives</th>
        <td><a href="/wiki/Denise_Cannon">Denise Cannon</a> (sister)</td>
      </tr>
    </table>
    """
    data = extract_relationship_data_from_wikipedia_html(html)

    assert data["global_partner_roles"] == [{"name": "John Barlow", "role": "Husband"}]
    assert data["kid_names"] == ["Jack Barlow", "Henry Barlow"]


def test_extract_relationship_data_from_wikipedia_html_strips_template_noise_and_splits_entries() -> None:
    html = """
    <table class="infobox biography vcard">
      <tr>
        <th>Spouse</th>
        <td>
          .mw-parser-output .marriage-line-margin2px{line-height:0;margin-bottom:-2px}
          Frank William Gay III [4] (m. 2000; div. 2014);
          .mw-parser-output .marriage-display-inline{display:inline}
          Jake Burton (m. 2018)
        </td>
      </tr>
    </table>
    """
    data = extract_relationship_data_from_wikipedia_html(html)

    names = {row["name"] for row in data["global_partner_roles"]}
    assert names == {"Frank William Gay III", "Jake Burton"}
    assert all(".mw-parser-output" not in name for name in names)
    assert all(row["role"] == "Husband" for row in data["global_partner_roles"])


def test_is_missing_wikipedia_page_detects_article_missing_notice() -> None:
    html = "<div>Wikipedia does not have an article with this exact name.</div>"
    assert is_missing_wikipedia_page(html, "https://en.wikipedia.org/wiki/Georgia_Gay")


def test_is_missing_fandom_page_detects_empty_page_notice() -> None:
    html = "<div>There is currently no text in this page.</div>"
    assert is_missing_fandom_page(html, "https://real-housewives.fandom.com/wiki/Georgia_Gay")
