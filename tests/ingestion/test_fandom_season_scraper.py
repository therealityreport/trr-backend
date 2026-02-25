from __future__ import annotations

from trr_backend.ingestion.fandom_season_scraper import parse_fandom_season_html


def test_fandom_season_dynamic_sections_include_canonical_and_unknown() -> None:
    html = """
    <html>
      <body>
        <div class="mw-parser-output">
          <p>Season intro summary.</p>
          <h2>Cast</h2>
          <p>Main cast text.</p>
          <h2>Production Notes</h2>
          <ul><li>Filmed in Utah</li></ul>
        </div>
      </body>
    </html>
    """
    payload = parse_fandom_season_html(
        html,
        source_url="https://real-housewives.fandom.com/wiki/Season_1",
    )
    sections = payload.get("dynamic_sections") or []
    assert any(section.get("canonical_title") == "Casting" for section in sections)
    assert any(
        section.get("title") == "Production Notes" and section.get("canonical_title") == "Production Notes"
        for section in sections
    )
