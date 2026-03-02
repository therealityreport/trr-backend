from __future__ import annotations

from pathlib import Path

from trr_backend.integrations.imdb.title_page_metadata import parse_imdb_title_html


def test_parse_imdb_title_page_metadata_from_fixture() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    html = (repo_root / "tests" / "fixtures" / "imdb" / "title_page_tt8819906_sample.html").read_text(encoding="utf-8")

    result = parse_imdb_title_html(html, imdb_id="tt8819906")

    assert result["title"] == "Love Island USA"
    assert (
        result["description"]
        == "U.S. version of the British show 'Love Island' where a group of singles come to stay in a villa for a few weeks and have to couple up with one another."  # noqa: E501
    )
    assert "Reality TV" in result["tags"]
    assert "Reality TV Dating" in result["tags"]
    assert "Reality-TV" in result["genres"]
    assert "Game-Show" in result["genres"]
    assert result["content_rating"] == "TV-MA"
    assert result["aggregate_rating_value"] == 5.2
    assert result["aggregate_rating_count"] == 3291


def test_parse_imdb_title_page_metadata_extracts_episode_parent_series_fields() -> None:
    html = """
    <html>
      <head>
        <title>Reunion Part 3 - IMDb</title>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"TVEpisode",
            "name":"Reunion Part 3",
            "datePublished":"2025-04-15",
            "episodeNumber":"20",
            "partOfSeason":{"@type":"TVSeason","name":"Season 14","url":"https://www.imdb.com/title/tt4789318/episodes/?season=14"},
            "partOfSeries":{"@type":"TVSeries","name":"The Real Housewives of Beverly Hills","url":"https://www.imdb.com/title/tt1720601/"}
          }
        </script>
      </head>
      <body></body>
    </html>
    """

    result = parse_imdb_title_html(html, imdb_id="tt35051926")

    assert result["title"] == "Reunion Part 3"
    assert result["title_type"] == "TVEpisode"
    assert result["episode_number"] == 20
    assert result["season_number"] == 14
    assert result["series_title"] == "The Real Housewives of Beverly Hills"
    assert result["series_imdb_id"] == "tt1720601"
    assert result["episode_air_date"] == "2025-04-15"


def test_parse_imdb_episode_metadata_falls_back_to_html_title_for_series_name() -> None:
    html = """
    <html>
      <head>
        <title>Milo Ventimiglia &amp; Alan Cumming - Watch What Happens Live with Andy Cohen - IMDb</title>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"TVEpisode",
            "name":"Milo Ventimiglia &amp; Alan Cumming",
            "datePublished":"2023-03-15",
            "episodeNumber":"37",
            "partOfSeason":{"@type":"TVSeason","name":"Season 20"}
          }
        </script>
      </head>
      <body></body>
    </html>
    """

    result = parse_imdb_title_html(html, imdb_id="tt26755932")

    assert result["title"] == "Milo Ventimiglia & Alan Cumming"
    assert result["title_type"] == "TVEpisode"
    assert result["series_title"] == "Watch What Happens Live with Andy Cohen"
    assert result["series_imdb_id"] is None


def test_parse_imdb_episode_metadata_extracts_series_id_from_anchor_link() -> None:
    html = """
    <html>
      <head>
        <title>
          &quot;Watch What Happens Live with Andy Cohen&quot;
          Milo Ventimiglia &amp; Alan Cumming (TV Episode 2023) - IMDb
        </title>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"TVEpisode",
            "name":"Milo Ventimiglia &amp; Alan Cumming",
            "datePublished":"2023-03-15",
            "episodeNumber":"37",
            "partOfSeason":{"@type":"TVSeason","name":"Season 20"}
          }
        </script>
      </head>
      <body>
        <a href="/title/tt0318220/?ref_=tt_ov_srs">Watch What Happens Live with Andy Cohen</a>
      </body>
    </html>
    """

    result = parse_imdb_title_html(html, imdb_id="tt26755932")

    assert result["title"] == "Milo Ventimiglia & Alan Cumming"
    assert result["title_type"] == "TVEpisode"
    assert result["series_title"] == "Watch What Happens Live with Andy Cohen"
    assert result["series_imdb_id"] == "tt0318220"


def test_parse_imdb_episode_metadata_falls_back_to_html_title_with_year_in_episode_segment() -> None:
    html = """
    <html>
      <head>
        <title>The Power of the Seer (2025) - The Traitors - IMDb</title>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"TVEpisode",
            "name":"The Power of the Seer",
            "datePublished":"2025-02-27",
            "episodeNumber":"10",
            "partOfSeason":{"@type":"TVSeason","name":"Season 3"}
          }
        </script>
      </head>
      <body></body>
    </html>
    """

    result = parse_imdb_title_html(html, imdb_id="tt35000010")

    assert result["title"] == "The Power of the Seer"
    assert result["title_type"] == "TVEpisode"
    assert result["series_title"] == "The Traitors"
    assert result["series_imdb_id"] is None
