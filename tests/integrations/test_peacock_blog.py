from __future__ import annotations

from trr_backend.integrations import peacock_blog


def test_full_size_image_url_removes_drupal_style_derivative() -> None:
    assert (
        peacock_blog.full_size_image_url(
            "https://www.peacocktv.com/sites/peacock/files/styles/scale_600/public/2026/06/example.jpg"
        )
        == "https://www.peacocktv.com/sites/peacock/files/2026/06/example.jpg"
    )


def test_extract_cast_images_from_html_prefers_original_urls_and_preserves_preview() -> None:
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {
            "@type": "NewsArticle",
            "image": [
              "https://www.peacocktv.com/sites/peacock/files/styles/scale_600/public/2026/06/pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg"
            ]
          }
        </script>
      </head>
      <body>
        <img
          src="/sites/peacock/files/styles/scale_600/public/2026/06/pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg"
          alt="Keyon Love Island USA Season 8 Casa Amor bombshell"
        />
      </body>
    </html>
    """

    rows = peacock_blog.extract_cast_images_from_html(
        html,
        page_url="https://www.peacocktv.com/blog/love-island-usa-season-8-cast",
    )

    assert len(rows) == 1
    assert rows[0]["image_url"] == (
        "https://www.peacocktv.com/sites/peacock/files/2026/06/"
        "pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg"
    )
    assert rows[0]["preview_image_url"] == (
        "https://www.peacocktv.com/sites/peacock/files/styles/scale_600/public/2026/06/"
        "pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg"
    )
    assert rows[0]["source_label"] == "Peacock Blog"
    assert rows[0]["width"] == 1080
    assert rows[0]["height"] == 1350


def test_collect_cast_images_matches_person_by_first_name_filename(monkeypatch) -> None:
    rows = [
        {
            "image_url": "https://www.peacocktv.com/sites/peacock/files/2026/06/keyon.jpg",
            "source_url": "https://www.peacocktv.com/sites/peacock/files/2026/06/keyon.jpg",
            "file_name": "pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg",
            "alt_text": "Keyon Love Island USA Season 8 Casa Amor bombshell",
        },
        {
            "image_url": "https://www.peacocktv.com/sites/peacock/files/2026/06/kyle.jpg",
            "source_url": "https://www.peacocktv.com/sites/peacock/files/2026/06/kyle.jpg",
            "file_name": "pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_kyle.jpg",
            "alt_text": "Kyle Love Island USA Season 8 Casa Amor bombshell",
        },
    ]
    monkeypatch.setattr(peacock_blog, "fetch_cast_images", lambda **_kwargs: rows)

    result = peacock_blog.collect_cast_images(
        show_name="Love Island USA",
        season=8,
        person_name="Keyon Harry",
    )

    assert [row["file_name"] for row in result] == [
        "pea_lis8_casaamor_characterportrait_titlesocial_1080x1350_keyon.jpg"
    ]
    assert result[0]["people_names"] == ["Keyon Harry"]
