from datetime import datetime

from trr_backend.socials.twitter.query import build_twitter_search_query, normalize_twitter_search_window
from trr_backend.socials.twitter.scraper import TwitterScrapeConfig

DATE_START = datetime(2026, 1, 1)
DATE_END = datetime(2026, 1, 11)


def _config(query: str) -> TwitterScrapeConfig:
    return TwitterScrapeConfig(query=query, date_start=DATE_START, date_end=DATE_END)


def test_hashtag_passthrough():
    # #RHOSLC should appear exactly once, not as "#RHOSLC OR ##RHOSLC"
    q = _config("#RHOSLC").build_search_query()
    assert q.startswith("#RHOSLC ")  # space confirms the date filter follows directly
    assert "OR ##RHOSLC" not in q


def test_mention_passthrough():
    q = _config("@BravoTV").build_search_query()
    # Should be "@BravoTV since:... until:..." — no quoting, no #@BravoTV
    assert q.startswith("@BravoTV ")
    assert "#@BravoTV" not in q
    assert '"@BravoTV"' not in q


def test_plain_text_wrapped():
    q = _config("RHOSLC").build_search_query()
    assert '"RHOSLC" OR #RHOSLC' in q


def test_route_independent_query_helper_matches_config_method():
    config = _config("RHOSLC")
    assert build_twitter_search_query(config.query, config.date_start, config.date_end) == config.build_search_query()


def test_advanced_passthrough():
    raw = "from:BravoTV OR from:Andy"
    q = _config(raw).build_search_query()
    assert q.startswith(raw)


def test_date_filters_always_appended():
    for term in ("#RHOSLC", "@BravoTV", "RHOSLC"):
        q = _config(term).build_search_query()
        assert "since:2026-01-01" in q
        assert "until:2026-01-12" in q


def test_whole_day_window_normalization_ignores_time_components():
    start, end_exclusive = normalize_twitter_search_window(
        datetime(2026, 1, 1, 9, 30, 45),
        datetime(2026, 1, 11, 17, 45, 12),
    )

    assert start.isoformat() == "2026-01-01T00:00:00"
    assert end_exclusive.isoformat() == "2026-01-12T00:00:00"
