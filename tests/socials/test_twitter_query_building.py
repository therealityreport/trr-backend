from datetime import datetime

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


def test_advanced_passthrough():
    raw = 'from:BravoTV OR from:Andy'
    q = _config(raw).build_search_query()
    assert q.startswith(raw)


def test_date_filters_always_appended():
    for term in ("#RHOSLC", "@BravoTV", "RHOSLC"):
        q = _config(term).build_search_query()
        assert "since:2026-01-01" in q
        assert "until:2026-01-11" in q
