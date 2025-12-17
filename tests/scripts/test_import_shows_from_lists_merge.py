from __future__ import annotations

from trr_backend.ingestion.shows_from_lists import CandidateShow, merge_candidates


def test_merge_candidates_dedupes_by_imdb_id_and_accumulates_tags() -> None:
    c1 = CandidateShow(
        imdb_id="tt1111111",
        tmdb_id=None,
        title="Sample Series One",
        source_tags={"imdb-list:ls123"},
    )
    c2 = CandidateShow(
        imdb_id="tt1111111",
        tmdb_id=999,
        title="Sample Series One (TMDb)",
        first_air_date="2020-01-01",
        source_tags={"tmdb-list:555"},
    )

    merged = merge_candidates([c1, c2])
    assert len(merged) == 1
    assert merged[0].imdb_id == "tt1111111"
    assert merged[0].tmdb_id == 999
    assert merged[0].source_tags == {"imdb-list:ls123", "tmdb-list:555"}
    assert merged[0].first_air_date == "2020-01-01"


def test_merge_candidates_dedupes_by_tmdb_id() -> None:
    c1 = CandidateShow(
        imdb_id=None,
        tmdb_id=123,
        title="TMDb Only",
        source_tags={"tmdb-list:1"},
    )
    c2 = CandidateShow(
        imdb_id="tt2222222",
        tmdb_id=123,
        title="TMDb Only (with IMDb)",
        source_tags={"imdb-list:ls999"},
    )

    merged = merge_candidates([c1, c2])
    assert len(merged) == 1
    assert merged[0].tmdb_id == 123
    assert merged[0].imdb_id == "tt2222222"
    assert merged[0].source_tags == {"tmdb-list:1", "imdb-list:ls999"}


def test_merge_candidates_strict_title_year_merge_when_no_ids() -> None:
    c1 = CandidateShow(imdb_id=None, tmdb_id=None, title="Same Title", year=2021)
    c2 = CandidateShow(imdb_id=None, tmdb_id=None, title="Same Title", year=2021, source_tags={"imdb-list:ls1"})
    c3 = CandidateShow(imdb_id=None, tmdb_id=None, title="Different Title", year=2021)

    merged = merge_candidates([c1, c2, c3])
    assert len(merged) == 2
    merged_titles = sorted(c.title for c in merged)
    assert merged_titles == ["Different Title", "Same Title"]
    same = next(c for c in merged if c.title == "Same Title")
    assert same.source_tags == {"imdb-list:ls1"}

