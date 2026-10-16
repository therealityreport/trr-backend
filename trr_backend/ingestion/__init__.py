"""Ingestion helpers, with legacy show-import exports loaded on demand."""

__all__ = [
    "CandidateShow",
    "ImdbListItem",
    "TmdbListItem",
    "merge_candidates",
    "merge_candidate_shows",
    "parse_imdb_list_url",
]


def __getattr__(name):
    if name not in __all__:
        raise AttributeError(name)
    from trr_backend.ingestion import shows_from_lists

    return getattr(shows_from_lists, name)
