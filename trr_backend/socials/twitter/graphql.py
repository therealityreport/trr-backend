from __future__ import annotations


def classify_search_transport(retrieval_mode: str | None) -> str:
    normalized = str(retrieval_mode or "").strip().lower()
    if normalized in {"graphql", "twikit", "syndication", "playwright"}:
        return normalized
    return "graphql"
