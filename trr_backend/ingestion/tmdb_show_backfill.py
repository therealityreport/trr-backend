from __future__ import annotations

import re
from typing import Any, Mapping

_TITLE_CLEAN_RE = re.compile(r"[^a-z0-9]+")


def _normalize_title(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip().casefold()
    if not text:
        return None
    text = _TITLE_CLEAN_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _extract_year(value: str | None) -> int | None:
    if not isinstance(value, str):
        return None
    match = re.search(r"(\d{4})", value)
    if not match:
        return None
    year = match.group(1)
    if year.isdigit():
        return int(year)
    return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _name_score(show_norm: str | None, candidate_norm: str | None) -> int:
    if not show_norm or not candidate_norm:
        return 0
    if show_norm == candidate_norm:
        return 3
    if show_norm in candidate_norm or candidate_norm in show_norm:
        return 2
    show_tokens = set(show_norm.split())
    candidate_tokens = set(candidate_norm.split())
    if not show_tokens or not candidate_tokens:
        return 0
    overlap = len(show_tokens & candidate_tokens)
    union = len(show_tokens | candidate_tokens)
    if union and (overlap / union) >= 0.6:
        return 1
    return 0


def _score_tv_result(
    result: Mapping[str, Any],
    *,
    show_norm: str | None,
    show_year: int | None,
) -> tuple[int, int, float]:
    name_score = 0
    if show_norm:
        name_score = max(
            _name_score(show_norm, _normalize_title(result.get("name"))),
            _name_score(show_norm, _normalize_title(result.get("original_name"))),
        )

    year_score = 0
    if show_year is not None:
        candidate_year = _extract_year(result.get("first_air_date"))
        if candidate_year is None:
            year_score = -1000
        else:
            year_score = -abs(show_year - candidate_year)

    popularity = _coerce_float(result.get("popularity")) or 0.0
    return (name_score, year_score, popularity)


def select_tmdb_tv_result(
    tv_results: list[Mapping[str, Any]],
    *,
    show_name: str | None = None,
    premiere_date: str | None = None,
) -> tuple[int | None, str]:
    if not tv_results:
        return None, "no_tv_results"

    show_norm = _normalize_title(show_name)
    show_year = _extract_year(premiere_date)

    scored: list[tuple[tuple[int, int, float], int]] = []
    for item in tv_results:
        if not isinstance(item, Mapping):
            continue
        tmdb_id = _coerce_int(item.get("id"))
        if tmdb_id is None:
            continue
        score = _score_tv_result(item, show_norm=show_norm, show_year=show_year)
        scored.append((score, tmdb_id))

    if not scored:
        return None, "no_tv_results"
    if len(scored) == 1:
        return scored[0][1], "single_result"

    scored.sort(key=lambda item: item[0], reverse=True)
    top_score, top_id = scored[0]
    if len(scored) > 1 and scored[1][0] == top_score:
        return None, "ambiguous"

    name_score, year_score, _pop = top_score
    if show_norm and name_score == 0:
        if show_year is None or year_score <= -1000:
            return None, "ambiguous"
        return top_id, "year_match"
    if name_score > 0:
        return top_id, "name_match"
    return top_id, "heuristic_match"


def resolve_tmdb_id_from_find_payload(
    payload: Mapping[str, Any],
    *,
    show_name: str | None = None,
    premiere_date: str | None = None,
) -> tuple[int | None, str]:
    tv_results = payload.get("tv_results")
    if not isinstance(tv_results, list):
        return None, "no_tv_results"
    return select_tmdb_tv_result(tv_results, show_name=show_name, premiere_date=premiere_date)


def _as_str(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _as_int(value: Any) -> int | None:
    return _coerce_int(value)


def _as_float(value: Any) -> float | None:
    return _coerce_float(value)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def build_tmdb_show_patch(details: Mapping[str, Any], *, fetched_at: str) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "tmdb_meta": dict(details),
        "tmdb_fetched_at": fetched_at,
    }

    tmdb_name = _as_str(details.get("name"))
    if tmdb_name:
        patch["tmdb_name"] = tmdb_name

    tmdb_status = _as_str(details.get("status"))
    if tmdb_status:
        patch["tmdb_status"] = tmdb_status

    tmdb_type = _as_str(details.get("type"))
    if tmdb_type:
        patch["tmdb_type"] = tmdb_type

    first_air = _as_str(details.get("first_air_date"))
    if first_air:
        patch["tmdb_first_air_date"] = first_air

    last_air = _as_str(details.get("last_air_date"))
    if last_air:
        patch["tmdb_last_air_date"] = last_air

    vote_average = _as_float(details.get("vote_average"))
    if vote_average is not None:
        patch["tmdb_vote_average"] = vote_average

    vote_count = _as_int(details.get("vote_count"))
    if vote_count is not None:
        patch["tmdb_vote_count"] = vote_count

    popularity = _as_float(details.get("popularity"))
    if popularity is not None:
        patch["tmdb_popularity"] = popularity

    return patch


def extract_tmdb_network_ids(details: Mapping[str, Any]) -> list[int]:
    networks = details.get("networks")
    if not isinstance(networks, list):
        return []
    ids: list[int] = []
    for item in networks:
        if not isinstance(item, Mapping):
            continue
        network_id = _as_int(item.get("id"))
        if network_id is not None:
            ids.append(network_id)
    return ids


def extract_tmdb_production_company_ids(details: Mapping[str, Any]) -> list[int]:
    companies = details.get("production_companies")
    if not isinstance(companies, list):
        return []
    ids: list[int] = []
    for item in companies:
        if not isinstance(item, Mapping):
            continue
        company_id = _as_int(item.get("id"))
        if company_id is not None:
            ids.append(company_id)
    return ids


def needs_tmdb_enrichment(show_row: Mapping[str, Any]) -> bool:
    if not isinstance(show_row, Mapping):
        return False
    if _is_missing(show_row.get("tmdb_id")):
        return False

    required = (
        "tmdb_meta",
        "tmdb_fetched_at",
        "tmdb_vote_average",
        "tmdb_vote_count",
        "tmdb_popularity",
        "tmdb_first_air_date",
        "tmdb_last_air_date",
        "tmdb_status",
        "tmdb_type",
    )
    for key in required:
        if _is_missing(show_row.get(key)):
            return True
    return False
