"""
IMDb episodic credits client.

Implements the persisted-query operation `TitleEpisodeBottomSheetCredits` against
`https://caching.graphql.imdb.com` and normalizes episodic credits for a given
series + person pair.

Automated tests for this module should never call the live IMDb endpoint. Use
fixtures and validate normalization helpers directly.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import requests

ImdbTitleId = str  # e.g. "tt0944947"
ImdbNameId = str  # e.g. "nm0000148"

IMDB_GRAPHQL_BASE_URL = "https://caching.graphql.imdb.com"
IMDB_GRAPHQL_OPERATION_TITLE_EPISODE_BOTTOM_SHEET_CREDITS = "TitleEpisodeBottomSheetCredits"
IMDB_GRAPHQL_PERSISTED_QUERY_SHA256 = "8af4e7a49ccd298796aec3d0aa5699c05a6b9efb23721f7eae98bb19b72eafa3"

# Example job category id (used by debug harness and as an optional default).
IMDB_JOB_CATEGORY_SELF = "amzn1.imdb.concept.name_credit_group.7caf7d16-5db9-4f4f-8864-d4c6e711c686"


@dataclass(frozen=True)
class ImdbEpisodeRef:
    """Normalized episode identity and basic metadata."""

    title_id: ImdbTitleId
    season_number: int | None = None
    episode_number: int | None = None
    title: str | None = None
    year: int | None = None
    air_date: str | None = None  # ISO-8601 date string when available

    @property
    def episode_code(self) -> str | None:
        if self.season_number is None or self.episode_number is None:
            return None
        return f"S{self.season_number}.E{self.episode_number}"


@dataclass(frozen=True)
class ImdbEpisodeCredit:
    """A normalized credit for a person on a specific episode."""

    episode: ImdbEpisodeRef
    credit_category: str | None = None  # e.g. "Self"
    job: str | None = None  # e.g. "Self", "Director"
    attributes: Sequence[str] = ()
    characters: Sequence[str] = ()
    is_archive_footage: bool = False
    billing_order: int | None = None
    raw: Mapping[str, Any] | None = None  # optional raw payload for debugging


@dataclass(frozen=True)
class ImdbEpisodicCredits:
    """Normalized episodic credits for one (series, person) pair."""

    series_id: ImdbTitleId
    person_id: ImdbNameId
    credits: Sequence[ImdbEpisodeCredit]


class ImdbEpisodicClient(Protocol):
    """
    Port used by the rest of the system to retrieve episodic credits from IMDb.

    Implementations may:
    - call IMDb's GraphQL endpoint directly
    - cache responses
    - perform retries/rate limiting
    """

    def get_episodic_credits(
        self,
        *,
        series_id: ImdbTitleId,
        person_id: ImdbNameId,
    ) -> ImdbEpisodicCredits: ...


class ImdbClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_snippet: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet


def _json_dumps_compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v in seen:
            continue
        out.append(v)
        seen.add(v)
    return out


def _extract_credits_v2_node(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ImdbClientError("Missing `data` in IMDb response.")

    title = data.get("title")
    if not isinstance(title, Mapping):
        raise ImdbClientError("Missing `data.title` in IMDb response.")

    credits_v2 = title.get("creditsV2")
    if not isinstance(credits_v2, Mapping):
        raise ImdbClientError("Missing `data.title.creditsV2` in IMDb response.")

    edges = credits_v2.get("edges")
    if not isinstance(edges, list) or not edges:
        raise ImdbClientError("Missing `data.title.creditsV2.edges` in IMDb response.")

    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        node = edge.get("node")
        if isinstance(node, Mapping):
            return node

    raise ImdbClientError("Missing `data.title.creditsV2.edges[].node` in IMDb response.")


def _parse_available_seasons_from_payload(payload: Mapping[str, Any]) -> list[int]:
    node = _extract_credits_v2_node(payload)

    nav = node.get("nav")
    if not isinstance(nav, Mapping):
        return []

    displayable_seasons = nav.get("displayableSeasons")
    if not isinstance(displayable_seasons, Mapping):
        return []

    edges = displayable_seasons.get("edges")
    if not isinstance(edges, list):
        return []

    seasons: list[int] = []
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        edge_node = edge.get("node")
        if not isinstance(edge_node, Mapping):
            continue
        season_int = _parse_optional_int(edge_node.get("season"))
        if season_int is None:
            continue
        seasons.append(season_int)

    return sorted(set(seasons))


def _normalize_episode_credit_node(node: Mapping[str, Any]) -> ImdbEpisodeCredit | None:
    title_obj = node.get("title")
    if not isinstance(title_obj, Mapping):
        return None

    title_id = title_obj.get("id")
    if not isinstance(title_id, str) or not title_id:
        return None

    release_year = None
    release_year_obj = title_obj.get("releaseYear")
    if isinstance(release_year_obj, Mapping):
        release_year = _parse_optional_int(release_year_obj.get("year"))

    title_text = None
    title_text_obj = title_obj.get("titleText")
    if isinstance(title_text_obj, Mapping):
        raw_title_text = title_text_obj.get("text")
        if isinstance(raw_title_text, str) and raw_title_text.strip():
            title_text = raw_title_text.strip()

    season_number = None
    episode_number = None
    series_obj = title_obj.get("series")
    if isinstance(series_obj, Mapping):
        den = series_obj.get("displayableEpisodeNumber")
        if isinstance(den, Mapping):
            ds = den.get("displayableSeason")
            if isinstance(ds, Mapping):
                season_number = _parse_optional_int(ds.get("text"))
            en = den.get("episodeNumber")
            if isinstance(en, Mapping):
                episode_number = _parse_optional_int(en.get("text"))

    episode_ref = ImdbEpisodeRef(
        title_id=title_id,
        season_number=season_number,
        episode_number=episode_number,
        title=title_text,
        year=release_year,
    )

    credited_roles = node.get("creditedRoles")
    role_edges = credited_roles.get("edges") if isinstance(credited_roles, Mapping) else None
    if not isinstance(role_edges, list) or not role_edges:
        return ImdbEpisodeCredit(episode=episode_ref, raw=node)

    primary_job: str | None = None
    primary_category: str | None = None
    all_attributes: list[str] = []
    all_characters: list[str] = []

    for role_edge in role_edges:
        if not isinstance(role_edge, Mapping):
            continue
        role_node = role_edge.get("node")
        if not isinstance(role_node, Mapping):
            continue

        if primary_job is None:
            text = role_node.get("text")
            if isinstance(text, str) and text.strip():
                primary_job = text.strip()

        if primary_category is None:
            category = role_node.get("category")
            if isinstance(category, Mapping):
                cat_text = category.get("text")
                if isinstance(cat_text, str) and cat_text.strip():
                    primary_category = cat_text.strip()

        attributes = role_node.get("attributes")
        if isinstance(attributes, list):
            for attr in attributes:
                if not isinstance(attr, Mapping):
                    continue
                attr_text = attr.get("text")
                if isinstance(attr_text, str) and attr_text.strip():
                    all_attributes.append(attr_text.strip())

        characters = role_node.get("characters")
        char_edges = characters.get("edges") if isinstance(characters, Mapping) else None
        if isinstance(char_edges, list):
            for char_edge in char_edges:
                if not isinstance(char_edge, Mapping):
                    continue
                char_node = char_edge.get("node")
                if not isinstance(char_node, Mapping):
                    continue
                name = char_node.get("name")
                if isinstance(name, str) and name.strip():
                    all_characters.append(name.strip())

    deduped_attributes = _dedupe_preserve_order(all_attributes)
    deduped_characters = _dedupe_preserve_order(all_characters)
    is_archive_footage = any("archive footage" in a.casefold() for a in deduped_attributes)

    return ImdbEpisodeCredit(
        episode=episode_ref,
        credit_category=primary_category,
        job=primary_job,
        attributes=deduped_attributes,
        characters=deduped_characters,
        is_archive_footage=is_archive_footage,
        raw=node,
    )


@dataclass(frozen=True)
class ImdbEpisodeCreditsPage:
    credits: Sequence[ImdbEpisodeCredit]
    end_cursor: str
    has_next_page: bool


def _parse_episode_credits_page_from_payload(payload: Mapping[str, Any]) -> ImdbEpisodeCreditsPage:
    node = _extract_credits_v2_node(payload)

    episode_credits = node.get("episodeCredits")
    if not isinstance(episode_credits, Mapping):
        raise ImdbClientError("Missing `episodeCredits` in IMDb response.")

    edges_any = episode_credits.get("edges", [])
    edges: list[Any] = edges_any if isinstance(edges_any, list) else []

    credits: list[ImdbEpisodeCredit] = []
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        edge_node = edge.get("node")
        if not isinstance(edge_node, Mapping):
            continue
        normalized = _normalize_episode_credit_node(edge_node)
        if normalized is not None:
            credits.append(normalized)

    page_info = episode_credits.get("pageInfo")
    if not isinstance(page_info, Mapping):
        page_info = {}

    has_next_page = bool(page_info.get("hasNextPage"))
    end_cursor_raw = page_info.get("endCursor")
    end_cursor = end_cursor_raw.strip() if isinstance(end_cursor_raw, str) else ""

    if has_next_page and not end_cursor:
        raise ImdbClientError("IMDb response has `hasNextPage=true` but no `endCursor`.")

    return ImdbEpisodeCreditsPage(credits=credits, end_cursor=end_cursor, has_next_page=has_next_page)


class HttpImdbEpisodicClient(ImdbEpisodicClient):
    """
    HTTP client for IMDb episodic credits via persisted GraphQL query.

    `get_episodic_credits()` is provided for protocol compatibility, but IMDb's
    GraphQL operation requires a job category id; use `fetch_*` methods directly
    unless you construct the client with `default_job_category_id`.
    """

    def __init__(
        self,
        *,
        base_url: str = IMDB_GRAPHQL_BASE_URL,
        timeout_seconds: float = 20.0,
        extra_headers: Mapping[str, str] | None = None,
        session: requests.Session | None = None,
        default_job_category_id: str | None = IMDB_JOB_CATEGORY_SELF,
        default_locale: str = "en-US",
    ) -> None:
        self._base_url = base_url
        self._timeout_seconds = timeout_seconds
        self._session = session or requests.Session()
        self._extra_headers = dict(extra_headers or {})
        self._default_job_category_id = default_job_category_id
        self._default_locale = default_locale

    def _build_headers(self, extra_headers: Mapping[str, str] | None) -> dict[str, str]:
        headers: dict[str, str] = {
            "accept": "application/graphql+json, application/json",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0",
        }
        headers.update(self._extra_headers)
        if extra_headers:
            headers.update(dict(extra_headers))
        return headers

    def _request_title_episode_bottom_sheet_credits(
        self,
        *,
        title_id: ImdbTitleId,
        name_id: ImdbNameId,
        job_category_id: str,
        season: int,
        after: str,
        locale: str,
        extra_headers: Mapping[str, str] | None,
    ) -> dict[str, Any]:
        variables = {
            "after": after,
            "episodeCreditsFilter": {"episodes": {"includeSeasons": [str(season)]}},
            "jobCategoryId": job_category_id,
            "locale": locale,
            "nameId": name_id,
            "projectStatus": [],
            "titleId": title_id,
            "useEntitlement": False,
        }
        extensions = {
            "persistedQuery": {
                "sha256Hash": IMDB_GRAPHQL_PERSISTED_QUERY_SHA256,
                "version": 1,
            }
        }

        params = {
            "operationName": IMDB_GRAPHQL_OPERATION_TITLE_EPISODE_BOTTOM_SHEET_CREDITS,
            "variables": _json_dumps_compact(variables),
            "extensions": _json_dumps_compact(extensions),
        }

        headers = self._build_headers(extra_headers)

        try:
            resp = self._session.get(
                self._base_url,
                params=params,
                headers=headers,
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            raise ImdbClientError(f"IMDb request failed: {exc}") from exc

        if resp.status_code != 200:
            snippet = (resp.text or "")[:400]
            raise ImdbClientError(
                f"IMDb request failed with HTTP {resp.status_code}.",
                status_code=resp.status_code,
                body_snippet=snippet,
            )

        try:
            payload = resp.json()
        except ValueError as exc:
            snippet = (resp.text or "")[:400]
            raise ImdbClientError(
                "IMDb returned non-JSON response.",
                status_code=resp.status_code,
                body_snippet=snippet,
            ) from exc

        if not isinstance(payload, Mapping):
            raise ImdbClientError("IMDb returned unexpected JSON shape (not an object).")

        errors = payload.get("errors")
        if errors:
            if isinstance(errors, list):
                messages: list[str] = []
                for err in errors:
                    if isinstance(err, Mapping):
                        msg = err.get("message")
                        messages.append(str(msg) if msg is not None else str(err))
                    else:
                        messages.append(str(err))
                summary = "; ".join(messages[:5])
            else:
                summary = str(errors)
            raise ImdbClientError(f"IMDb GraphQL error(s): {summary}")

        _extract_credits_v2_node(payload)
        return dict(payload)

    def fetch_available_seasons(
        self,
        title_id: ImdbTitleId,
        name_id: ImdbNameId,
        job_category_id: str,
        *,
        locale: str = "en-US",
        extra_headers: Mapping[str, str] | None = None,
    ) -> list[int]:
        payload = self._request_title_episode_bottom_sheet_credits(
            title_id=title_id,
            name_id=name_id,
            job_category_id=job_category_id,
            season=1,
            after="",
            locale=locale,
            extra_headers=extra_headers,
        )
        return _parse_available_seasons_from_payload(payload)

    def fetch_episode_credits_for_seasons(
        self,
        title_id: ImdbTitleId,
        name_id: ImdbNameId,
        job_category_id: str,
        *,
        seasons: Sequence[int],
        locale: str = "en-US",
        extra_headers: Mapping[str, str] | None = None,
    ) -> list[ImdbEpisodeCredit]:
        credits: list[ImdbEpisodeCredit] = []

        for season in seasons:
            after = ""
            while True:
                payload = self._request_title_episode_bottom_sheet_credits(
                    title_id=title_id,
                    name_id=name_id,
                    job_category_id=job_category_id,
                    season=int(season),
                    after=after,
                    locale=locale,
                    extra_headers=extra_headers,
                )
                page = _parse_episode_credits_page_from_payload(payload)
                credits.extend(page.credits)

                if not page.has_next_page:
                    break
                after = page.end_cursor

        return credits

    def get_episodic_credits(
        self,
        *,
        series_id: ImdbTitleId,
        person_id: ImdbNameId,
    ) -> ImdbEpisodicCredits:
        if not self._default_job_category_id:
            raise ImdbClientError(
                "HttpImdbEpisodicClient is missing `default_job_category_id`. "
                "Call fetch_episode_credits_for_seasons() or set `default_job_category_id`."
            )

        seasons = self.fetch_available_seasons(
            series_id,
            person_id,
            self._default_job_category_id,
            locale=self._default_locale,
        )
        credits = self.fetch_episode_credits_for_seasons(
            series_id,
            person_id,
            self._default_job_category_id,
            seasons=seasons,
            locale=self._default_locale,
        )
        return ImdbEpisodicCredits(series_id=series_id, person_id=person_id, credits=credits)


class NotImplementedImdbEpisodicClient:
    """Placeholder implementation to make wiring explicit until implemented."""

    def get_episodic_credits(
        self,
        *,
        series_id: ImdbTitleId,
        person_id: ImdbNameId,
    ) -> ImdbEpisodicCredits:
        raise NotImplementedError(
            "IMDb episodic credits client not implemented yet. "
            "Use `HttpImdbEpisodicClient` from `trr_backend.integrations.imdb.episodic_client`."
        )


def debug_print_example(extra_headers: Mapping[str, str] | None = None) -> None:
    """
    Manual integration test using the captured RHOSLC example:

    - title_id = "tt11363282"
    - name_id = "nm1278492"
    - job_category_id = "amzn1.imdb.concept.name_credit_group.7caf7d16-5db9-4f4f-8864-d4c6e711c686"

    Fetch available seasons, then season 5 credits, and print one line per episode.
    """

    title_id = "tt11363282"
    name_id = "nm1278492"
    job_category_id = IMDB_JOB_CATEGORY_SELF

    client = HttpImdbEpisodicClient(extra_headers=extra_headers)
    seasons = client.fetch_available_seasons(title_id, name_id, job_category_id)
    print("Available seasons:", seasons)

    credits = client.fetch_episode_credits_for_seasons(
        title_id,
        name_id,
        job_category_id,
        seasons=[5],
    )

    for credit in credits:
        code = credit.episode.episode_code or "S?.E?"
        year = credit.episode.year
        title = credit.episode.title or ""
        credit_text = credit.job or ""
        attrs = ", ".join(credit.attributes)
        suffix = f" ({attrs})" if attrs else ""
        print(
            f"{code} | {credit.episode.title_id} | {year or ''} | {title} | "
            f"{credit_text}{suffix} | archive={credit.is_archive_footage}"
        )


if __name__ == "__main__":
    debug_print_example()
