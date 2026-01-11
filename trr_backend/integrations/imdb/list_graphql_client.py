from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import requests

IMDB_GRAPHQL_API_BASE_URL = "https://api.graphql.imdb.com/"
IMDB_GRAPHQL_OPERATION_TITLE_LIST_MAIN_PAGE = "TitleListMainPage"
IMDB_GRAPHQL_TITLE_LIST_MAIN_PAGE_SHA256 = "0a1b6c8794a5858e2cfde0f43eac58ede044617135662963f12abcf21116b4f7"


class ImdbGraphqlError(RuntimeError):
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


class HttpImdbListGraphqlClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        extra_headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._extra_headers = dict(extra_headers or {})

    def fetch_title_list_main_page(
        self,
        list_id: str,
        *,
        locale: str = "en-US",
        first: int = 250,
        jump_to_position: int = 1,
        sort_by: str = "LIST_ORDER",
        sort_order: str = "ASC",
    ) -> dict[str, Any]:
        variables = {
            "lsConst": str(list_id),
            "first": int(first),
            "jumpToPosition": int(jump_to_position),
            "locale": str(locale),
            "sort": {"by": str(sort_by), "order": str(sort_order)},
            # Required by IMDb GraphQL schema (observed in production requests).
            "isInPace": False,
        }
        extensions = {
            "persistedQuery": {"sha256Hash": IMDB_GRAPHQL_TITLE_LIST_MAIN_PAGE_SHA256, "version": 1},
        }
        params = {
            "operationName": IMDB_GRAPHQL_OPERATION_TITLE_LIST_MAIN_PAGE,
            "variables": _json_dumps_compact(variables),
            "extensions": _json_dumps_compact(extensions),
        }

        # IMDb GraphQL expects content-type even for GET requests.
        headers = {
            "accept": "application/graphql+json, application/json",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0",
            **self._extra_headers,
        }

        resp = self._session.get(
            IMDB_GRAPHQL_API_BASE_URL,
            params=params,
            headers=headers,
            timeout=self._timeout_seconds,
        )

        body = resp.text or ""
        if resp.status_code != 200:
            snippet = body[:200].replace("\n", " ").strip()
            raise ImdbGraphqlError(
                f"IMDb GraphQL request failed (HTTP {resp.status_code}).",
                status_code=resp.status_code,
                body_snippet=snippet,
            )

        try:
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            snippet = body[:200].replace("\n", " ").strip()
            raise ImdbGraphqlError("IMDb GraphQL response was not valid JSON.", body_snippet=snippet) from exc

        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages: list[str] = []
            for err in errors:
                if isinstance(err, Mapping) and isinstance(err.get("message"), str):
                    messages.append(err["message"])
            summary = "; ".join(messages) if messages else str(errors)
            raise ImdbGraphqlError(f"IMDb GraphQL returned errors: {summary}")

        data = payload.get("data")
        if not isinstance(data, Mapping):
            raise ImdbGraphqlError("IMDb GraphQL response missing top-level `data`.")

        return payload
