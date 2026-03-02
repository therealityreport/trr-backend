"""IMDb GraphQL persisted query client with retry/pagination support."""

from __future__ import annotations

import json
import logging
import os
import random
import time
from typing import Any

import requests

from trr_backend.observability import inc_suppressed_path_conversion

logger = logging.getLogger(__name__)


class ImdbGraphQLError(RuntimeError):
    """Raised when IMDb GraphQL API request fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_data: dict[str, Any] | None = None,
        is_blocked: bool = False,  # 202/403/429
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        self.is_blocked = is_blocked


class ImdbGraphQLPersistedClient:
    """
    Client for IMDb's internal GraphQL persisted query API.

    Features:
    - Exponential backoff with jitter for transient errors
    - Automatic retry for 202/403/429 (blocked/rate-limited)
    - Limited retry for 500/502/503 (server errors)
    - Primary + fallback endpoint support
    - Cursor-based pagination helper
    - Optional custom headers (use with caution)

    Usage:
        >>> client = ImdbGraphQLPersistedClient ()
        >>> result = client.execute_query(
        ...     operation_name="TitleCreditPaginationV2",
        ...     sha256_hash="abc123...",
        ...     variables={"const": "tt1720601", "first": 250}
        ... )
        >>> edges = client.paginate_edges(
        ...     operation_name="TitleCreditPaginationV2",
        ...     sha256_hash="abc123...",
        ...     variables={"const": "tt1720601", "first": 250},
        ...     max_pages=10
        ... )
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        fallback_url: str | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
        retry_base_delay_sec: float | None = None,
    ) -> None:
        """
        Initialize GraphQL client.

        Args:
            base_url: Primary endpoint (defaults to IMDB_GRAPHQL_BASE_URL env var)
            fallback_url: Fallback endpoint if primary fails
            extra_headers: Optional HTTP headers (NEVER log these - may contain cookies)
            timeout_sec: Request timeout in seconds
            max_retries: Max retry attempts (excluding initial request)
            retry_base_delay_sec: Base delay for exponential backoff
        """
        self.base_url = base_url or os.getenv("IMDB_GRAPHQL_BASE_URL", "https://caching.graphql.imdb.com/")
        self.fallback_url = fallback_url or os.getenv("IMDB_GRAPHQL_FALLBACK_URL", "https://api.graphql.imdb.com/")
        self.timeout_sec = timeout_sec or float(os.getenv("IMDB_GRAPHQL_TIMEOUT_SEC", "30.0"))
        self.max_retries = max_retries if max_retries is not None else int(os.getenv("IMDB_GRAPHQL_MAX_RETRIES", "2"))
        self.retry_base_delay_sec = retry_base_delay_sec or float(os.getenv("IMDB_GRAPHQL_RETRY_BASE_DELAY_SEC", "2.0"))

        # Merge extra headers from environment JSON if present
        env_headers = os.getenv("IMDB_EXTRA_HEADERS_JSON", "").strip()
        merged_headers = {}
        if env_headers:
            try:
                merged_headers = json.loads(env_headers)
            except json.JSONDecodeError as exc:
                inc_suppressed_path_conversion("imdb_graphql_client", "malformed_env_headers_json")
                logger.warning("Ignoring malformed IMDB_EXTRA_HEADERS_JSON: %s", exc)

        # Override with explicitly provided headers
        if extra_headers:
            merged_headers.update(extra_headers)

        self._extra_headers = merged_headers
        self._session = requests.Session()

    def execute_query(
        self,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Execute a single persisted query request.

        Args:
            operation_name: GraphQL operation name (e.g., "TitleCreditPaginationV2")
            sha256_hash: Persisted query SHA-256 hash
            variables: GraphQL query variables

        Returns:
            Raw GraphQL response dict

        Raises:
            ImdbGraphQLError: If request fails after retries

        Note:
            For pagination, use paginate_edges() helper or specialized wrappers
            like fetch_title_credits_paginated_v2()
        """
        # Try primary endpoint first, then fallback
        endpoints = [self.base_url]
        if self.fallback_url and self.fallback_url != self.base_url:
            endpoints.append(self.fallback_url)

        last_error: ImdbGraphQLError | None = None

        for endpoint in endpoints:
            try:
                return self._execute_with_retry(endpoint, operation_name, sha256_hash, variables)
            except ImdbGraphQLError as exc:
                last_error = exc
                # If blocked or permanent error, don't try fallback
                if exc.is_blocked or (exc.status_code and 400 <= exc.status_code < 500):
                    raise
                # Try fallback endpoint if available
                continue

        # Both endpoints failed
        if last_error:
            raise last_error
        raise ImdbGraphQLError("GraphQL request failed (no response)")

    def _execute_with_retry(
        self,
        endpoint: str,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute request with exponential backoff retry logic."""
        # IMDb uses GET requests with URL parameters for persisted queries
        import json as json_lib

        base_url = f"{endpoint.rstrip('/')}"

        # Build URL parameters (use compact JSON for IMDb compatibility)
        params = {
            "operationName": operation_name,
            "variables": json_lib.dumps(variables, separators=(",", ":")),
            "extensions": json_lib.dumps(
                {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": sha256_hash,
                    }
                },
                separators=(",", ":"),
            ),
        }

        headers = {
            "accept": "application/graphql+json, application/json",
            "content-type": "application/json",
            **self._extra_headers,
        }

        last_exception: Exception | None = None
        last_response: requests.Response | None = None

        for attempt in range(1 + self.max_retries):
            try:
                resp = self._session.get(
                    base_url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_sec,
                )
                last_response = resp
            except requests.RequestException as exc:
                last_exception = exc
                if attempt < self.max_retries:
                    self._sleep_with_backoff(attempt)
                    continue
                raise ImdbGraphQLError(f"GraphQL request failed: {exc}") from exc

            # Success case (200-299)
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                    # Check for GraphQL errors in response
                    if "errors" in data:
                        errors = data.get("errors", [])
                        # Detect stale persisted query hash
                        for error in errors:
                            extensions = error.get("extensions", {})
                            code = extensions.get("code", "")
                            if code in {"PersistedQueryNotFound", "PersistedQueryNotSupported"}:
                                raise ImdbGraphQLError(
                                    f"Persisted query hash is stale or invalid (code: {code}). "
                                    f"Hash may need updating. Set IMDB_GRAPHQL_HASH_TITLE_CREDIT_PAGINATION_V2 "
                                    f"with current hash from browser DevTools.",
                                    status_code=resp.status_code,
                                    response_data=data,
                                )
                        # Other GraphQL errors
                        raise ImdbGraphQLError(
                            f"GraphQL returned errors: {data['errors']}",
                            response_data=data,
                        )
                    return data
                except (ValueError, json.JSONDecodeError) as exc:
                    raise ImdbGraphQLError(
                        f"GraphQL response not valid JSON: {exc}",
                        status_code=resp.status_code,
                    ) from exc

            # Determine if retryable
            is_blocked = resp.status_code in {202, 403, 429}
            is_server_error = resp.status_code in {500, 502, 503}
            should_retry = (is_blocked or is_server_error) and attempt < self.max_retries

            if should_retry:
                self._sleep_with_backoff(attempt)
                continue

            # Exhausted retries or non-retryable error
            raise ImdbGraphQLError(
                f"GraphQL request failed with HTTP {resp.status_code} (after {attempt + 1} attempt(s))",
                status_code=resp.status_code,
                is_blocked=is_blocked,
            )

        # Should never reach here
        if last_response:
            raise ImdbGraphQLError(
                f"GraphQL request failed with HTTP {last_response.status_code}",
                status_code=last_response.status_code,
            )
        if last_exception:
            raise ImdbGraphQLError(f"GraphQL request failed: {last_exception}") from last_exception
        raise ImdbGraphQLError("GraphQL request failed (no response)")

    def _sleep_with_backoff(self, attempt: int) -> None:
        """Sleep with exponential backoff and jitter."""
        delay = self.retry_base_delay_sec * (2**attempt)
        jitter = random.uniform(0, delay * 0.25)
        time.sleep(delay + jitter)

    def paginate_edges(
        self,
        operation_name: str,
        sha256_hash: str,
        variables: dict[str, Any],
        *,
        edges_path: str = "data.title.credits.edges",
        page_info_path: str = "data.title.credits.pageInfo",
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute paginated query and collect all edges.

        Handles cursor pagination automatically until hasNextPage=false
        or max_pages reached.

        Args:
            operation_name: GraphQL operation name
            sha256_hash: Persisted query hash
            variables: Initial query variables (must include "after" key for cursor)
            edges_path: JSONPath to edges array (e.g., "data.title.credits.edges")
            page_info_path: JSONPath to pageInfo object
            max_pages: Hard cap on page count (defaults to IMDB_GRAPHQL_MAX_PAGES)

        Returns:
            List of all edge nodes collected across pages

        Raises:
            ImdbGraphQLError: If pagination fails

        Example:
            >>> edges = client.paginate_edges(
            ...     "TitleCreditPaginationV2",
            ...     "abc123...",
            ...     {"const": "tt1720601", "first": 250, "after": None},
            ...     max_pages=10
            ... )
        """
        if max_pages is None:
            max_pages = int(os.getenv("IMDB_GRAPHQL_MAX_PAGES", "10"))

        all_edges: list[dict[str, Any]] = []
        page_count = 0

        while page_count < max_pages:
            page_count += 1
            response = self.execute_query(operation_name, sha256_hash, variables)

            # Extract edges and pageInfo using dot notation
            edges = self._get_nested(response, edges_path)
            page_info = self._get_nested(response, page_info_path)

            if not isinstance(edges, list):
                raise ImdbGraphQLError(
                    f"Expected list at {edges_path}, got {type(edges).__name__}",
                    response_data=response,
                )

            all_edges.extend(edges)

            # Check for next page
            if not page_info or not page_info.get("hasNextPage"):
                break

            # Update cursor for next page
            end_cursor = page_info.get("endCursor")
            if not end_cursor:
                break

            variables["after"] = end_cursor

        return all_edges

    @staticmethod
    def _get_nested(data: dict[str, Any], path: str) -> Any:
        """
        Extract nested value using dot notation path.

        Example:
            >>> _get_nested({"data": {"title": {"id": "tt1"}}}, "data.title.id")
            'tt1'
        """
        parts = path.split(".")
        current = data
        for part in parts:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current
