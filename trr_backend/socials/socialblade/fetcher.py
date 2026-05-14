"""Scrapling-backed SocialBlade fetch flow.

Architecture:
  warmup()      ->  StealthyFetcher (Patchright browser) -> profile page
  trpc followup ->  httpx.AsyncClient                    -> Instagram tRPC
  html fallback ->  BeautifulSoup                        -> body/table parsing

The browser warmup establishes the Cloudflare-cleared session and bridges
cookies into plain HTTP follow-up requests. Visible-browser and credential
login recovery remain in ``scraper.py``; this module owns the default fetch
path only.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from trr_backend.socials._scrapling_http_utils import (
    extract_response_cookies as _extract_response_cookies,
)
from trr_backend.socials._scrapling_http_utils import (
    response_text as _response_text,
)
from trr_backend.socials._scrapling_http_utils import (
    safe_location as _safe_location,
)
from trr_backend.socials._scrapling_http_utils import (
    status_code as _status_code,
)

from .auth import SOCIALBLADE_STEALTH_USER_AGENT

logger = logging.getLogger("socials.socialblade.fetcher")
_SOCIALBLADE_HISTORY_LIMIT = 60
_TRPC_CAPTURE_PLATFORMS = frozenset({"instagram", "tiktok"})


def _build_nav_headers(referer: str) -> dict[str, str]:
    return {
        "accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
        "user-agent": SOCIALBLADE_STEALTH_USER_AGENT,
    }


def _build_trpc_headers(referer: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
        "user-agent": SOCIALBLADE_STEALTH_USER_AGENT,
        "x-requested-with": "XMLHttpRequest",
    }


class SocialBladeScraplingFetcher:
    _MAX_TRANSIENT_RETRIES = 2
    _BASE_BACKOFF_SECONDS = 1.0

    def __init__(
        self,
        *,
        cookies: list[dict[str, Any]],
        raw_cookies: dict[str, str],
        platform: str,
        headless: bool = True,
        timeout_ms: int = 45_000,
    ) -> None:
        try:
            from scrapling.fetchers import StealthyFetcher
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("Scrapling StealthyFetcher is unavailable. Install scrapling[fetchers].") from exc

        self._cookies = list(cookies or [])
        self._raw_cookies = dict(raw_cookies or {})
        self._platform = str(platform or "instagram").strip().lower() or "instagram"
        self._headless = bool(headless)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._fetcher = StealthyFetcher()
        self._http_client: httpx.AsyncClient | None = None
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._fallback_chain: list[str] = []
        self._last_transport = "scrapling_warmup"

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        return {
            "warmup_cookie_names": sorted(self._warmup_cookie_delta.keys()),
            "warmup_cookie_count": len(self._warmup_cookie_delta),
            "request_count": self._request_count,
            "transport": self._last_transport,
            "fallback_chain": list(self._fallback_chain),
        }

    async def scrape(self, handle: str) -> dict[str, Any]:
        from trr_backend.socials.socialblade.scraper import (
            _build_profile_stats_from_user_payload,
            _build_total_followers_chart_from_daily_deltas,
            _build_total_followers_chart_from_total_rows,
            _default_profile_stat_labels,
            _extract_profile_stats_from_body_text,
            _followers_chart_from_table,
            _format_scrape_failure_message,
            _history_rows_to_metrics,
            _merge_followers_charts,
            _normalize_table_data,
            _page_access_denied,
            _socialblade_profile_url,
        )

        safe_handle = str(handle or "").strip().lstrip("@")
        if not safe_handle:
            raise RuntimeError("SocialBlade handle is required")

        sb_url = _socialblade_profile_url(self._platform, safe_handle)
        response = await self._fetch_page(sb_url)
        html = _response_text(response)
        body_text = self._html_body_text(html)
        status_code = _status_code(response)
        if status_code in {401, 403} or _page_access_denied(body_text):
            raise RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)")

        self._merge_warmup_cookies(response)
        self._rebuild_http_client()
        self._fallback_chain = ["scrapling_warmup"]

        chart_data = None
        metrics = None
        stats = None
        rankings = None
        authenticated_api_error: Exception | None = None
        history_source = "unavailable"
        profile_labels: dict[str, str] = {}

        if self._platform in _TRPC_CAPTURE_PLATFORMS:
            captured_payload = self._extract_captured_platform_payload(html, platform=self._platform)
            captured_user = captured_payload.get("user") if isinstance(captured_payload, dict) else None
            captured_history_rows = captured_payload.get("history_rows") if isinstance(captured_payload, dict) else None
            captured_daily_deltas = captured_payload.get("daily_deltas") if isinstance(captured_payload, dict) else None
            captured_daily_total_rows = (
                captured_payload.get("daily_total_rows") if isinstance(captured_payload, dict) else None
            )
            captured_payload_available = isinstance(captured_user, dict) and isinstance(captured_history_rows, list)

            def apply_captured_payload() -> None:
                nonlocal stats, rankings, metrics, chart_data, history_source
                stats, rankings = _build_profile_stats_from_user_payload(captured_user, platform=self._platform)
                metrics = _history_rows_to_metrics(
                    captured_history_rows,
                    limit=len(captured_history_rows),
                    platform=self._platform,
                )
                table_chart = _followers_chart_from_table(metrics, metric_label="Followers")
                if isinstance(captured_daily_total_rows, list):
                    chart_data = _merge_followers_charts(
                        _build_total_followers_chart_from_total_rows(captured_daily_total_rows),
                        table_chart,
                    )
                elif isinstance(captured_daily_deltas, list):
                    chart_data = _merge_followers_charts(
                        _build_total_followers_chart_from_daily_deltas(
                            stats["followers"],
                            captured_daily_deltas,
                        ),
                        table_chart,
                    )
                else:
                    chart_data = table_chart
                history_source = "page_trpc_capture"
                self._last_transport = "scrapling_page_trpc_capture"
                self._fallback_chain.append(f"{self._platform}_page_trpc_capture")

            if captured_payload_available and len(captured_history_rows) >= _SOCIALBLADE_HISTORY_LIMIT:
                apply_captured_payload()
            else:
                if captured_payload_available:
                    self._fallback_chain.append(f"{self._platform}_page_trpc_capture_short")
                try:
                    stats, rankings, metrics, chart_data = await self._scrape_authenticated_api(safe_handle, sb_url)
                    history_source = "authenticated_api"
                    self._last_transport = "httpx_after_scrapling_warmup"
                    self._fallback_chain.append(f"{self._platform}_trpc_http")
                except Exception as exc:  # noqa: BLE001
                    authenticated_api_error = exc
                    logger.info(
                        "SocialBlade authenticated API scrape unavailable",
                        extra={"handle": safe_handle, "platform": self._platform},
                        exc_info=True,
                    )
                    if captured_payload_available:
                        apply_captured_payload()

        if not stats or not rankings:
            stats, rankings, profile_labels = _extract_profile_stats_from_body_text(body_text, self._platform)
        if not profile_labels:
            profile_labels = _default_profile_stat_labels(self._platform)

        if not metrics:
            table_data = self._extract_table_data(html)
            metrics = _normalize_table_data(table_data, body_text)
            if metrics.get("row_count"):
                if history_source == "unavailable":
                    history_source = "table_fallback"
                self._fallback_chain.append("html_table_fallback")
                self._last_transport = "html_table_fallback"

        if not chart_data:
            chart_data = _followers_chart_from_table(
                metrics,
                metric_label=str(profile_labels.get("chart_metric_label") or "Followers"),
            )
            if chart_data:
                history_source = "table_fallback"

        stats_refreshed = bool(stats.get("followers", 0) > 0 and int(metrics.get("row_count") or 0) > 0)
        if not stats_refreshed:
            raise RuntimeError(_format_scrape_failure_message(authenticated_api_error)) from authenticated_api_error

        return {
            "username": safe_handle,
            "account_handle": safe_handle,
            "platform": self._platform,
            "scraped_at": self._now_iso(),
            "stats_refreshed": stats_refreshed,
            "history_source": history_source,
            "profile_stats": stats,
            "profile_stats_labels": profile_labels,
            "rankings": rankings,
            "daily_channel_metrics_60day": metrics
            or {
                "period": "Last 14 Days",
                "row_count": 0,
                "headers": [],
                "data": [],
            },
            "daily_total_followers_chart": chart_data,
            "chart_metric_label": profile_labels.get("chart_metric_label"),
            "socialblade_url": sb_url,
            "runtime_metadata": self.runtime_metadata,
        }

    async def aclose(self) -> None:
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:  # noqa: BLE001
                pass
            self._http_client = None

    def _merge_warmup_cookies(self, response: Any) -> None:
        new_cookies = _extract_response_cookies(response)
        self._warmup_cookie_delta = dict(new_cookies)
        for name, value in new_cookies.items():
            self._raw_cookies[name] = value

    def _rebuild_http_client(self) -> None:
        self._http_client = httpx.AsyncClient(
            base_url="https://socialblade.com",
            cookies=dict(self._raw_cookies),
            timeout=httpx.Timeout(self._timeout_ms / 1000),
            follow_redirects=False,
            trust_env=False,
            headers={"user-agent": SOCIALBLADE_STEALTH_USER_AGENT},
        )

    async def _fetch_page(self, url: str) -> Any:
        self._request_count += 1
        return await self._fetcher.async_fetch(
            url,
            headless=self._headless,
            network_idle=False,
            load_dom=self._platform in _TRPC_CAPTURE_PLATFORMS,
            disable_resources=False,
            cookies=self._cookies,
            extra_headers=_build_nav_headers(url),
            page_action=self._capture_platform_page_trpc if self._platform in _TRPC_CAPTURE_PLATFORMS else None,
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    async def _capture_platform_page_trpc(self, page: Any) -> None:
        from trr_backend.socials.socialblade.scraper import _SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT

        await page.evaluate(
            """async ({ platform, chartLimit }) => {
                const captureId = "trr-socialblade-capture";
                const platformKey = String(platform || "").toLowerCase();
                document.getElementById(captureId)?.remove();
                const nextDataElement = document.querySelector("#__NEXT_DATA__");
                if (!nextDataElement?.textContent) {
                    return;
                }
                const nextData = JSON.parse(nextDataElement.textContent);
                const queries = nextData?.props?.pageProps?.trpcState?.json?.queries || [];
                const user = queries.find(
                    query => query?.queryKey?.[0]?.[0] === platformKey && query?.queryKey?.[0]?.[1] === "user"
                )?.state?.data;
                const id = user?.id;
                if (!id) {
                    return;
                }
                const encodeInput = value => encodeURIComponent(JSON.stringify(value));
                const endpoints = {
                    history60:
                        `/api/trpc/${platformKey}.user,${platformKey}.history?batch=1&input=` +
                        encodeInput({
                            "0": { json: { id } },
                            "1": { json: { id, limit: 60 } },
                        }),
                    dailyDeltas:
                        `/api/trpc/${platformKey}.monthly?batch=1&input=` +
                        encodeInput({
                            "0": { json: { id, period: "daily" } },
                        }),
                    dailyTotalChart:
                        `/api/trpc/${platformKey}.monthly?batch=1&input=` +
                        encodeInput({
                            "0": {
                                json: {
                                    id,
                                    limit: chartLimit,
                                    period: "daily",
                                    type: "total",
                                },
                            },
                        }),
                };
                const capture = { user, responses: {} };
                for (const [key, endpoint] of Object.entries(endpoints)) {
                    const response = await fetch(endpoint, {
                        headers: { accept: "application/json, text/plain, */*" },
                    });
                    capture.responses[key] = {
                        status: response.status,
                        text: await response.text(),
                    };
                }
                const element = document.createElement("script");
                element.id = captureId;
                element.type = "application/json";
                element.textContent = JSON.stringify(capture);
                document.body.appendChild(element);
            }""",
            {"platform": self._platform, "chartLimit": _SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT},
        )

    async def _capture_instagram_page_trpc(self, page: Any) -> None:
        await self._capture_platform_page_trpc(page)

    async def _fetch_trpc_result(self, endpoint: str, *, referer: str, index: int | None = None) -> Any:
        from trr_backend.socials.socialblade.scraper import (
            SocialBladeEndpointError,
            _coerce_trpc_json,
            _unwrap_trpc_result,
        )

        attempt = 0
        while True:
            attempt += 1
            try:
                response = await self._fetch_http(endpoint, referer=referer)
            except (TimeoutError, httpx.TimeoutException) as exc:
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    raise RuntimeError("SocialBlade tRPC request timed out") from exc
                await asyncio.sleep(self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue

            status = _status_code(response)
            if 300 <= status < 400:
                location = _safe_location(response)
                raise RuntimeError(f"SocialBlade redirected tRPC request to {location or '/'}")
            if status == 429 or 500 <= status < 600:
                if attempt > self._MAX_TRANSIENT_RETRIES:
                    raise SocialBladeEndpointError(endpoint, status)
                await asyncio.sleep(self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue

            payload = _coerce_trpc_json(_response_text(response), endpoint=endpoint)
            if status != 200:
                raise SocialBladeEndpointError(endpoint, status)
            self._last_transport = "httpx_after_scrapling_warmup"
            return _unwrap_trpc_result(payload, endpoint=endpoint, index=index)

    async def _fetch_http(self, endpoint: str, *, referer: str) -> httpx.Response:
        if self._http_client is None:
            self._rebuild_http_client()
        self._request_count += 1
        return await self._http_client.get(  # type: ignore[union-attr]
            endpoint,
            headers=_build_trpc_headers(referer),
        )

    async def _scrape_authenticated_api(
        self,
        handle: str,
        referer: str,
    ) -> tuple[dict[str, Any], dict[str, str], dict[str, Any], dict[str, Any] | None]:
        from trr_backend.socials.socialblade.scraper import (
            _SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT,
            _build_profile_stats_from_user_payload,
            _build_total_followers_chart_from_total_rows,
            _default_profile_stat_labels,
            _followers_chart_from_table,
            _history_rows_to_metrics,
            _merge_followers_charts,
        )

        profile = await self._search_profile(handle, referer=referer)
        creator_id = str(profile.get("id") or "").strip()
        user_payload = await self._fetch_user(creator_id, referer=referer)
        stats, rankings = _build_profile_stats_from_user_payload(user_payload, platform=self._platform)
        history_rows = await self._fetch_history(creator_id, referer=referer, limit=60)
        metrics = _history_rows_to_metrics(history_rows, limit=60, platform=self._platform)
        daily_total_rows = await self._fetch_daily_total_rows(
            creator_id,
            referer=referer,
            limit=_SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT,
        )
        chart_data = _merge_followers_charts(
            _build_total_followers_chart_from_total_rows(daily_total_rows),
            _followers_chart_from_table(
                metrics,
                metric_label=_default_profile_stat_labels(self._platform)["chart_metric_label"],
            ),
        )
        return stats, rankings, metrics, chart_data

    async def _search_profile(self, handle: str, *, referer: str) -> dict[str, Any]:
        endpoint = f"/api/trpc/{self._platform}.search?input=" + quote(
            json.dumps({"json": {"query": handle}}, separators=(",", ":"))
        )
        result = await self._fetch_trpc_result(endpoint, referer=referer)
        profile = result.get("platformResult") if isinstance(result, dict) else None
        if not isinstance(profile, dict) or not str(profile.get("id") or "").strip():
            raise RuntimeError(f"SocialBlade could not resolve {self._platform}/@{handle}")
        return profile

    async def _fetch_user(self, creator_id: str, *, referer: str) -> dict[str, Any]:
        endpoint = f"/api/trpc/{self._platform}.user?input=" + quote(
            json.dumps({"json": {"id": creator_id}}, separators=(",", ":"))
        )
        result = await self._fetch_trpc_result(endpoint, referer=referer)
        if not isinstance(result, dict):
            raise RuntimeError(f"SocialBlade {self._platform} user endpoint returned an unexpected payload")
        return result

    async def _fetch_history(self, creator_id: str, *, referer: str, limit: int) -> list[dict[str, Any]]:
        endpoint = f"/api/trpc/{self._platform}.user,{self._platform}.history?batch=1&input=" + quote(
            json.dumps(
                {
                    "0": {"json": {"id": creator_id}},
                    "1": {"json": {"id": creator_id, "limit": limit}},
                },
                separators=(",", ":"),
            )
        )
        result = await self._fetch_trpc_result(endpoint, referer=referer, index=1)
        if not isinstance(result, list):
            raise RuntimeError(f"SocialBlade {self._platform} history endpoint returned an unexpected payload")
        return result

    async def _fetch_period_deltas(
        self,
        creator_id: str,
        *,
        referer: str,
        period: str,
    ) -> list[dict[str, Any]]:
        endpoint = f"/api/trpc/{self._platform}.monthly?batch=1&input=" + quote(
            json.dumps({"0": {"json": {"id": creator_id, "period": period}}}, separators=(",", ":"))
        )
        result = await self._fetch_trpc_result(endpoint, referer=referer, index=0)
        if not isinstance(result, list):
            raise RuntimeError(f"SocialBlade {self._platform} {period} endpoint returned an unexpected payload")
        return result

    async def _fetch_daily_total_rows(
        self,
        creator_id: str,
        *,
        referer: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        endpoint = f"/api/trpc/{self._platform}.monthly?batch=1&input=" + quote(
            json.dumps(
                {
                    "0": {
                        "json": {
                            "id": creator_id,
                            "limit": limit,
                            "period": "daily",
                            "type": "total",
                        }
                    }
                },
                separators=(",", ":"),
            )
        )
        result = await self._fetch_trpc_result(endpoint, referer=referer, index=0)
        if not isinstance(result, list):
            raise RuntimeError(f"SocialBlade {self._platform} daily total endpoint returned an unexpected payload")
        return result

    @staticmethod
    def _html_body_text(html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text("\n", strip=True)

    @staticmethod
    def _extract_table_data(html: str) -> dict[str, Any] | None:
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if not rows:
                continue
            headers = [cell.get_text(" ", strip=True) for cell in rows[0].find_all(["th", "td"])]
            headers = [header for header in headers if header]
            if not headers:
                continue
            normalized_headers = [header.lower() for header in headers]
            if (
                len(normalized_headers) == 4
                and normalized_headers[0] == "date"
                and "following" in normalized_headers
                and {"followers", "subscribers", "likes"}.intersection(normalized_headers)
            ):
                metric_headers = [header.title() for header in headers]
                headers = [
                    "Date",
                    f"{metric_headers[1]} Delta",
                    f"{metric_headers[1]} Total",
                    f"{metric_headers[2]} Delta",
                    f"{metric_headers[2]} Total",
                    f"{metric_headers[3]} Delta",
                    f"{metric_headers[3]} Total",
                ]
            data: list[dict[str, str]] = []
            for row in rows[1:]:
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
                if len(cells) < len(headers):
                    continue
                data.append({headers[index]: cells[index] or "" for index in range(len(headers))})
            if data:
                return {"headers": headers, "data": data}
        return None

    @staticmethod
    def _extract_captured_instagram_payload(html: str) -> dict[str, Any]:
        return SocialBladeScraplingFetcher._extract_captured_platform_payload(html, platform="instagram")

    @staticmethod
    def _extract_captured_platform_payload(html: str, *, platform: str) -> dict[str, Any]:
        del platform
        if not html:
            return {}
        soup = BeautifulSoup(html, "html.parser")
        capture_node = soup.find("script", id="trr-socialblade-capture")
        if capture_node is None or not capture_node.string:
            return {}
        try:
            captured = json.loads(capture_node.string)
        except json.JSONDecodeError:
            return {}

        responses = captured.get("responses") if isinstance(captured, dict) else None
        if not isinstance(responses, dict):
            return {}

        from trr_backend.socials.socialblade.scraper import _coerce_trpc_json, _unwrap_trpc_result

        def unwrap_response(name: str, *, index: int | None = None) -> Any:
            response = responses.get(name)
            if not isinstance(response, dict) or int(response.get("status") or 0) != 200:
                return None
            raw_text = str(response.get("text") or "")
            if not raw_text:
                return None
            payload = _coerce_trpc_json(raw_text, endpoint=f"captured:{name}")
            return _unwrap_trpc_result(payload, endpoint=f"captured:{name}", index=index)

        user = unwrap_response("history60", index=0)
        if not isinstance(user, dict):
            user = captured.get("user") if isinstance(captured.get("user"), dict) else None
        history_rows = unwrap_response("history60", index=1)
        daily_deltas = unwrap_response("dailyDeltas", index=0)
        daily_total_rows = unwrap_response("dailyTotalChart", index=0)
        return {
            "user": user,
            "history_rows": history_rows,
            "daily_deltas": daily_deltas,
            "daily_total_rows": daily_total_rows,
        }

    @staticmethod
    def _now_iso() -> str:
        from datetime import UTC, datetime

        return datetime.now(tz=UTC).isoformat()
