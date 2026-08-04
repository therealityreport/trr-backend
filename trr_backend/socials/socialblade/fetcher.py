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
import re
from typing import Any
from urllib.parse import quote, unquote, urlparse

import httpx
from bs4 import BeautifulSoup

from trr_backend.socials._scrapling_http_utils import (
    response_text as _response_text,
)
from trr_backend.socials._scrapling_http_utils import (
    safe_location as _safe_location,
)
from trr_backend.socials._scrapling_http_utils import (
    status_code as _status_code,
)
from trr_backend.socials.scrapling_transport import (
    build_stealthy_fetcher,
    merge_response_cookies,
    resolve_scrapling_fetcher_options,
    safe_cookie_metadata,
    safe_scrapling_proxy_metadata,
    scrapling_fetcher_metadata,
    scrapling_runtime_metadata,
)

from . import parser as socialblade_parser

logger = logging.getLogger("socials.socialblade.fetcher")
_SOCIALBLADE_HISTORY_LIMIT = 60
_TRPC_CAPTURE_PLATFORMS = frozenset({"instagram", "tiktok"})
_DATE_PREFIX_PATTERN = re.compile(r"^(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*)?\d{4}-\d{2}-\d{2}$")
_SOCIALBLADE_SCRAPLING_OPTION_KEYS = frozenset(
    {
        "additional_args",
        "ai_targeted",
        "allow_webgl",
        "block_ads",
        "block_webrtc",
        "blocked_domains",
        "dns_over_https",
        "google_search",
        "hide_canvas",
        "init_script",
        "real_chrome",
        "selector_config",
        "solve_cloudflare",
        "useragent",
        "wait_selector",
        "wait_selector_state",
    }
)


def _build_nav_headers(referer: str) -> dict[str, str]:
    return {
        "accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
        "user-agent": socialblade_parser.SOCIALBLADE_STEALTH_USER_AGENT,
    }


def _build_trpc_headers(referer: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
        "user-agent": socialblade_parser.SOCIALBLADE_STEALTH_USER_AGENT,
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
        proxy_config: Any | None = None,
        headless: bool = True,
        timeout_ms: int = 45_000,
    ) -> None:
        self._cookies = list(cookies or [])
        self._raw_cookies = dict(raw_cookies or {})
        self._seed_cookie_names = sorted(self._raw_cookies.keys())
        self._platform = str(platform or "instagram").strip().lower() or "instagram"
        self._proxy_config = proxy_config
        self._browser_proxy = proxy_config.browser_proxy if proxy_config else None
        self._api_proxy_url = proxy_config.api_proxy_url if proxy_config else None
        self._selected_proxy_fingerprint = proxy_config.fingerprint if proxy_config else "none"
        self._proxy_session_mode = proxy_config.session_mode if proxy_config else "none"
        self._headless = bool(headless)
        self._timeout_ms = max(5_000, int(timeout_ms))
        self._scrapling_runtime_metadata = scrapling_runtime_metadata()
        self._scrapling_fetcher_options = resolve_scrapling_fetcher_options(
            "SOCIALBLADE_SCRAPLING",
            allowed_keys=_SOCIALBLADE_SCRAPLING_OPTION_KEYS,
        )
        self._scrapling_fetcher_metadata = scrapling_fetcher_metadata(
            "StealthyFetcher",
            self._scrapling_fetcher_options.metadata,
            safe_scrapling_proxy_metadata(),
        )
        self._fetcher = build_stealthy_fetcher()
        self._http_client: httpx.AsyncClient | None = None
        self._request_count = 0
        self._warmup_cookie_delta: dict[str, str] = {}
        self._fallback_chain: list[str] = []
        self._last_transport = "scrapling_warmup"
        self._capture_source = "none"
        self._history_source_detail = "none"
        self._profile_source = "none"
        self._capture_control_updates: dict[str, str] = {}
        self._captured_xhr_count = 0
        self._captured_xhr_paths: list[str] = []

    @property
    def runtime_metadata(self) -> dict[str, Any]:
        cookie_metadata = safe_cookie_metadata(
            dict.fromkeys(self._seed_cookie_names, ""),
            self._warmup_cookie_delta,
            prefix="",
        )
        return {
            "scrapling_runtime": dict(self._scrapling_runtime_metadata),
            **self._scrapling_fetcher_metadata,
            "warmup_cookie_names": cookie_metadata["warmup_cookie_names"],
            "warmup_cookie_count": cookie_metadata["warmup_cookie_count"],
            "seed_cookie_names": cookie_metadata["seed_cookie_names"],
            "seed_cookie_count": cookie_metadata["seed_cookie_count"],
            "seed_has_socialblade_session": "session" in self._seed_cookie_names,
            "selected_proxy_fingerprint": self._selected_proxy_fingerprint,
            "proxy_session_mode": self._proxy_session_mode,
            "capture_source": self._capture_source,
            "history_source_detail": self._history_source_detail,
            "profile_source": self._profile_source,
            "captured_xhr_count": self._captured_xhr_count,
            "captured_xhr_paths": list(self._captured_xhr_paths),
            "request_count": self._request_count,
            "transport": self._last_transport,
            "fallback_chain": list(self._fallback_chain),
            "capture_control_updates": dict(self._capture_control_updates),
        }

    async def scrape(self, handle: str) -> dict[str, Any]:
        safe_handle = str(handle or "").strip().lstrip("@")
        if not safe_handle:
            raise RuntimeError("SocialBlade handle is required")

        sb_url = socialblade_parser._socialblade_profile_url(self._platform, safe_handle)
        response = await self._fetch_page(sb_url)
        html = _response_text(response)
        body_text = self._html_body_text(html)
        status_code = _status_code(response)
        if status_code in {401, 403} or socialblade_parser._page_access_denied(body_text):
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
            captured_xhr = self._response_captured_xhr(response)
            self._captured_xhr_count = len(captured_xhr)
            self._captured_xhr_paths = self._captured_xhr_safe_paths(captured_xhr)
            xhr_payload = self._extract_captured_platform_payload_from_xhr(captured_xhr, platform=self._platform)
            captured_score = self._captured_payload_chart_score(captured_payload)
            if captured_score > 0:
                self._capture_source = "html_script"
            xhr_score = self._captured_payload_chart_score(xhr_payload)
            if xhr_score > captured_score:
                captured_payload = xhr_payload
                captured_score = xhr_score
                self._capture_source = "scrapling_xhr"
            captured_user = captured_payload.get("user") if isinstance(captured_payload, dict) else None
            captured_control_updates = (
                captured_payload.get("control_updates") if isinstance(captured_payload, dict) else None
            )
            if isinstance(captured_control_updates, dict):
                self._capture_control_updates = {
                    str(key): str(value) for key, value in captured_control_updates.items() if value is not None
                }
            captured_history_rows = captured_payload.get("history_rows") if isinstance(captured_payload, dict) else None
            captured_daily_deltas = captured_payload.get("daily_deltas") if isinstance(captured_payload, dict) else None
            captured_daily_total_rows = (
                captured_payload.get("daily_total_rows") if isinstance(captured_payload, dict) else None
            )
            captured_payload_available = isinstance(captured_user, dict) and isinstance(captured_history_rows, list)

            captured_history_source = (
                "page_trpc_capture"
                if captured_payload_available and captured_score >= _SOCIALBLADE_HISTORY_LIMIT
                else "page_trpc_capture_short"
            )
            captured_fallback_step = f"{self._platform}_{captured_history_source}"

            def apply_captured_payload() -> None:
                nonlocal stats, rankings, metrics, chart_data, history_source
                stats, rankings = socialblade_parser._build_profile_stats_from_user_payload(
                    captured_user,
                    platform=self._platform,
                )
                metrics = socialblade_parser._history_rows_to_metrics(
                    captured_history_rows,
                    limit=len(captured_history_rows),
                    platform=self._platform,
                )
                table_chart = socialblade_parser._followers_chart_from_table(metrics, metric_label="Followers")
                if isinstance(captured_daily_total_rows, list):
                    chart_data = socialblade_parser._merge_followers_charts(
                        socialblade_parser._build_total_followers_chart_from_total_rows(captured_daily_total_rows),
                        table_chart,
                    )
                elif isinstance(captured_daily_deltas, list):
                    chart_data = socialblade_parser._merge_followers_charts(
                        socialblade_parser._build_total_followers_chart_from_daily_deltas(
                            stats["followers"],
                            captured_daily_deltas,
                        ),
                        table_chart,
                    )
                else:
                    chart_data = table_chart
                history_source = captured_history_source
                capture_source = self._capture_source if self._capture_source != "none" else "html_script"
                self._history_source_detail = capture_source
                self._profile_source = capture_source
                self._last_transport = f"scrapling_{captured_history_source}"
                if captured_fallback_step not in self._fallback_chain:
                    self._fallback_chain.append(captured_fallback_step)

            if captured_payload_available and captured_score >= _SOCIALBLADE_HISTORY_LIMIT:
                apply_captured_payload()
            else:
                if captured_payload_available:
                    self._fallback_chain.append(f"{self._platform}_page_trpc_capture_short")
                try:
                    stats, rankings, metrics, chart_data = await self._scrape_authenticated_api(safe_handle, sb_url)
                    history_source = "authenticated_api"
                    self._history_source_detail = "authenticated_api"
                    self._profile_source = "authenticated_api"
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
            stats, rankings, profile_labels = socialblade_parser._extract_profile_stats_from_body_text(
                body_text,
                self._platform,
            )
            if stats or rankings:
                self._profile_source = "html_body_fallback"
        if not profile_labels:
            profile_labels = socialblade_parser._default_profile_stat_labels(self._platform)

        if not metrics:
            table_data = self._extract_table_data(html)
            metrics = socialblade_parser._normalize_table_data(table_data, body_text)
            if metrics.get("row_count"):
                if history_source == "unavailable":
                    history_source = "table_fallback"
                self._history_source_detail = "html_table_fallback"
                self._fallback_chain.append("html_table_fallback")
                self._last_transport = "html_table_fallback"

        if not chart_data:
            chart_data = socialblade_parser._followers_chart_from_table(
                metrics,
                metric_label=str(profile_labels.get("chart_metric_label") or "Followers"),
            )
            if chart_data:
                history_source = "table_fallback"
                if self._history_source_detail == "none":
                    self._history_source_detail = "html_table_fallback"

        stats_refreshed = bool(stats.get("followers", 0) > 0 and int(metrics.get("row_count") or 0) > 0)
        if not stats_refreshed:
            raise RuntimeError(
                socialblade_parser._format_scrape_failure_message(authenticated_api_error)
            ) from authenticated_api_error

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
        merged_cookies = merge_response_cookies(self._raw_cookies, response)
        self._warmup_cookie_delta = {
            name: value for name, value in merged_cookies.items() if self._raw_cookies.get(name) != value
        }
        self._raw_cookies = merged_cookies

    def _rebuild_http_client(self) -> None:
        self._http_client = httpx.AsyncClient(
            base_url="https://socialblade.com",
            cookies=dict(self._raw_cookies),
            timeout=httpx.Timeout(self._timeout_ms / 1000),
            proxy=self._api_proxy_url,
            follow_redirects=False,
            trust_env=False,
            headers={"user-agent": socialblade_parser.SOCIALBLADE_STEALTH_USER_AGENT},
        )

    async def _fetch_page(self, url: str) -> Any:
        self._request_count += 1
        return await self._fetcher.async_fetch(
            url,
            **self._scrapling_fetcher_options.kwargs,
            headless=self._headless,
            network_idle=False,
            load_dom=self._platform in _TRPC_CAPTURE_PLATFORMS,
            disable_resources=False,
            cookies=self._cookies,
            proxy=self._browser_proxy,
            extra_headers=_build_nav_headers(url),
            page_action=self._capture_platform_page_trpc if self._platform in _TRPC_CAPTURE_PLATFORMS else None,
            capture_xhr=r"/api/trpc/",
            wait=2_000 if self._platform in _TRPC_CAPTURE_PLATFORMS else 0,
            timeout=self._timeout_ms,
            retries=1,
            retry_delay=1.0,
        )

    async def _capture_platform_page_trpc(self, page: Any) -> None:
        await page.evaluate(
            """async ({ platform, chartLimit }) => {
                const captureId = "trr-socialblade-capture";
                const platformKey = String(platform || "").toLowerCase();
                document.getElementById(captureId)?.remove();
                const capture = { user: null, responses: {} };
                const appendCapture = () => {
                    const element = document.createElement("script");
                    element.id = captureId;
                    element.type = "application/json";
                    element.textContent = JSON.stringify(capture);
                    document.body.appendChild(element);
                };
                const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
                const normalizeText = value => String(value || "").replace(/\\s+/g, " ").trim();
                const clickListboxOption = async targetOption => {
                    const target = normalizeText(targetOption).toLowerCase();
                    const buttons = Array.from(document.querySelectorAll('button[id*="headlessui-listbox-button"]'));
                    for (const button of buttons) {
                        if (normalizeText(button.textContent).toLowerCase() === target) {
                            return "already_selected";
                        }
                        button.click();
                        for (let attempt = 0; attempt < 20; attempt += 1) {
                            const options = Array.from(
                                document.querySelectorAll('[role="option"], li[id*="headlessui-listbox-option"]')
                            );
                            const option = options.find(
                                item => normalizeText(item.textContent).toLowerCase() === target
                            );
                            if (option) {
                                if (option.getAttribute("data-disabled") !== null) {
                                    document.dispatchEvent(
                                        new KeyboardEvent("keydown", { key: "Escape", bubbles: true })
                                    );
                                    return "disabled";
                                }
                                option.click();
                                await sleep(1000);
                                return "selected";
                            }
                            await sleep(100);
                        }
                        document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
                    }
                    return "missing";
                };
                const configureChartControls = async () => {
                    const updates = {};
                    updates.last60Days = await clickListboxOption("Last 60 Days");
                    updates.daily = await clickListboxOption("Daily");
                    updates.total = await clickListboxOption("Total");
                    capture.control_updates = updates;
                };
                const readNextData = () => {
                    const nextDataElement = document.querySelector("#__NEXT_DATA__");
                    if (!nextDataElement?.textContent) {
                        return null;
                    }
                    return JSON.parse(nextDataElement.textContent);
                };
                const parseInputJson = rawInput => {
                    if (!rawInput) {
                        return null;
                    }
                    const candidates = [rawInput];
                    try {
                        candidates.push(decodeURIComponent(rawInput));
                    } catch {
                    }
                    for (const candidate of candidates) {
                        try {
                            return JSON.parse(candidate);
                        } catch {
                        }
                    }
                    return null;
                };
                const idFromTrpcUrl = rawUrl => {
                    try {
                        const parsed = new URL(rawUrl, window.location.origin);
                        if (!parsed.pathname.includes(`/api/trpc/${platformKey}.`)) {
                            return null;
                        }
                        const input = parseInputJson(parsed.searchParams.get("input"));
                        const entries = Array.isArray(input) ? input : Object.values(input || {});
                        for (const entry of entries) {
                            const id = entry?.json?.id;
                            if (id) {
                                return id;
                            }
                        }
                    } catch {
                    }
                    return null;
                };
                const idFromResourceEntries = () => {
                    const resourceUrls = (performance.getEntriesByType("resource") || []).map(entry => entry.name);
                    for (const resourceUrl of resourceUrls) {
                        const id = idFromTrpcUrl(resourceUrl);
                        if (id) {
                            return id;
                        }
                    }
                    return null;
                };
                let nextData = null;
                for (let attempt = 0; attempt < 20; attempt += 1) {
                    try {
                        nextData = readNextData();
                    } catch (error) {
                        capture.error = `next_data_parse_failed:${String(error?.message || error)}`;
                        appendCapture();
                        return;
                    }
                    if (nextData) {
                        break;
                    }
                    await sleep(500);
                }
                const queries = nextData?.props?.pageProps?.trpcState?.json?.queries || [];
                const user = queries.find(
                    query => query?.queryKey?.[0]?.[0] === platformKey && query?.queryKey?.[0]?.[1] === "user"
                )?.state?.data;
                capture.user = user || null;
                const id = user?.id || idFromResourceEntries();
                if (!id) {
                    capture.error = nextData ? "user_id_missing" : "next_data_and_resource_id_missing";
                    appendCapture();
                    return;
                }
                await configureChartControls();
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
                for (const [key, endpoint] of Object.entries(endpoints)) {
                    try {
                        const response = await fetch(endpoint, {
                            headers: { accept: "application/json, text/plain, */*" },
                        });
                        capture.responses[key] = {
                            status: response.status,
                            text: await response.text(),
                        };
                    } catch (error) {
                        capture.responses[key] = {
                            status: 0,
                            text: "",
                            error: String(error?.message || error),
                        };
                    }
                }
                appendCapture();
            }""",
            {"platform": self._platform, "chartLimit": socialblade_parser._SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT},
        )

    async def _capture_instagram_page_trpc(self, page: Any) -> None:
        await self._capture_platform_page_trpc(page)

    async def _fetch_trpc_result(self, endpoint: str, *, referer: str, index: int | None = None) -> Any:
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
                    raise socialblade_parser.SocialBladeEndpointError(endpoint, status)
                await asyncio.sleep(self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
                continue

            payload = socialblade_parser._coerce_trpc_json(_response_text(response), endpoint=endpoint)
            if status != 200:
                raise socialblade_parser.SocialBladeEndpointError(endpoint, status)
            self._last_transport = "httpx_after_scrapling_warmup"
            return socialblade_parser._unwrap_trpc_result(payload, endpoint=endpoint, index=index)

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
        profile = await self._search_profile(handle, referer=referer)
        creator_id = str(profile.get("id") or "").strip()
        user_payload = await self._fetch_user(creator_id, referer=referer)
        stats, rankings = socialblade_parser._build_profile_stats_from_user_payload(
            user_payload,
            platform=self._platform,
        )
        history_rows = await self._fetch_history(creator_id, referer=referer, limit=60)
        metrics = socialblade_parser._history_rows_to_metrics(history_rows, limit=60, platform=self._platform)
        daily_total_rows = await self._fetch_daily_total_rows(
            creator_id,
            referer=referer,
            limit=socialblade_parser._SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT,
        )
        chart_data = socialblade_parser._merge_followers_charts(
            socialblade_parser._build_total_followers_chart_from_total_rows(daily_total_rows),
            socialblade_parser._followers_chart_from_table(
                metrics,
                metric_label=socialblade_parser._default_profile_stat_labels(self._platform)["chart_metric_label"],
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
            if headers[0].strip().lower() != "date":
                continue
            data: list[dict[str, str]] = []
            for row in rows[1:]:
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
                if len(cells) < len(headers):
                    continue
                if not _DATE_PREFIX_PATTERN.match(cells[0].strip()):
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
        captured_user = captured.get("user") if isinstance(captured.get("user"), dict) else None
        control_updates = captured.get("control_updates") if isinstance(captured.get("control_updates"), dict) else None
        return SocialBladeScraplingFetcher._extract_captured_platform_payload_from_responses(
            responses,
            platform=platform,
            captured_user=captured_user,
            control_updates=control_updates,
        )

    @staticmethod
    def _extract_captured_platform_payload_from_xhr(captured_xhr: list[Any], *, platform: str) -> dict[str, Any]:
        responses: dict[str, dict[str, Any]] = {}
        for xhr in captured_xhr:
            name = SocialBladeScraplingFetcher._captured_xhr_response_name(xhr, platform=platform)
            if not name:
                continue
            response_entry = {"status": _status_code(xhr), "text": _response_text(xhr)}
            if not response_entry["text"]:
                continue
            if name == "history60" and name in responses:
                current_rows = SocialBladeScraplingFetcher._history_count_from_capture_response(
                    responses[name],
                    platform=platform,
                )
                candidate_rows = SocialBladeScraplingFetcher._history_count_from_capture_response(
                    response_entry,
                    platform=platform,
                )
                if candidate_rows < current_rows:
                    continue
            responses[name] = response_entry
        return SocialBladeScraplingFetcher._extract_captured_platform_payload_from_responses(
            responses,
            platform=platform,
        )

    @staticmethod
    def _extract_captured_platform_payload_from_responses(
        responses: Any,
        *,
        platform: str,
        captured_user: dict[str, Any] | None = None,
        control_updates: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        del platform
        if not isinstance(responses, dict):
            return {}

        def unwrap_response(name: str, *, index: int | None = None) -> Any:
            response = responses.get(name)
            if not isinstance(response, dict) or int(response.get("status") or 0) != 200:
                return None
            raw_text = str(response.get("text") or "")
            if not raw_text:
                return None
            payload = socialblade_parser._coerce_trpc_json(raw_text, endpoint=f"captured:{name}")
            return socialblade_parser._unwrap_trpc_result(payload, endpoint=f"captured:{name}", index=index)

        user = unwrap_response("history60", index=0)
        if not isinstance(user, dict):
            user = unwrap_response("user")
        if not isinstance(user, dict):
            user = captured_user
        history_rows = unwrap_response("history60", index=1)
        daily_deltas = unwrap_response("dailyDeltas", index=0)
        daily_total_rows = unwrap_response("dailyTotalChart", index=0)
        return {
            "user": user,
            "history_rows": history_rows,
            "daily_deltas": daily_deltas,
            "daily_total_rows": daily_total_rows,
            "control_updates": control_updates or {},
        }

    @staticmethod
    def _history_count_from_capture_response(response_entry: dict[str, Any], *, platform: str) -> int:
        payload = SocialBladeScraplingFetcher._extract_captured_platform_payload_from_responses(
            {"history60": response_entry},
            platform=platform,
        )
        return SocialBladeScraplingFetcher._captured_payload_history_count(payload)

    @staticmethod
    def _captured_payload_history_count(payload: Any) -> int:
        if not isinstance(payload, dict):
            return 0
        rows = payload.get("history_rows")
        return len(rows) if isinstance(rows, list) else 0

    @staticmethod
    def _captured_payload_daily_total_count(payload: Any) -> int:
        if not isinstance(payload, dict):
            return 0
        rows = payload.get("daily_total_rows")
        return len(rows) if isinstance(rows, list) else 0

    @staticmethod
    def _captured_payload_chart_score(payload: Any) -> int:
        return max(
            SocialBladeScraplingFetcher._captured_payload_history_count(payload),
            SocialBladeScraplingFetcher._captured_payload_daily_total_count(payload),
        )

    @staticmethod
    def _response_captured_xhr(response: Any) -> list[Any]:
        captured_xhr = getattr(response, "captured_xhr", []) or []
        try:
            return list(captured_xhr)
        except TypeError:
            return []

    @staticmethod
    def _captured_xhr_safe_paths(captured_xhr: list[Any]) -> list[str]:
        paths: list[str] = []
        for xhr in captured_xhr[:20]:
            url = str(getattr(xhr, "url", "") or "")
            try:
                path = urlparse(url).path
            except Exception:  # noqa: BLE001
                continue
            if path and path not in paths:
                paths.append(path)
        return paths

    @staticmethod
    def _captured_xhr_response_name(xhr: Any, *, platform: str) -> str | None:
        url = str(getattr(xhr, "url", "") or "")
        if "/api/trpc/" not in url:
            return None
        try:
            parsed = urlparse(url)
        except Exception:  # noqa: BLE001
            return None
        endpoint = parsed.path.rsplit("/api/trpc/", 1)[-1]
        platform_key = str(platform or "").strip().lower()
        if endpoint == f"{platform_key}.user":
            return "user"
        if endpoint == f"{platform_key}.user,{platform_key}.history":
            return "history60"
        if endpoint == f"{platform_key}.monthly":
            query = unquote(parsed.query or "")
            compact_query = re.sub(r"\s+", "", query)
            if '"type":"total"' in compact_query:
                return "dailyTotalChart"
            if '"period":"daily"' in compact_query:
                return "dailyDeltas"
        return None

    @staticmethod
    def _now_iso() -> str:
        from datetime import UTC, datetime

        return datetime.now(tz=UTC).isoformat()
