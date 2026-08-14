from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from trr_backend.socials.instagram.permalink_metadata import fetch_permalink_metadata
from trr_backend.socials.instagram.scraper import InstagramScraper

PUBLIC_PROBE_VERSION = "2026-06-15.public-instagram-proof-v1"

COOKIE_ENV_VARS = (
    "SOCIAL_INSTAGRAM_COOKIES_JSON",
    "SOCIAL_INSTAGRAM_COOKIES_FILE",
    "INSTAGRAM_COOKIES_JSON",
    "INSTAGRAM_COOKIES_FILE",
)

DECODO_ENV_VARS = (
    "DECODO_USERNAME",
    "DECODO_PASSWORD",
    "DECODO_GATEWAY",
    "DECODO_PROXY_URL",
)

PROXY_ENV_VARS = (
    "SOCIAL_INSTAGRAM_PROXY_URLS",
    "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
)

PROXY_PROVIDER_ENV_VARS = (
    "SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER",
    "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER",
)

AUTH_ENV_VARS = (
    "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID",
    "SOCIAL_INSTAGRAM_CHROME_PROFILE",
)

BACKOFF_SECONDS_BY_STATUS = {
    401: (60 * 60, 4 * 60 * 60, 12 * 60 * 60, 24 * 60 * 60),
    403: (2 * 60 * 60, 6 * 60 * 60, 24 * 60 * 60),
    429: (2 * 60 * 60,),
}

BACKOFF_STOP_REASON_BY_STATUS = {
    401: "public_graphql_401_backoff_required",
    403: "public_graphql_403_backoff_required",
    429: "public_graphql_429_backoff_required",
}


class PublicModeEnvironmentError(RuntimeError):
    def __init__(self, violations: list[str]) -> None:
        self.violations = list(violations)
        super().__init__("Instagram public probe requires a clean public-only environment.")


@dataclass(slots=True)
class PublicProbeConfig:
    account: str
    until_date: date
    target_years: tuple[int, ...] = (2025, 2026)
    max_pages: int | None = None
    continue_after_boundary: bool = False
    sample_details_per_page: int = 2
    sample_comments_per_page: int = 1
    comments_mode: str = "sampled"
    details_mode: str = "sampled"
    state_file: Path | None = None
    output: Path | None = None
    resume: bool = False
    strict_public: bool = True
    fail_if_cookies: bool = True
    fail_if_decodo: bool = True
    retry_profile: str = "patient"
    delay_seconds: float = 0.15


@dataclass(slots=True)
class PublicProbePage:
    posts: list[dict[str, Any]]
    page_info: dict[str, Any]
    metadata: dict[str, Any]
    connection_found: bool


@dataclass(slots=True)
class PublicPostSummary:
    shortcode: str
    media_id: str | None = None
    taken_at: str | None = None
    taken_at_epoch: int | None = None
    comment_count: int | None = None
    like_count: int | None = None
    media_type: str | None = None
    page_index: int = 0


@dataclass(slots=True)
class PublicDetailStatus:
    shortcode: str
    attempted: bool
    succeeded: bool
    media_type: str | None = None
    media_url_count: int = 0
    thumbnail_present: bool = False
    taken_at: str | None = None
    caption_present: bool = False
    stop_reason: str | None = None
    source: str | None = None


@dataclass(slots=True)
class PublicCommentsStatus:
    shortcode: str
    attempted: bool
    classification: str
    advertised_count: int | None = None
    recovered_count: int = 0
    coverage_ratio: float | None = None
    terminal_reason: str | None = None
    fallback_source: str | None = None
    stop_reason: str | None = None


@dataclass(slots=True)
class PublicProbeResult:
    account: str
    auth_state: str = "public"
    proxy_state: str = "none"
    decodo_state: str = "none"
    historical_boundary: str = ""
    target_years: list[int] = field(default_factory=list)
    target_year_window_complete: bool = False
    target_posts_recovered: int = 0
    target_year_counts: dict[str, int] = field(default_factory=dict)
    continue_after_boundary: bool = False
    historical_boundary_reached: bool = False
    account_exhausted: bool = False
    pages_recovered: int = 0
    unique_posts_recovered: int = 0
    oldest_post_at: str | None = None
    post_details: dict[str, Any] = field(default_factory=dict)
    comments: dict[str, Any] = field(default_factory=dict)
    stop_reason: str = "unknown"
    next_resume_cursor: str | None = None
    next_retry_after_seconds: int | None = None
    requires_approval: bool = False
    state_file: str | None = None
    generated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    version: str = PUBLIC_PROBE_VERSION
    pages: list[dict[str, Any]] = field(default_factory=list)
    posts: list[dict[str, Any]] = field(default_factory=list)
    target_posts: list[dict[str, Any]] = field(default_factory=list)
    detail_statuses: list[dict[str, Any]] = field(default_factory=list)
    comment_statuses: list[dict[str, Any]] = field(default_factory=list)
    environment: dict[str, Any] = field(default_factory=dict)


def validate_public_environment(
    *,
    strict_public: bool = True,
    fail_if_cookies: bool = True,
    fail_if_decodo: bool = True,
) -> list[str]:
    violations: list[str] = []
    if fail_if_cookies:
        violations.extend(_set_env_violations(COOKIE_ENV_VARS))
    if fail_if_decodo:
        violations.extend(_set_env_violations(DECODO_ENV_VARS))
    if strict_public:
        violations.extend(_set_env_violations(PROXY_ENV_VARS))
        violations.extend(_set_env_violations(AUTH_ENV_VARS))
        for name in PROXY_PROVIDER_ENV_VARS:
            value = str(os.getenv(name) or "").strip().lower()
            if value and value not in {"none", "off", "false", "0"}:
                violations.append(f"{name}={value}")
    return violations


def run_public_probe(
    config: PublicProbeConfig,
    *,
    page_fetcher: Callable[[str, str | None], PublicProbePage] | None = None,
    detail_fetcher: Callable[[str], PublicDetailStatus] | None = None,
    comments_fetcher: Callable[[PublicPostSummary], PublicCommentsStatus] | None = None,
    clock: Callable[[], float] = time.time,
) -> PublicProbeResult:
    account = _normalize_account(config.account)
    if not account:
        raise ValueError("account is required")
    config = PublicProbeConfig(
        account=account,
        until_date=config.until_date,
        target_years=parse_target_years(config.target_years),
        max_pages=_normalize_max_pages(config.max_pages),
        continue_after_boundary=bool(config.continue_after_boundary),
        sample_details_per_page=max(0, int(config.sample_details_per_page or 0)),
        sample_comments_per_page=max(0, int(config.sample_comments_per_page or 0)),
        comments_mode=_normalize_mode(config.comments_mode),
        details_mode=_normalize_mode(config.details_mode),
        state_file=config.state_file,
        output=config.output,
        resume=bool(config.resume),
        strict_public=bool(config.strict_public),
        fail_if_cookies=bool(config.fail_if_cookies),
        fail_if_decodo=bool(config.fail_if_decodo),
        retry_profile=str(config.retry_profile or "patient").strip().lower() or "patient",
        delay_seconds=max(0.0, float(config.delay_seconds or 0.0)),
    )
    violations = validate_public_environment(
        strict_public=config.strict_public,
        fail_if_cookies=config.fail_if_cookies,
        fail_if_decodo=config.fail_if_decodo,
    )
    if violations:
        raise PublicModeEnvironmentError(violations)

    state = _load_state(config.state_file) if config.resume else {}
    if state and str(state.get("account") or "").strip().lower() not in {"", account}:
        raise ValueError(f"state file account does not match requested account: {state.get('account')!r}")
    state = _initial_state(config, state=state)
    fetch_page = page_fetcher or _default_page_fetcher(config)
    fetch_detail = detail_fetcher or _default_detail_fetcher
    fetch_comments = comments_fetcher or _default_comments_fetcher

    terminal_stop_reason: str | None = None
    next_retry_after_seconds: int | None = None
    cursor = str(state.get("cursor") or "").strip() or None
    seen_cursors = {str(value).strip() for value in state.get("seen_cursors", []) if str(value).strip()}
    seen_shortcodes = {str(value).strip() for value in state.get("unique_shortcodes", []) if str(value).strip()}
    posts_by_shortcode: dict[str, PublicPostSummary] = {
        str(row.get("shortcode") or "").strip(): _summary_from_state(row)
        for row in state.get("posts", [])
        if isinstance(row, dict) and str(row.get("shortcode") or "").strip()
    }
    detail_statuses: dict[str, PublicDetailStatus] = {
        str(row.get("shortcode") or "").strip(): _detail_status_from_state(row)
        for row in state.get("detail_statuses", [])
        if isinstance(row, dict) and str(row.get("shortcode") or "").strip()
    }
    comment_statuses: dict[str, PublicCommentsStatus] = {
        str(row.get("shortcode") or "").strip(): _comments_status_from_state(row)
        for row in state.get("comment_statuses", [])
        if isinstance(row, dict) and str(row.get("shortcode") or "").strip()
    }

    page_index = max(0, int(state.get("pages_recovered") or 0)) + 1
    while True:
        page = fetch_page(account, cursor)
        page_posts = [_post_summary_from_node(node, page_index=page_index) for node in page.posts]
        page_posts = [post for post in page_posts if post.shortcode]
        metadata = dict(page.metadata or {})
        page_info = dict(page.page_info or {})
        next_cursor = str(page_info.get("end_cursor") or "").strip() or None
        has_next_page = bool(page_info.get("has_next_page"))
        page_summary = {
            "page_index": page_index,
            "cursor_in": cursor,
            "cursor_out": next_cursor,
            "post_count": len(page_posts),
            "unique_post_count_before": len(seen_shortcodes),
            "has_next_page": has_next_page,
            "connection_found": bool(page.connection_found),
            "doc_id_used": metadata.get("doc_id_used") or metadata.get("profile_posts_doc_id"),
            "doc_ids_attempted": _doc_ids_attempted(metadata),
            "transport": metadata.get("retrieval_transport") or metadata.get("transport") or "requests_enriched",
            "status_or_error": _status_or_error(metadata),
        }
        state.setdefault("pages", []).append(page_summary)

        if not page_posts and not page.connection_found:
            terminal_stop_reason = _stop_reason_from_metadata(metadata, after_progress=bool(seen_shortcodes))
            next_retry_after_seconds = _backoff_seconds_for_stop_reason(terminal_stop_reason, state=state)
            _update_terminal_state(
                state,
                stop_reason=terminal_stop_reason,
                cursor=cursor,
                retry_after_seconds=next_retry_after_seconds,
                completed=False,
            )
            _save_state(config.state_file, state)
            break

        for post in page_posts:
            if post.shortcode not in seen_shortcodes:
                seen_shortcodes.add(post.shortcode)
                posts_by_shortcode[post.shortcode] = post
                state.setdefault("posts", []).append(asdict(post))
            else:
                existing = posts_by_shortcode.get(post.shortcode)
                posts_by_shortcode[post.shortcode] = _merge_post_summary(existing, post)
        state["unique_shortcodes"] = sorted(seen_shortcodes)
        state["pages_recovered"] = page_index
        state["oldest_post_at"] = _oldest_post_at(posts_by_shortcode.values())
        page_summary["unique_post_count_after"] = len(seen_shortcodes)
        page_summary["oldest_post_at"] = state["oldest_post_at"]

        _collect_details(config, page_posts, detail_statuses, fetch_detail)
        _collect_comments(config, page_posts, comment_statuses, fetch_comments)
        state["detail_statuses"] = [asdict(status) for status in detail_statuses.values()]
        state["comment_statuses"] = [asdict(status) for status in comment_statuses.values()]
        state["cursor"] = next_cursor
        _save_state(config.state_file, state)

        if _boundary_reached(posts_by_shortcode.values(), config.until_date):
            state["historical_boundary_reached"] = True
            if not config.continue_after_boundary:
                terminal_stop_reason = "historical_boundary_reached"
                _update_terminal_state(state, stop_reason=terminal_stop_reason, cursor=next_cursor, completed=True)
                _save_state(config.state_file, state)
                break
        if not has_next_page or not next_cursor:
            terminal_stop_reason = "account_exhausted"
            _update_terminal_state(state, stop_reason=terminal_stop_reason, cursor=None, completed=True)
            _save_state(config.state_file, state)
            break
        if next_cursor in seen_cursors or next_cursor == cursor:
            terminal_stop_reason = "public_repeated_cursor_manual_review"
            _update_terminal_state(state, stop_reason=terminal_stop_reason, cursor=next_cursor, completed=False)
            _save_state(config.state_file, state)
            break
        seen_cursors.add(next_cursor)
        state["seen_cursors"] = sorted(seen_cursors)
        cursor = next_cursor
        if config.delay_seconds > 0:
            time.sleep(config.delay_seconds)
        page_index += 1

    if terminal_stop_reason is None:
        terminal_stop_reason = "unknown_public_probe_stop"
        _update_terminal_state(state, stop_reason=terminal_stop_reason, cursor=cursor, completed=False)
        _save_state(config.state_file, state)

    detail_values = list(detail_statuses.values())
    comment_values = list(comment_statuses.values())
    if terminal_stop_reason in {"historical_boundary_reached", "account_exhausted"}:
        if any(status.attempted and not status.succeeded for status in detail_values):
            terminal_stop_reason = "details_partial"
        elif any(status.attempted and status.classification != "public_complete" for status in comment_values):
            terminal_stop_reason = "comments_partial"
    terminal_stop_reason = terminal_stop_reason or str(state.get("stop_reason") or "unknown")
    requires_approval = terminal_stop_reason == "requires_approval_for_auth_or_proxy"
    oldest_post_at = _oldest_post_at(posts_by_shortcode.values())
    target_years = list(config.target_years)
    target_posts = _target_posts(posts_by_shortcode.values(), target_years=target_years)
    boundary_reached = _boundary_reached(posts_by_shortcode.values(), config.until_date)
    result = PublicProbeResult(
        account=account,
        historical_boundary=config.until_date.isoformat(),
        target_years=target_years,
        target_year_window_complete=boundary_reached or terminal_stop_reason == "account_exhausted",
        target_posts_recovered=len(target_posts),
        target_year_counts=_target_year_counts(target_posts, target_years=target_years),
        continue_after_boundary=config.continue_after_boundary,
        historical_boundary_reached=boundary_reached,
        account_exhausted=terminal_stop_reason == "account_exhausted",
        pages_recovered=max(0, int(state.get("pages_recovered") or 0)),
        unique_posts_recovered=len(seen_shortcodes),
        oldest_post_at=oldest_post_at,
        post_details=_detail_rollup(detail_values),
        comments=_comments_rollup(comment_values),
        stop_reason=terminal_stop_reason,
        next_resume_cursor=str(state.get("cursor") or "").strip() or None,
        next_retry_after_seconds=next_retry_after_seconds,
        requires_approval=requires_approval,
        state_file=str(config.state_file) if config.state_file else None,
        posts=[asdict(post) for post in posts_by_shortcode.values()],
        target_posts=[asdict(post) for post in target_posts],
        detail_statuses=[asdict(status) for status in detail_values],
        comment_statuses=[asdict(status) for status in comment_values],
        pages=list(state.get("pages", [])),
        environment={
            "strict_public": config.strict_public,
            "fail_if_cookies": config.fail_if_cookies,
            "fail_if_decodo": config.fail_if_decodo,
            "retry_profile": config.retry_profile,
            "page_cap": config.max_pages,
            "continue_after_boundary": config.continue_after_boundary,
            "validated_at_epoch": int(clock()),
        },
    )
    if config.output:
        _write_json(config.output, asdict(result))
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe Instagram public-only account scraping.")
    parser.add_argument("--account", required=True, help="Instagram account handle, e.g. bravotv")
    parser.add_argument("--until-date", required=True, help="Historical boundary date, e.g. 2025-01-01")
    parser.add_argument(
        "--target-years",
        default="2025,2026",
        help="Comma-delimited post years to count as the target public scrape window.",
    )
    parser.add_argument("--max-pages", type=int, default=0, help="0 means no page cap.")
    parser.add_argument(
        "--continue-after-boundary",
        action="store_true",
        help="Keep scraping after the historical boundary is reached.",
    )
    parser.add_argument("--sample-details-per-page", type=int, default=2)
    parser.add_argument("--sample-comments-per-page", type=int, default=1)
    parser.add_argument("--comments-mode", choices=("sampled", "all"), default="sampled")
    parser.add_argument("--details-mode", choices=("sampled", "all"), default="sampled")
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--strict-public", action="store_true", default=False)
    parser.add_argument("--fail-if-cookies", action="store_true", default=False)
    parser.add_argument("--fail-if-decodo", action="store_true", default=False)
    parser.add_argument("--retry-profile", choices=("aggressive", "patient"), default="patient")
    parser.add_argument("--delay-seconds", type=float, default=0.15)
    return parser


def config_from_args(args: argparse.Namespace) -> PublicProbeConfig:
    return PublicProbeConfig(
        account=args.account,
        until_date=date.fromisoformat(str(args.until_date)),
        target_years=parse_target_years(args.target_years),
        max_pages=args.max_pages,
        continue_after_boundary=args.continue_after_boundary,
        sample_details_per_page=args.sample_details_per_page,
        sample_comments_per_page=args.sample_comments_per_page,
        comments_mode=args.comments_mode,
        details_mode=args.details_mode,
        state_file=args.state_file,
        output=args.output,
        resume=args.resume,
        strict_public=args.strict_public,
        fail_if_cookies=args.fail_if_cookies,
        fail_if_decodo=args.fail_if_decodo,
        retry_profile=args.retry_profile,
        delay_seconds=args.delay_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        result = run_public_probe(config_from_args(args))
    except PublicModeEnvironmentError as exc:
        payload = {
            "auth_state": "public",
            "proxy_state": "none",
            "decodo_state": "none",
            "stop_reason": "requires_approval_for_auth_or_proxy",
            "requires_approval": True,
            "environment_violations": exc.violations,
            "generated_at": datetime.now(UTC).isoformat(),
            "version": PUBLIC_PROBE_VERSION,
        }
        if args.output:
            _write_json(args.output, payload)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 2
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.stop_reason in {"historical_boundary_reached", "account_exhausted"} else 1


def _default_page_fetcher(config: PublicProbeConfig) -> Callable[[str, str | None], PublicProbePage]:
    scraper = InstagramScraper(cookies={}, browser_account_id=None, attach_auth_session=False)

    def fetch_page(account: str, cursor: str | None) -> PublicProbePage:
        payload = scraper.fetch_posts_graphql(
            account,
            cursor=cursor,
            delay=config.delay_seconds,
            allow_browser_fallback=False,
            allow_recovery=False,
        )
        posts, page_info, connection_found = _posts_from_public_graphql_payload(payload)
        return PublicProbePage(
            posts=posts,
            page_info=page_info,
            metadata=_public_scraper_runtime_metadata(scraper),
            connection_found=connection_found,
        )

    return fetch_page


def _default_detail_fetcher(shortcode: str) -> PublicDetailStatus:
    try:
        metadata = fetch_permalink_metadata(shortcode, cookies={})
    except Exception as exc:  # noqa: BLE001
        return PublicDetailStatus(
            shortcode=shortcode,
            attempted=True,
            succeeded=False,
            stop_reason=f"{exc.__class__.__name__}: {exc}",
        )
    if metadata is None:
        return PublicDetailStatus(
            shortcode=shortcode,
            attempted=True,
            succeeded=False,
            stop_reason="metadata_not_found",
        )
    return PublicDetailStatus(
        shortcode=shortcode,
        attempted=True,
        succeeded=True,
        media_type=metadata.media_type,
        media_url_count=len(metadata.media_urls or []),
        thumbnail_present=bool(metadata.thumbnail_url),
        taken_at=metadata.taken_at.isoformat() if metadata.taken_at else None,
        caption_present=bool(str((metadata.raw_media or {}).get("caption") or "").strip()),
        source="public_permalink_metadata",
    )


def _default_comments_fetcher(post: PublicPostSummary) -> PublicCommentsStatus:
    async def fetch() -> PublicCommentsStatus:
        from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher

        fetcher = InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={},
            browser_account_id=None,
            proxy_config=None,
            timeout_ms=45_000,
        )
        fetcher._parser = InstagramScraper(cookies={}, browser_account_id=None, attach_auth_session=False)  # noqa: SLF001
        result = await fetcher.fetch_comments_for_shortcode(
            post.shortcode,
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=post.comment_count,
            load_strategy="public_relay",
        )
        diagnostic = getattr(result, "diagnostic_metadata", {}) or {}
        is_diag_dict = isinstance(diagnostic, dict)
        public_comments_raw = diagnostic.get("public_comments") if is_diag_dict else {}
        relay_comments_raw = diagnostic.get("relay_comments") if is_diag_dict else {}
        public_comments = public_comments_raw if isinstance(public_comments_raw, dict) else {}
        relay_comments = relay_comments_raw if isinstance(relay_comments_raw, dict) else {}
        classification = str(public_comments.get("classification") or "public_blocked")
        recovered = _safe_int(
            public_comments.get("recovered_count"), default=_comment_tree_count(getattr(result, "comments", []))
        )
        return PublicCommentsStatus(
            shortcode=post.shortcode,
            attempted=True,
            classification=classification,
            advertised_count=_optional_int(public_comments.get("advertised_count")),
            recovered_count=recovered,
            coverage_ratio=_optional_float(public_comments.get("coverage_ratio")),
            terminal_reason=str(
                public_comments.get("terminal_reason") or getattr(result, "fetch_reason", "") or ""
            ).strip()
            or None,
            fallback_source=str(relay_comments.get("fallback_source") or "").strip() or None,
            stop_reason=str(getattr(result, "fetch_reason", "") or "").strip() or None,
        )

    return asyncio.run(fetch())


def _set_env_violations(names: tuple[str, ...]) -> list[str]:
    violations = []
    for name in names:
        value = str(os.getenv(name) or "").strip()
        if value:
            violations.append(name)
    return violations


def _normalize_account(value: Any) -> str:
    return str(value or "").strip().lower().lstrip("@")


def _normalize_mode(value: Any) -> str:
    normalized = str(value or "sampled").strip().lower()
    return normalized if normalized in {"sampled", "all"} else "sampled"


def _normalize_max_pages(value: Any) -> int | None:
    return None


def parse_target_years(value: Any) -> tuple[int, ...]:
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = str(value or "").replace(";", ",").split(",")
    parsed_years: set[int] = set()
    for raw in raw_values:
        text = str(raw).strip()
        if not text:
            continue
        try:
            parsed_years.add(int(text))
        except ValueError:
            continue
    years = sorted(parsed_years)
    if not years:
        return (2025, 2026)
    return tuple(year for year in years if 2000 <= year <= 2100) or (2025, 2026)


def _initial_state(config: PublicProbeConfig, *, state: dict[str, Any]) -> dict[str, Any]:
    return {
        "account": config.account,
        "auth_state": "public",
        "proxy_state": "none",
        "decodo_state": "none",
        "historical_boundary": config.until_date.isoformat(),
        "cursor": str(state.get("cursor") or "").strip() or None,
        "seen_cursors": list(state.get("seen_cursors") or []),
        "unique_shortcodes": list(state.get("unique_shortcodes") or []),
        "pages_recovered": max(0, int(state.get("pages_recovered") or 0)),
        "oldest_post_at": state.get("oldest_post_at"),
        "posts": list(state.get("posts") or []),
        "pages": list(state.get("pages") or []),
        "detail_statuses": list(state.get("detail_statuses") or []),
        "comment_statuses": list(state.get("comment_statuses") or []),
        "backoff_attempts": dict(state.get("backoff_attempts") or {}),
        "stop_reason": state.get("stop_reason"),
        "version": PUBLIC_PROBE_VERSION,
    }


def _load_state(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def _save_state(path: Path | None, state: Mapping[str, Any]) -> None:
    if path is None:
        return
    _write_json(path, dict(state))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        tmp_name = handle.name
    os.replace(tmp_name, path)


def _posts_from_public_graphql_payload(payload: Any) -> tuple[list[dict[str, Any]], dict[str, Any], bool]:
    connection = {}
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            candidate = data.get("xdt_api__v1__feed__user_timeline_graphql_connection")
            if isinstance(candidate, dict):
                connection = candidate
    edges = connection.get("edges") if isinstance(connection, dict) else []
    posts = [
        node
        for edge in (edges or [])
        if isinstance(edge, dict)
        for node in [edge.get("node")]
        if isinstance(node, dict) and node
    ]
    page_info = connection.get("page_info") if isinstance(connection.get("page_info"), dict) else {}
    return posts, dict(page_info or {}), bool(connection)


def _public_scraper_runtime_metadata(scraper: Any | None = None) -> dict[str, Any]:
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {}) if scraper is not None else {}
    doc_ids_attempted = [
        str(value).strip()
        for value in (retrieval_meta.get("profile_posts_doc_ids_attempted") or [])
        if str(value).strip()
    ]
    doc_id_used = str(retrieval_meta.get("doc_id_used") or retrieval_meta.get("profile_posts_doc_id") or "").strip()
    return {
        "scrape_mode": "public_first",
        "auth_state": "public",
        "proxy_state": "none",
        "selected_proxy_fingerprint": "none",
        "transport": retrieval_meta.get("retrieval_transport") or retrieval_meta.get("transport"),
        "retrieval_transport": retrieval_meta.get("retrieval_transport") or retrieval_meta.get("transport"),
        "doc_id_used": doc_id_used or None,
        "profile_posts_doc_id": doc_id_used or None,
        "profile_posts_doc_ids_attempted": doc_ids_attempted,
        "doc_ids_attempted": doc_ids_attempted,
        "retrieval_meta": retrieval_meta,
    }


def _post_summary_from_node(node: dict[str, Any], *, page_index: int) -> PublicPostSummary:
    shortcode = _first_str(node, "code", "shortcode", "shortCode")
    taken_at_epoch = _first_int(node, "taken_at", "taken_at_timestamp", "timestamp")
    return PublicPostSummary(
        shortcode=shortcode,
        media_id=_first_str(node, "pk", "id", "media_id") or None,
        taken_at=_iso_from_epoch(taken_at_epoch),
        taken_at_epoch=taken_at_epoch,
        comment_count=_first_int(
            node,
            "comment_count",
            "comments_count",
            "edge_media_to_comment.count",
        ),
        like_count=_first_int(node, "like_count", "edge_liked_by.count", "edge_media_preview_like.count"),
        media_type=_media_type_from_node(node),
        page_index=page_index,
    )


def _summary_from_state(row: Mapping[str, Any]) -> PublicPostSummary:
    return PublicPostSummary(
        shortcode=str(row.get("shortcode") or "").strip(),
        media_id=str(row.get("media_id") or "").strip() or None,
        taken_at=str(row.get("taken_at") or "").strip() or None,
        taken_at_epoch=_optional_int(row.get("taken_at_epoch")),
        comment_count=_optional_int(row.get("comment_count")),
        like_count=_optional_int(row.get("like_count")),
        media_type=str(row.get("media_type") or "").strip() or None,
        page_index=max(0, int(row.get("page_index") or 0)),
    )


def _detail_status_from_state(row: Mapping[str, Any]) -> PublicDetailStatus:
    return PublicDetailStatus(
        shortcode=str(row.get("shortcode") or "").strip(),
        attempted=bool(row.get("attempted")),
        succeeded=bool(row.get("succeeded")),
        media_type=str(row.get("media_type") or "").strip() or None,
        media_url_count=max(0, int(row.get("media_url_count") or 0)),
        thumbnail_present=bool(row.get("thumbnail_present")),
        taken_at=str(row.get("taken_at") or "").strip() or None,
        caption_present=bool(row.get("caption_present")),
        stop_reason=str(row.get("stop_reason") or "").strip() or None,
        source=str(row.get("source") or "").strip() or None,
    )


def _comments_status_from_state(row: Mapping[str, Any]) -> PublicCommentsStatus:
    return PublicCommentsStatus(
        shortcode=str(row.get("shortcode") or "").strip(),
        attempted=bool(row.get("attempted")),
        classification=str(row.get("classification") or "public_blocked"),
        advertised_count=_optional_int(row.get("advertised_count")),
        recovered_count=max(0, int(row.get("recovered_count") or 0)),
        coverage_ratio=_optional_float(row.get("coverage_ratio")),
        terminal_reason=str(row.get("terminal_reason") or "").strip() or None,
        fallback_source=str(row.get("fallback_source") or "").strip() or None,
        stop_reason=str(row.get("stop_reason") or "").strip() or None,
    )


def _merge_post_summary(existing: PublicPostSummary | None, incoming: PublicPostSummary) -> PublicPostSummary:
    if existing is None:
        return incoming
    return PublicPostSummary(
        shortcode=existing.shortcode or incoming.shortcode,
        media_id=existing.media_id or incoming.media_id,
        taken_at=existing.taken_at or incoming.taken_at,
        taken_at_epoch=existing.taken_at_epoch or incoming.taken_at_epoch,
        comment_count=existing.comment_count if existing.comment_count is not None else incoming.comment_count,
        like_count=existing.like_count if existing.like_count is not None else incoming.like_count,
        media_type=existing.media_type or incoming.media_type,
        page_index=existing.page_index or incoming.page_index,
    )


def _collect_details(
    config: PublicProbeConfig,
    page_posts: list[PublicPostSummary],
    detail_statuses: dict[str, PublicDetailStatus],
    fetch_detail: Callable[[str], PublicDetailStatus],
) -> None:
    for post in _select_posts_for_mode(
        page_posts,
        mode=config.details_mode,
        per_page=config.sample_details_per_page,
        all_first_three_pages=True,
    ):
        if post.shortcode not in detail_statuses:
            detail_statuses[post.shortcode] = fetch_detail(post.shortcode)


def _collect_comments(
    config: PublicProbeConfig,
    page_posts: list[PublicPostSummary],
    comment_statuses: dict[str, PublicCommentsStatus],
    fetch_comments: Callable[[PublicPostSummary], PublicCommentsStatus],
) -> None:
    ordered = sorted(page_posts, key=lambda post: post.comment_count or 0, reverse=True)
    for post in _select_posts_for_mode(
        ordered,
        mode=config.comments_mode,
        per_page=config.sample_comments_per_page,
        all_first_three_pages=False,
    ):
        if post.shortcode not in comment_statuses:
            comment_statuses[post.shortcode] = fetch_comments(post)


def _select_posts_for_mode(
    page_posts: list[PublicPostSummary],
    *,
    mode: str,
    per_page: int,
    all_first_three_pages: bool,
) -> list[PublicPostSummary]:
    if mode == "all":
        return list(page_posts)
    if not page_posts or per_page <= 0:
        return []
    if all_first_three_pages and page_posts[0].page_index <= 3:
        return list(page_posts)
    return list(page_posts[:per_page])


def _stop_reason_from_metadata(metadata: Mapping[str, Any], *, after_progress: bool) -> str:
    status = _status_from_metadata(metadata)
    if status in BACKOFF_STOP_REASON_BY_STATUS:
        return BACKOFF_STOP_REASON_BY_STATUS[status]
    if after_progress:
        return "public_empty_after_progress_retry_later"
    return "public_empty_initial_page"


def _backoff_seconds_for_stop_reason(stop_reason: str, *, state: dict[str, Any]) -> int | None:
    status = None
    for candidate_status, reason in BACKOFF_STOP_REASON_BY_STATUS.items():
        if reason == stop_reason:
            status = candidate_status
            break
    if status is None:
        return None
    attempts = state.setdefault("backoff_attempts", {})
    attempt_index = max(0, int(attempts.get(stop_reason) or 0))
    schedule = BACKOFF_SECONDS_BY_STATUS.get(status) or ()
    attempts[stop_reason] = attempt_index + 1
    if not schedule:
        return None
    return int(schedule[min(attempt_index, len(schedule) - 1)])


def _update_terminal_state(
    state: dict[str, Any],
    *,
    stop_reason: str,
    cursor: str | None,
    completed: bool,
    retry_after_seconds: int | None = None,
) -> None:
    state["stop_reason"] = stop_reason
    state["cursor"] = cursor
    state["completed"] = bool(completed)
    state["partial"] = not completed
    state["updated_at"] = datetime.now(UTC).isoformat()
    if retry_after_seconds is not None:
        state["next_retry_after_seconds"] = retry_after_seconds


def _detail_rollup(statuses: list[PublicDetailStatus]) -> dict[str, Any]:
    attempted = [status for status in statuses if status.attempted]
    succeeded = [status for status in attempted if status.succeeded]
    return {
        "attempted": len(attempted),
        "succeeded": len(succeeded),
        "failed": len(attempted) - len(succeeded),
    }


def _comments_rollup(statuses: list[PublicCommentsStatus]) -> dict[str, Any]:
    attempted = [status for status in statuses if status.attempted]
    complete = [status for status in attempted if status.classification == "public_complete"]
    partial = [status for status in attempted if status.classification == "public_partial"]
    blocked = [status for status in attempted if status.classification == "public_blocked"]
    return {
        "attempted": len(attempted),
        "complete": len(complete),
        "partial": len(partial),
        "blocked": len(blocked),
    }


def _boundary_reached(posts: Any, until_date: date) -> bool:
    for post in posts:
        epoch = getattr(post, "taken_at_epoch", None)
        if epoch is None:
            continue
        post_date = datetime.fromtimestamp(epoch, tz=UTC).date()
        if post_date <= until_date:
            return True
    return False


def _target_posts(posts: Any, *, target_years: list[int]) -> list[PublicPostSummary]:
    target_set = set(target_years)
    return [post for post in posts if _post_year(post) in target_set]


def _target_year_counts(posts: list[PublicPostSummary], *, target_years: list[int]) -> dict[str, int]:
    counts = {str(year): 0 for year in target_years}
    for post in posts:
        year = _post_year(post)
        if year is not None and str(year) in counts:
            counts[str(year)] += 1
    return counts


def _post_year(post: PublicPostSummary) -> int | None:
    epoch = getattr(post, "taken_at_epoch", None)
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), tz=UTC).year
    except (OSError, OverflowError, ValueError):
        return None


def _oldest_post_at(posts: Any) -> str | None:
    epochs = [epoch for post in posts if (epoch := getattr(post, "taken_at_epoch", None))]
    if not epochs:
        return None
    return _iso_from_epoch(min(epochs))


def _doc_ids_attempted(metadata: Mapping[str, Any]) -> list[str]:
    for key in ("doc_ids_attempted", "profile_posts_doc_ids_attempted"):
        value = metadata.get(key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    retrieval_meta = metadata.get("retrieval_meta")
    if isinstance(retrieval_meta, Mapping):
        value = retrieval_meta.get("profile_posts_doc_ids_attempted")
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    return []


def _status_or_error(metadata: Mapping[str, Any]) -> str | int | None:
    status = _status_from_metadata(metadata)
    if status is not None:
        return status
    retrieval_meta = metadata.get("retrieval_meta")
    if isinstance(retrieval_meta, Mapping):
        return (
            retrieval_meta.get("error_code")
            or retrieval_meta.get("request_error_code")
            or retrieval_meta.get("error_class")
        )
    return metadata.get("error_code") or metadata.get("request_error_code")


def _status_from_metadata(metadata: Mapping[str, Any]) -> int | None:
    candidates: list[Any] = [
        metadata.get("error_status_code"),
        metadata.get("request_error_status_code"),
        metadata.get("status_code"),
    ]
    retrieval_meta = metadata.get("retrieval_meta")
    if isinstance(retrieval_meta, Mapping):
        candidates.extend(
            [
                retrieval_meta.get("error_status_code"),
                retrieval_meta.get("request_error_status_code"),
                retrieval_meta.get("status_code"),
            ]
        )
    for candidate in candidates:
        parsed = _optional_int(candidate)
        if parsed is not None:
            return parsed
    text = str(_status_or_error_text(metadata) or "").lower()
    if "unauthorized" in text or "401" in text:
        return 401
    if "forbidden" in text or "403" in text:
        return 403
    if "rate_limited" in text or "429" in text:
        return 429
    return None


def _status_or_error_text(metadata: Mapping[str, Any]) -> str:
    retrieval_meta = metadata.get("retrieval_meta")
    pieces = [
        metadata.get("error_code"),
        metadata.get("request_error_code"),
        metadata.get("error_class"),
    ]
    if isinstance(retrieval_meta, Mapping):
        pieces.extend(
            [
                retrieval_meta.get("error_code"),
                retrieval_meta.get("request_error_code"),
                retrieval_meta.get("error_class"),
                retrieval_meta.get("error_message"),
            ]
        )
    return " ".join(str(piece or "") for piece in pieces)


def _first_str(node: Mapping[str, Any], *paths: str) -> str:
    for path in paths:
        value = _path_value(node, path)
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _first_int(node: Mapping[str, Any], *paths: str) -> int | None:
    for path in paths:
        parsed = _optional_int(_path_value(node, path))
        if parsed is not None:
            return parsed
    return None


def _path_value(node: Mapping[str, Any], path: str) -> Any:
    current: Any = node
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _media_type_from_node(node: Mapping[str, Any]) -> str | None:
    raw = _first_int(node, "media_type")
    if raw == 8:
        return "carousel"
    if raw == 2:
        return "video"
    if raw == 1:
        return "image"
    text = _first_str(node, "product_type", "__typename")
    return text or None


def _iso_from_epoch(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), tz=UTC).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any, *, default: int = 0) -> int:
    parsed = _optional_int(value)
    return default if parsed is None else parsed


def _comment_tree_count(comments: Any) -> int:
    if not isinstance(comments, list):
        return 0
    total = 0
    stack = list(comments)
    while stack:
        item = stack.pop()
        total += 1
        replies = getattr(item, "replies", None)
        if isinstance(replies, list):
            stack.extend(replies)
    return total


__all__ = [
    "PublicCommentsStatus",
    "PublicDetailStatus",
    "PublicModeEnvironmentError",
    "PublicPostSummary",
    "PublicProbeConfig",
    "PublicProbePage",
    "PublicProbeResult",
    "build_arg_parser",
    "config_from_args",
    "main",
    "parse_target_years",
    "run_public_probe",
    "validate_public_environment",
]
