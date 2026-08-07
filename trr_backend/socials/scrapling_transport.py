"""Shared transport glue for TRR Scrapling fetchers.

This module intentionally stays below platform policy. It only owns fetcher
construction, cookie shape conversion/sync, proxy rotator construction, and
redaction-safe runtime metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any, cast
from urllib.parse import urlparse

from trr_backend.socials._scrapling_http_utils import extract_response_cookies

DEFAULT_TIMEOUT_MS = 45_000
DEFAULT_MAX_TRANSIENT_RETRIES = 3
DEFAULT_BASE_BACKOFF_SECONDS = 1.0
SCRAPLING_BROWSER_LOCALE = "en-US"


@dataclass(frozen=True, slots=True)
class ScraplingTransportDefaults:
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_transient_retries: int = DEFAULT_MAX_TRANSIENT_RETRIES
    base_backoff_seconds: float = DEFAULT_BASE_BACKOFF_SECONDS


DEFAULT_TRANSPORT = ScraplingTransportDefaults()
SCRAPLING_PROXY_CONFLICT_REASON = "scrapling_proxy_conflict"
_DECODO_SESSION_PARAM_RE = re.compile(r"(?:^|-)session-[^-]+(?:-|$)")
_DECODO_SESSION_DURATION_RE = re.compile(r"(?:^|-)sessionduration-\d+(?:-|$)")
_SCRAPLING_PROXY_SESSION_RE = re.compile(r"session-[A-Za-z0-9_.~]+")
_SCRAPLING_PROXY_SESSION_DURATION_RE = re.compile(r"sessionduration-\d+")


@dataclass(frozen=True, slots=True)
class ScraplingResolvedFetcherOptions:
    kwargs: dict[str, Any]
    metadata: dict[str, Any]


SCRAPLING_BROWSER_FETCHER_OPTION_SHAPES: dict[str, str] = {
    "headless": "bool",
    "disable_resources": "bool",
    "block_webrtc": "bool",
    "solve_cloudflare": "bool",
    "allow_webgl": "bool",
    "network_idle": "bool",
    "real_chrome": "bool",
    "hide_canvas": "bool",
    "dns_over_https": "bool",
    "block_ads": "bool",
    "ai_targeted": "bool",
    "load_dom": "bool",
    "locale": "str",
    "google_search": "bool",
    "timeout": "int",
    "wait": "int",
    "wait_selector": "str",
    "wait_selector_state": "str",
    "useragent": "str",
    "init_script": "str",
    "additional_args": "list_str",
    "blocked_domains": "list_str",
    "extra_headers": "dict_str",
    "cookies": "dict_str",
    "selector_config": "dict",
    "proxy": "proxy",
}
DEFAULT_SCRAPLING_BROWSER_FETCHER_OPTION_KEYS = frozenset(SCRAPLING_BROWSER_FETCHER_OPTION_SHAPES)


def _normalized_env_prefix(prefix: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", str(prefix or "").strip()).strip("_").upper()
    return normalized or "SCRAPLING"


def _env_option_name(prefix: str, key: str) -> str:
    return f"{_normalized_env_prefix(prefix)}_{str(key).upper()}"


def _safe_json_loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _parse_bool(raw: Any) -> bool | None:
    if isinstance(raw, bool):
        return raw
    value = str(raw or "").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def _parse_option_value(key: str, raw: Any, shape: str) -> Any:
    if shape == "bool":
        return _parse_bool(raw)
    if shape == "int":
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return None
    if shape == "str":
        value = str(raw or "").strip()
        return value or None
    if shape == "list_str":
        parsed = raw if isinstance(raw, list) else _safe_json_loads(str(raw))
        if parsed is None and isinstance(raw, str):
            parsed = [item.strip() for item in raw.split(",") if item.strip()]
        if isinstance(parsed, list) and all(not isinstance(item, (dict, list)) for item in parsed):
            return [str(item) for item in parsed if str(item).strip()]
        return None
    if shape == "dict_str":
        parsed = raw if isinstance(raw, Mapping) else _safe_json_loads(str(raw))
        if isinstance(parsed, Mapping) and all(not isinstance(value, (dict, list)) for value in parsed.values()):
            return {str(name): str(value) for name, value in parsed.items() if str(name).strip()}
        return None
    if shape == "dict":
        parsed = raw if isinstance(raw, Mapping) else _safe_json_loads(str(raw))
        return dict(parsed) if isinstance(parsed, Mapping) else None
    if shape == "proxy":
        if isinstance(raw, Mapping):
            return dict(raw)
        value = str(raw or "").strip()
        return value or None
    raise ValueError(f"unsupported_scrapling_option_shape:{key}:{shape}")


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [_json_safe(item) for item in value]
    return str(value)


def _scrub_session_tokens(value: str) -> str:
    text = _SCRAPLING_PROXY_SESSION_DURATION_RE.sub("sessionduration-redacted", str(value or ""))
    return _SCRAPLING_PROXY_SESSION_RE.sub("session-redacted", text)


def _safe_proxy_label(proxy: Any) -> str | None:
    raw = proxy.get("server") if isinstance(proxy, Mapping) else proxy
    if raw is None:
        return None
    value = str(raw or "").strip()
    if not value:
        return None
    parsed = urlparse(value)
    if parsed.hostname:
        host = parsed.hostname
        if parsed.port:
            host = f"{host}:{parsed.port}"
        scheme = parsed.scheme or "proxy"
        return _scrub_session_tokens(f"{scheme}://{host}")
    return _scrub_session_tokens(value.split("@")[-1].split("/")[0])


def _safe_option_metadata(key: str, value: Any) -> Any:
    if key == "proxy":
        return _safe_proxy_label(value)
    if key in {"cookies", "extra_headers", "selector_config"} and isinstance(value, Mapping):
        return {"keys": sorted(str(name) for name in value.keys())}
    if key == "init_script":
        return "configured"
    if isinstance(value, list):
        return {"count": len(value), "values": [_scrub_session_tokens(str(item)) for item in value]}
    if isinstance(value, Mapping):
        return {"keys": sorted(str(name) for name in value.keys())}
    return _json_safe(value)


def resolve_scrapling_fetcher_options(
    env_prefix: str,
    allowed_keys: Iterable[str] | None = None,
) -> ScraplingResolvedFetcherOptions:
    """Resolve opt-in Scrapling fetcher options from env without changing defaults."""
    allowed = set(allowed_keys or DEFAULT_SCRAPLING_BROWSER_FETCHER_OPTION_KEYS)
    kwargs: dict[str, Any] = {}
    invalid: list[str] = []
    raw_bundle = os.getenv(_env_option_name(env_prefix, "SCRAPLING_FETCHER_OPTIONS"))
    bundled: Mapping[str, Any] = {}
    if raw_bundle:
        parsed = _safe_json_loads(raw_bundle)
        if isinstance(parsed, Mapping):
            bundled = parsed
        else:
            invalid.append("SCRAPLING_FETCHER_OPTIONS")

    for key in sorted(allowed):
        shape = SCRAPLING_BROWSER_FETCHER_OPTION_SHAPES.get(key)
        if not shape:
            invalid.append(key)
            continue
        env_name = _env_option_name(env_prefix, key)
        has_raw = env_name in os.environ
        raw = os.getenv(env_name) if has_raw else bundled.get(key)
        if raw is None:
            continue
        parsed = _parse_option_value(key, raw, shape)
        if parsed is None:
            invalid.append(key)
            continue
        kwargs[key] = parsed

    # v0.4.12 otherwise follows the host locale. Keep the browser-facing
    # social lanes deterministic while still allowing their validated env
    # option to supply the same explicit value.
    kwargs.setdefault("locale", SCRAPLING_BROWSER_LOCALE)
    metadata: dict[str, Any] = {
        "configured_options": sorted(kwargs),
        "invalid_options": sorted(set(invalid)),
    }
    for key, value in kwargs.items():
        metadata[key] = _safe_option_metadata(key, value)
    return ScraplingResolvedFetcherOptions(kwargs=kwargs, metadata=metadata)


def _proxy_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        return [*value.keys(), *value.values()]
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return list(value)
    return [value]


def _response_meta_proxy(response: Any) -> Any | None:
    meta = getattr(response, "meta", None)
    if isinstance(meta, Mapping):
        return meta.get("proxy")
    return None


def safe_scrapling_proxy_metadata(
    response: Any | None = None,
    stats: Any | None = None,
    proxy_config: Any | None = None,
) -> dict[str, Any]:
    """Return redaction-safe proxy diagnostics from Scrapling responses/stats."""
    candidates: list[Any] = []
    candidates.extend(_proxy_values(_response_meta_proxy(response)))
    candidates.extend(_proxy_values(getattr(stats, "proxies", None)))
    labels = sorted({label for item in candidates if (label := _safe_proxy_label(item))})
    metadata: dict[str, Any] = {
        "scrapling_observed_proxy_labels": labels,
        "scrapling_observed_proxy_count": len(labels),
    }
    fingerprint = str(getattr(proxy_config, "fingerprint", "") or "").strip()
    if fingerprint:
        metadata["selected_proxy_fingerprint"] = _scrub_session_tokens(fingerprint)
    session_mode = str(getattr(proxy_config, "session_mode", "") or "").strip()
    if session_mode:
        metadata["proxy_session_mode"] = _scrub_session_tokens(session_mode)
    return metadata


def scrapling_fetcher_metadata(
    fetcher_class: Any,
    options_metadata: Mapping[str, Any] | None = None,
    observed_proxy_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return JSON-safe metadata for the Scrapling fetcher boundary."""
    if isinstance(fetcher_class, str):
        class_name = fetcher_class
    else:
        class_name = getattr(fetcher_class, "__name__", fetcher_class.__class__.__name__)
    return {
        "scrapling_fetcher_class": str(class_name),
        "scrapling_browser_tuning": _json_safe(dict(options_metadata or {})),
        **_json_safe(dict(observed_proxy_metadata or {})),
    }


def scrapling_runtime_versions() -> dict[str, str | None]:
    """Return installed Scrapling transport package versions without importing Scrapling."""
    packages = ("scrapling", "patchright", "playwright")
    result: dict[str, str | None] = {}
    for package in packages:
        try:
            result[package] = version(package)
        except PackageNotFoundError:
            result[package] = None
    return result


def scrapling_runtime_metadata() -> dict[str, Any]:
    """Redaction-safe runtime metadata for Scrapling-backed jobs and canaries."""
    versions = scrapling_runtime_versions()
    return {
        "scrapling_version": versions["scrapling"],
        "patchright_version": versions["patchright"],
        "playwright_version": versions["playwright"],
    }


def apply_decodo_session_affinity(
    username: str,
    *,
    use_sticky_proxy: bool,
    session_ttl_seconds: int,
    session_id: str | None = None,
) -> tuple[str, str]:
    """Return a Decodo username with optional sticky-session parameters."""
    normalized = str(username or "").strip()
    if not normalized:
        return "", "rotating"

    has_session = bool(_DECODO_SESSION_PARAM_RE.search(normalized))
    has_duration = bool(_DECODO_SESSION_DURATION_RE.search(normalized))

    if has_session:
        return normalized, "sticky_preconfigured"
    if not use_sticky_proxy:
        return normalized, "rotating"

    normalized_session_id = str(session_id or "").strip().lower()
    if normalized_session_id:
        sticky_session_id = hashlib.sha256(normalized_session_id.encode("utf-8")).hexdigest()[:16]
    else:
        sticky_session_id = uuid.uuid4().hex[:12]
    ttl_minutes = max(1, min(1440, math.ceil(max(1, int(session_ttl_seconds)) / 60)))
    updated = f"{normalized}-session-{sticky_session_id}"
    if not has_duration:
        updated = f"{updated}-sessionduration-{ttl_minutes}"
    return updated, "sticky"


def assert_no_conflicting_scrapling_proxies(
    *,
    session_proxy: Any | None = None,
    request_proxy: Any | None = None,
    request_proxies: Any | None = None,
) -> None:
    """Fail early when Scrapling proxy modes would conflict.

    Scrapling 0.4.9 correctly applies session-level proxies and raises when
    callers mix them with per-request proxy overrides. TRR checks this at the
    shared boundary so social lanes fail with one stable reason string.
    """
    if session_proxy is None:
        return
    if request_proxy is not None or request_proxies is not None:
        raise ValueError(SCRAPLING_PROXY_CONFLICT_REASON)


def cookies_to_scrapling(
    cookies: Mapping[str, str] | None,
    domain: str,
    path: str = "/",
) -> list[dict[str, Any]]:
    """Convert a simple name/value cookie mapping into browser cookie records."""
    cookie_domain = str(domain or "").strip()
    cookie_path = str(path or "").strip() or "/"
    if not cookie_domain:
        raise ValueError("domain is required for Scrapling browser cookies")

    payload: list[dict[str, Any]] = []
    for name, value in (cookies or {}).items():
        cookie_name = str(name or "").strip()
        cookie_value = str(value or "").strip()
        if not (cookie_name and cookie_value):
            continue
        payload.append(
            {
                "name": cookie_name,
                "value": cookie_value,
                "domain": cookie_domain,
                "path": cookie_path,
            }
        )
    return payload


def _build_fetcher_class(class_name: str, **kwargs: Any) -> Any:
    assert_no_conflicting_scrapling_proxies(
        session_proxy=kwargs.get("proxy"),
        request_proxies=kwargs.get("proxies"),
    )
    try:
        import scrapling.fetchers as fetchers
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling fetchers are unavailable. Install scrapling[fetchers].") from exc

    try:
        fetcher_cls = getattr(fetchers, class_name)
    except AttributeError as exc:
        raise RuntimeError(f"Scrapling {class_name} is unavailable. Install scrapling[fetchers].") from exc
    return fetcher_cls(**kwargs)


def build_stealthy_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's browser-backed StealthyFetcher."""
    return _build_fetcher_class("StealthyFetcher", **kwargs)


def build_dynamic_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's DynamicFetcher."""
    return _build_fetcher_class("DynamicFetcher", **kwargs)


def build_fetcher(**kwargs: Any) -> Any:
    """Lazily construct Scrapling's basic Fetcher."""
    return _build_fetcher_class("Fetcher", **kwargs)


def build_proxy_rotator(
    proxies: Iterable[str | dict[str, str]] | str | dict[str, str] | None,
) -> Any | None:
    """Build a Scrapling ProxyRotator from one or more proxy definitions."""
    if proxies is None:
        return None
    if isinstance(proxies, str):
        normalized: list[str | dict[str, str]] = [proxies.strip()] if proxies.strip() else []
    elif isinstance(proxies, dict):
        normalized = [cast("dict[str, str]", proxies)] if proxies else []
    else:
        normalized = []
        for proxy in proxies:
            if isinstance(proxy, str):
                value = proxy.strip()
                if value:
                    normalized.append(value)
            elif isinstance(proxy, dict) and proxy:
                normalized.append(proxy)

    if not normalized:
        return None

    try:
        from scrapling.fetchers import ProxyRotator
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Scrapling proxy rotation is unavailable. Install scrapling[fetchers].") from exc
    return ProxyRotator(normalized)


def merge_response_cookies(raw_cookies: Mapping[str, str] | None, response: Any) -> dict[str, str]:
    """Merge response cookies into an existing raw cookie mapping."""
    merged = {str(name): str(value) for name, value in (raw_cookies or {}).items()}
    merged.update(extract_response_cookies(response))
    return merged


def _cookie_names(cookies: Any) -> list[str]:
    if isinstance(cookies, Mapping):
        values = cookies.keys()
    elif isinstance(cookies, Iterable) and not isinstance(cookies, (str, bytes)):
        names: list[str] = []
        for cookie in cookies:
            if isinstance(cookie, Mapping):
                name = str(cookie.get("name") or "").strip()
                if name:
                    names.append(name)
        return sorted(set(names))
    else:
        values = []
    return sorted({str(name).strip() for name in values if str(name or "").strip()})


def safe_cookie_metadata(
    seed_cookies: Any,
    warmup_delta: Any | None = None,
    *,
    prefix: str = "cookie",
) -> dict[str, Any]:
    """Return cookie diagnostics that expose names/counts but never values."""
    key_prefix = f"{prefix.strip()}_" if prefix.strip() else ""
    seed_names = _cookie_names(seed_cookies)
    warmup_names = _cookie_names(warmup_delta or {})
    return {
        f"{key_prefix}seed_cookie_names": seed_names,
        f"{key_prefix}seed_cookie_count": len(seed_names),
        f"{key_prefix}warmup_cookie_names": warmup_names,
        f"{key_prefix}warmup_cookie_count": len(warmup_names),
    }
