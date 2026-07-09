"""Shared safety checks for scraper-discovered media URLs."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse


class UnsafeMediaUrlError(ValueError):
    """Raised when a media URL is not safe for backend fetching."""


@dataclass(frozen=True, slots=True)
class MediaUrlSafetyPolicy:
    allowed_host_suffixes: tuple[str, ...]
    allow_test_hosts: bool = True
    resolve_dns: bool = True


_RESERVED_TEST_SUFFIXES = (".test", ".example", ".invalid")
_RESERVED_TEST_HOSTS = {"example.com", "example.net", "example.org"}

_PLATFORM_ALLOWED_HOST_SUFFIXES: dict[str, tuple[str, ...]] = {
    "instagram": (
        "instagram.com",
        "cdninstagram.com",
        "fbcdn.net",
        "fbsbx.com",
    ),
    "threads": (
        "threads.net",
        "threads.com",
        "instagram.com",
        "cdninstagram.com",
        "fbcdn.net",
        "fbsbx.com",
    ),
    "facebook": (
        "facebook.com",
        "fbcdn.net",
        "fbsbx.com",
    ),
    "tiktok": (
        "tiktok.com",
        "tiktokcdn.com",
        "tiktokcdn-us.com",
        "tiktokv.com",
        "byteoversea.com",
        "tikwm.com",
    ),
    "twitter": (
        "twitter.com",
        "x.com",
        "twimg.com",
    ),
    "youtube": (
        "youtube.com",
        "youtu.be",
        "ggpht.com",
        "googlevideo.com",
        "ytimg.com",
    ),
    "reddit": (
        "redd.it",
        "redditmedia.com",
        "redditstatic.com",
        "reddit.com",
    ),
}


def allowed_hosts_for_platform(platform: str | None) -> tuple[str, ...]:
    normalized = str(platform or "").strip().lower()
    return _PLATFORM_ALLOWED_HOST_SUFFIXES.get(normalized, ())


def _hostname_matches(hostname: str, allowed_suffixes: Iterable[str]) -> bool:
    normalized = hostname.strip(".").lower()
    if not normalized:
        return False
    for suffix in allowed_suffixes:
        allowed = str(suffix or "").strip(".").lower()
        if not allowed:
            continue
        if normalized == allowed or normalized.endswith(f".{allowed}"):
            return True
    return False


def _is_reserved_test_host(hostname: str) -> bool:
    normalized = hostname.strip(".").lower()
    return normalized in _RESERVED_TEST_HOSTS or normalized.endswith(_RESERVED_TEST_SUFFIXES)


def _is_blocked_ip(ip: ipaddress._BaseAddress) -> bool:
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    return any(
        (
            ip.is_loopback,
            ip.is_private,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        )
    )


def _validate_resolved_addresses(hostname: str) -> None:
    try:
        infos = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeMediaUrlError("media_url_dns_resolution_failed") from exc

    addresses: set[str] = set()
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        addresses.add(str(sockaddr[0]))
    if not addresses:
        raise UnsafeMediaUrlError("media_url_dns_resolution_empty")

    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError as exc:
            raise UnsafeMediaUrlError("media_url_dns_resolution_invalid") from exc
        if _is_blocked_ip(ip):
            raise UnsafeMediaUrlError("media_url_resolves_to_blocked_ip")


def validate_media_url(
    url: str,
    *,
    policy: MediaUrlSafetyPolicy | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
) -> str:
    """Return a normalized URL after enforcing outbound media safety rules."""

    candidate = str(url or "").strip()
    if not candidate:
        raise UnsafeMediaUrlError("media_url_empty")
    parsed = urlparse(candidate)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UnsafeMediaUrlError("media_url_invalid_scheme")
    hostname = str(parsed.hostname or "").strip().lower()
    if not hostname:
        raise UnsafeMediaUrlError("media_url_missing_host")

    try:
        literal_ip = ipaddress.ip_address(hostname)
    except ValueError:
        literal_ip = None
    if literal_ip is not None:
        if _is_blocked_ip(literal_ip):
            raise UnsafeMediaUrlError("media_url_blocked_ip")
        active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
        if not _hostname_matches(hostname, active_policy.allowed_host_suffixes):
            raise UnsafeMediaUrlError("media_url_host_not_allowed")
        return candidate

    active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
    host_allowed = _hostname_matches(hostname, active_policy.allowed_host_suffixes)
    if not host_allowed:
        if not (active_policy.allow_test_hosts and _is_reserved_test_host(hostname)):
            raise UnsafeMediaUrlError("media_url_host_not_allowed")

    if active_policy.resolve_dns and not _is_reserved_test_host(hostname):
        _validate_resolved_addresses(hostname)
    return candidate


def validate_media_redirect(
    base_url: str,
    location: str,
    *,
    policy: MediaUrlSafetyPolicy | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
) -> str:
    return validate_media_url(
        urljoin(base_url, str(location or "").strip()),
        policy=policy,
        allowed_host_suffixes=allowed_host_suffixes,
    )


def safe_requests_request(
    client: Any,
    method: str,
    url: str,
    *,
    policy: MediaUrlSafetyPolicy | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
    max_redirects: int = 5,
    **kwargs: Any,
) -> Any:
    """Issue a requests-compatible call while validating each redirect hop."""

    active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
    current_url = validate_media_url(url, policy=active_policy)
    caller_allow_redirects = bool(kwargs.pop("allow_redirects", True))
    request_method = getattr(client, method.lower(), None)
    request = getattr(client, "request", None)
    if callable(request_method):

        def _send(target_url: str, *, allow_redirects: bool) -> Any:
            return request_method(target_url, allow_redirects=allow_redirects, **kwargs)
    elif callable(request):

        def _send(target_url: str, *, allow_redirects: bool) -> Any:
            return request(method, target_url, allow_redirects=allow_redirects, **kwargs)
    else:
        raise AttributeError(f"client does not support {method}")

    if not caller_allow_redirects:
        return _send(current_url, allow_redirects=False)

    for _ in range(max(0, int(max_redirects)) + 1):
        response = _send(current_url, allow_redirects=False)
        status_code = int(getattr(response, "status_code", 0) or 0)
        location = (getattr(response, "headers", None) or {}).get("location")
        if status_code not in {301, 302, 303, 307, 308} or not location:
            final_url = str(getattr(response, "url", "") or current_url)
            validate_media_url(final_url, policy=active_policy)
            return response
        next_url = validate_media_redirect(current_url, str(location), policy=active_policy)
        close = getattr(response, "close", None)
        if callable(close):
            close()
        current_url = next_url
    raise UnsafeMediaUrlError("media_url_redirect_limit_exceeded")


def safe_requests_get(client: Any, url: str, **kwargs: Any) -> Any:
    return safe_requests_request(client, "GET", url, **kwargs)


def safe_requests_head(client: Any, url: str, **kwargs: Any) -> Any:
    return safe_requests_request(client, "HEAD", url, **kwargs)
