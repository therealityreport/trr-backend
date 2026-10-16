"""Shared safety checks for scraper-discovered media URLs."""

from __future__ import annotations

import ipaddress
import math
import socket
import threading
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError, NewConnectionError


class UnsafeMediaUrlError(ValueError):
    """Raised when a media URL is not safe for backend fetching."""


@dataclass(frozen=True, slots=True)
class MediaUrlSafetyPolicy:
    allowed_host_suffixes: tuple[str, ...]
    allow_test_hosts: bool = True
    resolve_dns: bool = True
    allow_any_public_host: bool = False


# DNS work has bounded concurrency; a blocked resolver cannot grow one thread per fetch.
_DNS_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="media-dns")
_PUBLIC_IPV6 = ipaddress.ip_network("2000::/3")
_IPV6_EXCLUDED = tuple(
    ipaddress.ip_network(value) for value in ("2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20")
)
_IPV4_RELAY = ipaddress.ip_network("192.88.99.0/24")
_IPV4_PROTOCOL = ipaddress.ip_network("192.0.0.0/24")


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


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    if not ip.is_global or ip.is_multicast:
        return True
    if isinstance(ip, ipaddress.IPv4Address):
        return ip in _IPV4_RELAY or ip in _IPV4_PROTOCOL
    return ip not in _PUBLIC_IPV6 or any(ip in network for network in _IPV6_EXCLUDED)


def _validate_resolved_addresses(hostname: str, port: int = 443, *, deadline: float | None = None) -> tuple[str, ...]:
    try:
        deadline = deadline if deadline is not None else time.monotonic() + 5
        future = _DNS_EXECUTOR.submit(socket.getaddrinfo, hostname, port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        try:
            infos = future.result(timeout=max(0, deadline - time.monotonic()))
        except FutureTimeoutError:
            future.cancel()
            raise requests.exceptions.Timeout("media_request_timeout") from None
    except requests.exceptions.Timeout:
        raise
    except OSError as exc:
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
        if "%" in address:
            raise UnsafeMediaUrlError("media_url_dns_resolution_invalid")
        try:
            ip = ipaddress.ip_address(address)
        except ValueError as exc:
            raise UnsafeMediaUrlError("media_url_dns_resolution_invalid") from exc
        if _is_blocked_ip(ip):
            raise UnsafeMediaUrlError("media_url_resolves_to_blocked_ip")
    return tuple(sorted(addresses, key=lambda value: (ipaddress.ip_address(value).version, value)))


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
    try:
        parsed = urlparse(candidate)
        port = parsed.port
    except ValueError as exc:
        raise UnsafeMediaUrlError("media_url_invalid") from exc
    if parsed.scheme.lower() not in {"http", "https"}:
        raise UnsafeMediaUrlError("media_url_invalid_scheme")
    hostname = str(parsed.hostname or "").strip().lower()
    if not hostname:
        raise UnsafeMediaUrlError("media_url_missing_host")
    if parsed.username is not None or parsed.password is not None or "%" in hostname or port == 0:
        raise UnsafeMediaUrlError("media_url_invalid")
    if hostname.rstrip(".") in {
        "localhost",
        "localhost.localdomain",
        "ip6-localhost",
        "ip6-loopback",
    } or hostname.rstrip(".").endswith((".localhost", ".local")):
        raise UnsafeMediaUrlError("media_url_blocked_ip")

    try:
        literal_ip = ipaddress.ip_address(hostname)
    except ValueError:
        literal_ip = None
        try:
            socket.inet_aton(hostname)
        except OSError:
            pass
        else:
            # Reject legacy shorthand/hex/octal forms rather than delegate their
            # interpretation to a second parser or the system resolver.
            raise UnsafeMediaUrlError("media_url_invalid")
    if literal_ip is not None:
        if _is_blocked_ip(literal_ip):
            raise UnsafeMediaUrlError("media_url_blocked_ip")
        active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
        if not active_policy.allow_any_public_host and not _hostname_matches(
            hostname, active_policy.allowed_host_suffixes
        ):
            raise UnsafeMediaUrlError("media_url_host_not_allowed")
        return candidate

    active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
    host_allowed = active_policy.allow_any_public_host or _hostname_matches(
        hostname, active_policy.allowed_host_suffixes
    )
    if not host_allowed:
        if not (active_policy.allow_test_hosts and _is_reserved_test_host(hostname)):
            raise UnsafeMediaUrlError("media_url_host_not_allowed")

    if active_policy.resolve_dns and not _is_reserved_test_host(hostname):
        _validate_resolved_addresses(hostname, port or (443 if parsed.scheme.lower() == "https" else 80))
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


class _PinnedAdapter(HTTPAdapter):
    """A fresh pool per hop: original origin for HTTP/TLS, numeric address for TCP."""

    def __init__(self, address: str, deadline: float) -> None:
        self.address = address
        self.deadline = deadline
        self.connections: list[Any] = []
        super().__init__(max_retries=0)

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        owner = self

        class _PinnedConnection:
            def _new_conn(self) -> socket.socket:
                remaining = owner.deadline - time.monotonic()
                if remaining <= 0:
                    raise ConnectTimeoutError(self, "media_request_timeout")
                ip = ipaddress.ip_address(owner.address)
                sock = socket.socket(socket.AF_INET6 if ip.version == 6 else socket.AF_INET, socket.SOCK_STREAM)
                owner.connections.append(self)
                self._media_socket = sock
                try:
                    sock.settimeout(min(float(self.timeout), remaining))
                    for option in self.socket_options or ():
                        sock.setsockopt(*option)
                    if self.source_address:
                        sock.bind(self.source_address)
                    # No second resolver lookup. TLS still uses self.host, the URL hostname.
                    sock.connect((str(ip), self.port))
                    remaining = owner.deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("media_request_timeout")
                    sock.settimeout(min(float(self.timeout), remaining))
                    # TLS wrapping detaches the original socket before the
                    # handshake finishes. An independent descriptor keeps a
                    # hard abort path to the same TCP stream through that gap.
                    self._media_abort_socket = sock.dup()
                    return sock
                except TimeoutError as exc:
                    sock.close()
                    raise ConnectTimeoutError(self, "media_request_timeout") from exc
                except OSError as exc:
                    sock.close()
                    raise NewConnectionError(self, "media_connection_failed") from exc

        class _HttpConnection(_PinnedConnection, HTTPConnection):
            pass

        class _HttpsConnection(_PinnedConnection, HTTPSConnection):
            def connect(self) -> None:
                super().connect()
                # With Connection: close, urllib3 detaches conn.sock while its
                # response still owns the TLS stream. Retain it for hard abort.
                self._media_socket = self.sock

        class _HttpPool(HTTPConnectionPool):
            ConnectionCls = _HttpConnection

        class _HttpsPool(HTTPSConnectionPool):
            ConnectionCls = _HttpsConnection

        super().init_poolmanager(*args, **kwargs)
        # Assign an instance-local mapping; never mutate urllib3's shared registry.
        self.poolmanager.pool_classes_by_scheme = {"http": _HttpPool, "https": _HttpsPool}

    def abort(self) -> None:
        for connection in self.connections:
            for sock in (
                getattr(connection, "sock", None),
                getattr(connection, "_media_socket", None),
                getattr(connection, "_media_abort_socket", None),
            ):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()


def _send_pinned_request(
    prepared: requests.PreparedRequest,
    *,
    address: str,
    deadline: float,
    timeout: tuple[float, float],
    body_timeout: float,
    max_bytes: int,
    headers_only: bool = False,
    verify: Any = True,
    cert: Any = None,
) -> requests.Response:
    adapter = _PinnedAdapter(address, deadline)
    expired = threading.Event()

    def expire() -> None:
        expired.set()
        adapter.abort()

    timer = threading.Timer(max(0, deadline - time.monotonic()), expire)
    timer.daemon = True
    timer.start()
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise requests.exceptions.Timeout("media_request_timeout")
        response = adapter.send(
            prepared,
            stream=True,
            timeout=(min(timeout[0], remaining), min(timeout[1], remaining)),
            verify=verify,
            cert=cert,
            proxies={},
        )
        if expired.is_set() or time.monotonic() >= deadline:
            response.close()
            raise requests.exceptions.Timeout("media_request_timeout")
    except Exception as exc:
        timer.cancel()
        adapter.abort()
        adapter.close()
        if expired.is_set() or isinstance(exc, requests.exceptions.Timeout):
            raise requests.exceptions.Timeout("media_request_timeout") from None
        if isinstance(exc, requests.exceptions.RequestException):
            raise requests.exceptions.ConnectionError("media_connection_failed") from None
        raise

    if headers_only:
        # A metadata probe never downloads a body, even if the caller later
        # accesses response.content or iter_content. Preserve status/headers/url.
        timer.cancel()
        response.close()
        adapter.abort()
        adapter.close()
        response._content = b""
        response._content_consumed = True
        return response

    # Keep the deadline alive through body consumption, including slow dripping streams.
    timer.cancel()
    body_deadline = min(deadline, time.monotonic() + body_timeout)
    timer = threading.Timer(max(0, body_deadline - time.monotonic()), expire)
    timer.daemon = True
    timer.start()
    original_close = response.close
    original_iter = response.iter_content

    def close() -> None:
        timer.cancel()
        original_close()
        adapter.abort()
        adapter.close()

    def iter_content(chunk_size: int = 1, decode_unicode: bool = False) -> Iterable[Any]:
        received = 0
        try:
            for chunk in original_iter(chunk_size=chunk_size, decode_unicode=decode_unicode):
                if expired.is_set() or time.monotonic() >= body_deadline:
                    raise requests.exceptions.Timeout("media_request_timeout")
                received += len(chunk.encode(response.encoding or "utf-8") if isinstance(chunk, str) else chunk)
                if received > max_bytes:
                    raise UnsafeMediaUrlError("asset_too_large")
                yield chunk
            if expired.is_set() or time.monotonic() >= body_deadline:
                raise requests.exceptions.Timeout("media_request_timeout")
        except Exception as exc:
            if expired.is_set() or isinstance(exc, requests.exceptions.Timeout):
                raise requests.exceptions.Timeout("media_request_timeout") from None
            if isinstance(exc, requests.exceptions.RequestException):
                raise requests.exceptions.ConnectionError("media_body_failed") from None
            raise
        finally:
            close()

    response.close = close
    response.iter_content = iter_content
    content_length = response.headers.get("Content-Length")
    if content_length and content_length.isdecimal() and int(content_length) > max_bytes:
        close()
        raise UnsafeMediaUrlError("asset_too_large")
    return response


def safe_requests_request(
    client: Any,
    method: str,
    url: str,
    *,
    policy: MediaUrlSafetyPolicy | None = None,
    allowed_host_suffixes: Iterable[str] | None = None,
    max_redirects: int = 5,
    max_bytes: int = 50 * 1024 * 1024,
    headers_only: bool = False,
    total_timeout: float = 120,
    body_timeout: float = 90,
    **kwargs: Any,
) -> requests.Response:
    """Validate every destination and pin TCP while preserving original-host TLS.

    Test-host/DNS validation shortcuts apply only to standalone validation. Real
    transport always resolves all answers, rejects any unsafe answer and pins one.
    Environment proxies and disabled certificate verification are never used.
    HEAD and explicit headers_only probes close after metadata, with an empty
    consumed response body. Full downloads must pass their effective byte cap.
    """
    from dataclasses import replace

    active_policy = policy or MediaUrlSafetyPolicy(tuple(allowed_host_suffixes or ()))
    if method.upper() not in {"GET", "HEAD"}:
        raise UnsafeMediaUrlError("media_request_method_not_allowed")
    if (
        not isinstance(max_bytes, int)
        or max_bytes <= 0
        or any(not math.isfinite(value) or value <= 0 for value in (total_timeout, body_timeout))
        or not isinstance(max_redirects, int)
        or max_redirects < 0
    ):
        raise UnsafeMediaUrlError("media_request_invalid_limits")
    deadline = time.monotonic() + total_timeout
    follow_redirects = bool(kwargs.pop("allow_redirects", True))
    stream = bool(kwargs.pop("stream", False))
    timeout = kwargs.pop("timeout", (5, 30))
    if not isinstance(timeout, tuple):
        timeout = (timeout, timeout)
    if len(timeout) != 2 or any(
        value is None or not math.isfinite(float(value)) or float(value) <= 0 for value in timeout
    ):
        raise UnsafeMediaUrlError("media_request_invalid_limits")
    connect_timeout, read_timeout = (float(value) for value in timeout)
    session = client if isinstance(client, requests.Session) else requests.Session()
    if client is not requests and not isinstance(client, requests.Session):
        raise UnsafeMediaUrlError("media_request_unsupported_client")
    verify = kwargs.pop("verify", session.verify)
    cert = kwargs.pop("cert", session.cert)
    if (
        (verify is not True and not (isinstance(verify, str) and verify))
        or kwargs.pop("proxies", None)
        or session.proxies
    ):
        raise UnsafeMediaUrlError("media_request_unsafe_transport")
    initial_url = validate_media_url(url, policy=replace(active_policy, resolve_dns=False))
    # Explicit session/request auth is retained; arbitrary image origins must not
    # silently pick up credentials from the operator's environment/netrc.
    if kwargs.get("auth") is None and session.auth is None:
        kwargs["auth"] = lambda request: request
    try:
        prepared = session.prepare_request(requests.Request(method.upper(), initial_url, **kwargs))
    except requests.exceptions.RequestException:
        raise UnsafeMediaUrlError("media_url_invalid") from None
    finally:
        if client is requests:
            session.close()
    prepared.headers.pop("Host", None)
    prepared.headers.pop("Proxy-Authorization", None)
    prepared.headers["Connection"] = "close"
    # Avoid decompression amplification; byte caps still apply to actual decoded data.
    prepared.headers["Accept-Encoding"] = "identity"
    for redirect_count in range(max(0, int(max_redirects)) + 1):
        current_url = validate_media_url(str(prepared.url), policy=replace(active_policy, resolve_dns=False))
        parsed = urlparse(current_url)
        hostname = str(parsed.hostname)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            literal = ipaddress.ip_address(hostname)
        except ValueError:
            literal = None
        addresses = (
            (str(literal),)
            if literal is not None
            else _validate_resolved_addresses(
                hostname,
                port,
                deadline=min(deadline, time.monotonic() + connect_timeout),
            )
        )
        response = _send_pinned_request(
            prepared,
            address=addresses[0],
            deadline=deadline,
            timeout=(connect_timeout, read_timeout),
            body_timeout=body_timeout,
            max_bytes=max_bytes,
            headers_only=headers_only or method.upper() == "HEAD",
            verify=verify,
            cert=cert,
        )
        if follow_redirects and response.status_code in {301, 302, 303, 307, 308}:
            try:
                location = response.headers.get("location")
                if not location:
                    raise UnsafeMediaUrlError("media_url_redirect_missing_location")
                next_url = validate_media_redirect(
                    current_url, location, policy=replace(active_policy, resolve_dns=False)
                )
                if redirect_count >= max_redirects:
                    raise UnsafeMediaUrlError("media_url_redirect_limit_exceeded")
                headers = dict(prepared.headers)
                next_parsed = urlparse(next_url)
                if (parsed.scheme, parsed.hostname, parsed.port) != (
                    next_parsed.scheme,
                    next_parsed.hostname,
                    next_parsed.port,
                ):
                    for key in tuple(headers):
                        if key.lower() in {"authorization", "cookie", "proxy-authorization"}:
                            del headers[key]
                prepared = requests.Request(method.upper(), next_url, headers=headers).prepare()
            finally:
                response.close()
            continue
        if not stream:
            try:
                _ = response.content
            finally:
                response.close()
        return response
    raise UnsafeMediaUrlError("media_url_redirect_limit_exceeded")


def safe_requests_get(client: Any, url: str, **kwargs: Any) -> requests.Response:
    return safe_requests_request(client, "GET", url, **kwargs)


def safe_requests_head(client: Any, url: str, **kwargs: Any) -> requests.Response:
    return safe_requests_request(client, "HEAD", url, **kwargs)
