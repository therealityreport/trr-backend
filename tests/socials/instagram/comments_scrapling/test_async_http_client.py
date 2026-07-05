"""Comments-lane async transport factory: httpx default + curl_cffi opt-in.

Covers resolver defaults, the httpx default path, the missing-dependency
fallback (a mis-set flag must never be fatal), the transport-exception tuple,
timeout coercion, and the curl_cffi adapter when the dependency is installed.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import patch

import httpx

from trr_backend.socials.instagram.comments_scrapling import async_http_client as mod


def test_resolve_defaults_to_httpx_on_unset_and_garbage():
    with patch.dict(os.environ, {}, clear=True):
        assert mod.resolve_comments_http_client_name() == "httpx"
    with patch.dict(os.environ, {mod.HTTP_CLIENT_ENV: "nonsense"}, clear=True):
        assert mod.resolve_comments_http_client_name() == "httpx"
    with patch.dict(os.environ, {mod.HTTP_CLIENT_ENV: "curl_cffi"}, clear=True):
        assert mod.resolve_comments_http_client_name() == "curl_cffi"
    with patch.dict(os.environ, {mod.HTTP_CLIENT_ENV: "HTTPX"}, clear=True):
        assert mod.resolve_comments_http_client_name() == "httpx"


def test_build_defaults_to_httpx_async_client():
    client = mod.build_comments_async_client(
        timeout=httpx.Timeout(5.0),
        headers={"accept-encoding": "identity"},
    )
    try:
        assert isinstance(client, httpx.AsyncClient)
    finally:
        asyncio.run(client.aclose())


def test_explicit_httpx_name_builds_httpx():
    client = mod.build_comments_async_client(client_name="httpx")
    try:
        assert isinstance(client, httpx.AsyncClient)
    finally:
        asyncio.run(client.aclose())


def test_curl_cffi_missing_dependency_falls_back_to_httpx():
    # When curl_cffi is requested but unavailable, the factory must not raise — it
    # logs and falls back to httpx so a mis-set flag can't take the lane down.
    with patch.object(mod, "curl_cffi_available", return_value=False):
        client = mod.build_comments_async_client(client_name="curl_cffi")
    try:
        assert isinstance(client, httpx.AsyncClient)
    finally:
        asyncio.run(client.aclose())


def test_transport_exc_types_cover_httpx_base():
    for exc in (TimeoutError, httpx.TimeoutException, httpx.TransportError, httpx.DecodingError, OSError):
        assert exc in mod.TRANSPORT_EXC_TYPES


def test_timeout_seconds_coercion():
    assert mod._timeout_seconds(None, default=7.0) == 7.0
    assert mod._timeout_seconds(12) == 12.0
    assert mod._timeout_seconds(3.5) == 3.5
    # httpx.Timeout exposes per-phase floats; the read ceiling is used.
    assert mod._timeout_seconds(httpx.Timeout(9.0)) == 9.0


def test_curl_cffi_adapter_builds_when_available():
    # curl_cffi is installed in CI/dev; the adapter must construct (inside a loop,
    # as it is in production) and expose the async get/post interface.
    if not mod.curl_cffi_available():
        return  # environment without curl_cffi: covered by the fallback test

    async def _build_and_close() -> object:
        client = mod.build_comments_async_client(
            client_name="curl_cffi",
            timeout=httpx.Timeout(5.0),
            headers={"accept-encoding": "identity"},
        )
        assert isinstance(client, mod._CurlCffiAsyncClient)
        assert hasattr(client, "get") and hasattr(client, "post")
        await client.__aexit__(None, None, None)
        return client

    asyncio.run(_build_and_close())
