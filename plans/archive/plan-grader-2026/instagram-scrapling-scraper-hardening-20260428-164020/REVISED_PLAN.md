# Instagram Scrapling Scraper Hardening Revised Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Instagram Scrapling scraper lanes safe to dispatch, retry, cancel, and diagnose without rewriting the production Instagram scraper.

**Architecture:** Keep the legacy Instagram scraper and existing stage-based posts/comments Scrapling lanes intact. Fix the pluggable `ScraplingRuntime` scaffold so it cannot crash dispatcher fallback, share retry classification across posts/comments, add posts warmup parity with comments, preserve final runtime metadata, and make cancellation checks cooperative without adding unnecessary DB pool pressure.

**Tech Stack:** Python 3.11, Scrapling 0.4.6, httpx, pytest, Ruff, TRR backend social queue helpers.

---

## Non-Goals

- Do not implement the full pluggable `ScraplingRuntime` fetch methods in this pass.
- Do not change app routes or API contracts.
- Do not run destructive Supabase or live Instagram reset operations.
- Do not broaden this into a rewrite of `trr_backend/socials/instagram/scraper.py`.

## Success Signals

- `ScraplingRuntime.healthcheck()` is unhealthy with reason `scrapling_runtime_not_wired` until the runtime is truly implemented.
- `httpx.TransportError` failures in posts/comments lanes become retryable `transport_error` results.
- posts warmup no-cookie failures carry `instagram_posts_warmup_no_cookies` and include fetcher runtime metadata in job metadata.
- comments cancellation checks do not open a second DB connection while `persist_conn` is held.
- focused pytest, Ruff check, Ruff format check, and `git diff --check` pass for touched files.

## Files

Modify:

- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- `trr_backend/socials/instagram/runtimes/__init__.py`
- `trr_backend/socials/_scrapling_http_utils.py`
- `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- `trr_backend/socials/instagram/posts_scrapling/session.py`
- `trr_backend/socials/instagram/comments_scrapling/session.py`
- `docs/workspace/instagram-posts-scrapling.md`
- `docs/workspace/instagram-comments-scrapling.md`

Create:

- `trr_backend/socials/instagram/scrapling_session.py`
- `tests/socials/instagram/runtimes/test_scrapling_runtime.py`
- `tests/socials/instagram/test_scrapling_session.py`

Update tests:

- `tests/socials/instagram/runtimes/test_dispatcher.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`

## Task 0: Preflight Current State

**Files:**
- Read: all files listed above

- [ ] **Step 1: Confirm repo status**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git status --short
```

Expected: note any unrelated dirty files. Do not revert unrelated user work.

- [ ] **Step 2: Confirm Scrapling package surface**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python - <<'PY'
from scrapling.fetchers import StealthyFetcher
import inspect
import scrapling
print("scrapling", scrapling.__version__)
print("StealthyFetcher.async_fetch", inspect.signature(StealthyFetcher.async_fetch))
PY
```

Expected: `scrapling 0.4.6` and `StealthyFetcher.async_fetch` exists.

- [ ] **Step 3: Run current dispatcher tests**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest tests/socials/instagram/runtimes/test_dispatcher.py -q
```

Expected: current dispatcher tests pass before changes.

## Task 1: Make `ScraplingRuntime` a Safe Unsupported Scaffold

**Files:**
- Modify: `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- Modify: `trr_backend/socials/instagram/runtimes/__init__.py`
- Create: `tests/socials/instagram/runtimes/test_scrapling_runtime.py`
- Modify: `tests/socials/instagram/runtimes/test_dispatcher.py`

- [ ] **Step 1: Write failing scaffold tests**

Create `tests/socials/instagram/runtimes/test_scrapling_runtime.py`:

```python
from __future__ import annotations

import asyncio

import pytest

from trr_backend.socials.instagram.runtimes.protocol import RuntimeUnsupported
from trr_backend.socials.instagram.runtimes.scrapling_runtime import ScraplingRuntime


def _run(coro):
    return asyncio.run(coro)


def test_scrapling_runtime_health_is_unsupported_scaffold() -> None:
    runtime = ScraplingRuntime()

    health = runtime.healthcheck()

    assert health.healthy is False
    assert health.reason == "scrapling_runtime_not_wired"


def test_scrapling_runtime_methods_raise_runtime_unsupported() -> None:
    runtime = ScraplingRuntime()

    with pytest.raises(RuntimeUnsupported, match="fetch_profile is not wired"):
        _run(runtime.fetch_profile("bravotv"))

    with pytest.raises(RuntimeUnsupported, match="fetch_posts is not wired"):
        _run(runtime.fetch_posts("bravotv", limit=3))

    with pytest.raises(RuntimeUnsupported, match="fetch_post_detail is not wired"):
        _run(runtime.fetch_post_detail("abc123"))
```

Append to `tests/socials/instagram/runtimes/test_dispatcher.py`:

```python
def test_dispatcher_skips_unhealthy_scrapling_scaffold_when_present() -> None:
    from trr_backend.socials.instagram.runtimes.scrapling_runtime import ScraplingRuntime

    crawlee = _RuntimeStub("crawlee")
    disp = InstagramRuntimeDispatcher(
        factories={
            "scrapling": lambda: ScraplingRuntime(),
            "crawlee": lambda: crawlee,
        },
        order=["scrapling", "crawlee"],
    )

    result = _run(disp.fetch_profile("bravotv"))

    assert result.username == "bravotv"
    assert crawlee.fetch_profile_calls == 1
```

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  -q
```

Expected: new runtime tests fail because current `ScraplingRuntime.healthcheck()` returns healthy and methods raise `NotImplementedError`.

- [ ] **Step 3: Implement unsupported scaffold**

Replace `trr_backend/socials/instagram/runtimes/scrapling_runtime.py` with:

```python
"""Scrapling-based Instagram runtime scaffold.

The production Scrapling lanes currently live under posts_scrapling/ and
comments_scrapling/. This pluggable runtime is not wired yet, so it must be
skipped by the dispatcher instead of reporting healthy and raising
NotImplementedError at request time.
"""

from __future__ import annotations

from typing import Any

from trr_backend.socials.instagram.runtimes.protocol import (
    InstagramRuntime,
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)


class ScraplingRuntime:
    name = "scrapling"

    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self._cookies = dict(cookies or {})

    def healthcheck(self) -> RuntimeHealth:
        try:
            import scrapling  # noqa: F401
        except ImportError as exc:
            return RuntimeHealth(
                healthy=False,
                reason=f"scrapling_not_installed: pip install scrapling ({exc})",
            )
        return RuntimeHealth(healthy=False, reason="scrapling_runtime_not_wired")

    async def fetch_profile(self, username: str) -> ProfileInfo:
        raise RuntimeUnsupported(f"ScraplingRuntime.fetch_profile is not wired for {username!r}")

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        raise RuntimeUnsupported(f"ScraplingRuntime.fetch_posts is not wired for {username!r}, limit={limit}")

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        raise RuntimeUnsupported(f"ScraplingRuntime.fetch_post_detail is not wired for {shortcode!r}")

    async def _fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        raise RuntimeUnsupported(f"ScraplingRuntime._fetch_json is not wired for {url!r}")


assert isinstance(ScraplingRuntime(), InstagramRuntime)  # type: ignore[misc]
```

In `trr_backend/socials/instagram/runtimes/__init__.py`, update the scaffold language to say:

```python
Runtimes that wrap external packages are scaffolded as unhealthy or
RuntimeUnsupported until their current-version APIs are verified via docs;
do not call them in production without completing the implementation plan
for each file.
```

- [ ] **Step 4: Verify pass**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  -q
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/runtimes/__init__.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py
git commit -m "fix: make instagram scrapling runtime scaffold unsupported"
```

## Task 2: Classify Transport Errors as Retryable

**Files:**
- Modify: `trr_backend/socials/_scrapling_http_utils.py`
- Modify: `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- Modify: `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- Modify: `tests/socials/test_instagram_comments_scrapling_retry.py`

- [ ] **Step 1: Add failing transport error tests**

Append to `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`:

```python
def test_transport_error_is_retryable_then_succeeds() -> None:
    fetcher = _make_fetcher()
    resp_ok = _make_httpx_response(status_code=200, json_data={"status": "ok"})
    fetcher._fetch_graphql = AsyncMock(side_effect=[httpx.ConnectError("network down"), resp_ok])

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is False
    assert fetcher._fetch_graphql.await_count == 2


def test_transport_error_exhausts_retries() -> None:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

    fetcher = _make_fetcher()
    fetcher._fetch_graphql = AsyncMock(side_effect=httpx.ConnectError("network down"))

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="r", data={}, headers={}))

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_error"
    assert fetcher._fetch_graphql.await_count == InstagramPostsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
```

Append to `tests/socials/test_instagram_comments_scrapling_retry.py`:

```python
def test_transport_error_is_retryable_then_succeeds() -> None:
    fetcher = _build_fetcher()
    response = _mock_httpx_response(status_code=200, json_data={"status": "ok", "comments": []})
    fetcher._fetch_api = AsyncMock(side_effect=[httpx.ConnectError("network down"), response])

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/abc/",
            )
        )

    assert result["failed"] is False
    assert fetcher._fetch_api.await_count == 2


def test_transport_error_exhausts_retries() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(side_effect=httpx.ConnectError("network down"))

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/abc/",
            )
        )

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_error"
    assert fetcher._fetch_api.await_count == InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
```

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py::test_transport_error_is_retryable_then_succeeds \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py::test_transport_error_exhausts_retries \
  tests/socials/test_instagram_comments_scrapling_retry.py::test_transport_error_is_retryable_then_succeeds \
  tests/socials/test_instagram_comments_scrapling_retry.py::test_transport_error_exhausts_retries \
  -q
```

Expected: uncaught `httpx.ConnectError` failures.

- [ ] **Step 3: Add shared helpers**

Append to `trr_backend/socials/_scrapling_http_utils.py`:

```python
def transport_failure_reason(exc: BaseException) -> str:
    """Map transport exceptions to stable retry metadata."""
    exc_name = exc.__class__.__name__.lower()
    if "timeout" in exc_name:
        return "transport_timeout"
    return "transport_error"


def transient_backoff_seconds(*, attempt: int, base_seconds: float, retry_after: float | None = None) -> float:
    """Return bounded exponential backoff, honoring Retry-After when present."""
    if retry_after is not None:
        return max(0.0, retry_after)
    exponent = max(0, int(attempt) - 1)
    return max(0.0, float(base_seconds) * (2 ** exponent))
```

- [ ] **Step 4: Use helpers in both fetchers**

In both fetchers, import:

```python
from trr_backend.socials._scrapling_http_utils import transient_backoff_seconds as _transient_backoff_seconds
from trr_backend.socials._scrapling_http_utils import transport_failure_reason as _transport_failure_reason
```

Replace timeout-only exception handling in `_fetch_json_response()` with:

```python
except (TimeoutError, httpx.TimeoutException, httpx.TransportError) as exc:
    last_transient_reason = _transport_failure_reason(exc)
    if attempt > self._MAX_TRANSIENT_RETRIES:
        return {
            "failed": True,
            "auth_failed": False,
            "reason": last_transient_reason,
            "retryable": True,
            "payload": None,
        }
    await asyncio.sleep(
        _transient_backoff_seconds(
            attempt=attempt,
            base_seconds=self._BASE_BACKOFF_SECONDS,
        )
    )
    continue
```

Replace transient status sleeps with:

```python
sleep_seconds = _transient_backoff_seconds(
    attempt=attempt,
    base_seconds=self._BASE_BACKOFF_SECONDS,
    retry_after=self._retry_after_seconds(response),
)
await asyncio.sleep(sleep_seconds)
```

- [ ] **Step 5: Verify retry suites**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Expected: retry tests pass.

- [ ] **Step 6: Commit**

```bash
git add \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/test_instagram_comments_scrapling_retry.py
git commit -m "fix: classify instagram scrapling transport errors as retryable"
```

## Task 3: Harden Posts Warmup and Preserve Warmup Metadata

**Files:**
- Modify: `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- Modify: `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- Modify: `tests/socials/instagram/posts_scrapling/test_fetcher.py`
- Modify: `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- Modify: `tests/socials/instagram/posts_scrapling/test_job_runner.py`

- [ ] **Step 1: Add failing warmup tests**

Append to `tests/socials/instagram/posts_scrapling/test_fetcher.py`:

```python
def test_posts_warmup_raises_when_no_cookies_are_bridged(_mock_scrapling):
    import asyncio
    from unittest.mock import AsyncMock, MagicMock

    import pytest

    from trr_backend.socials.instagram.posts_scrapling.fetcher import (
        InstagramPostsScraplingFetcher,
        InstagramPostsWarmupError,
    )

    fetcher = InstagramPostsScraplingFetcher(cookies=[], raw_cookies={}, browser_account_id="bravotv")
    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.text = "<html></html>"
    fake_resp.cookies = {}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=fake_resp)

    with pytest.raises(InstagramPostsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup("bravotv"))

    assert exc_info.value.error_code == "instagram_posts_warmup_no_cookies"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["warmup_cookie_count"] == 0
```

Append to `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`:

```python
def test_redirect_to_homepage_recovers_once() -> None:
    fetcher = _make_fetcher()
    redirect = _make_httpx_response(status_code=302, headers={"location": "https://www.instagram.com/"})
    ok = _make_httpx_response(status_code=200, json_data={"status": "ok"})
    fetcher._fetch_graphql = AsyncMock(side_effect=[redirect, ok])
    fetcher._recover_homepage_redirect = AsyncMock(return_value=True)

    with patch(_SLEEP_TARGET, AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                _URL,
                referer="https://www.instagram.com/bravotv/",
                data={},
                headers={},
            )
        )

    assert result["failed"] is False
    fetcher._recover_homepage_redirect.assert_awaited_once()
    assert fetcher._fetch_graphql.await_count == 2
```

Append to `tests/socials/instagram/posts_scrapling/test_job_runner.py`:

```python
def test_job_runner_preserves_posts_warmup_runtime_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsWarmupError
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job

    captured_finish: dict[str, object] = {}

    class _FakeFetcher:
        runtime_metadata = {"warmup_cookie_count": 0, "request_count": 1}

        def __init__(self, **_kwargs) -> None:
            pass

        async def warmup(self, _account_handle: str) -> None:
            raise InstagramPostsWarmupError(
                "no cookies",
                error_code="instagram_posts_warmup_no_cookies",
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies=[],
            browser_account_id="bravotv",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr("trr_backend.socials.instagram.posts_scrapling.job_runner.select_posts_proxy", lambda: None)
    monkeypatch.setattr("trr_backend.socials.instagram.posts_scrapling.job_runner.InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt: 5)
    monkeypatch.setattr(repo, "_now_utc", lambda: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: {},
    )

    def _fake_finish_job(job_id, *, status, metadata=None, **kwargs):
        captured_finish["status"] = status
        captured_finish["metadata"] = metadata
        captured_finish["kwargs"] = kwargs

    monkeypatch.setattr(repo, "_finish_job", _fake_finish_job)

    run_instagram_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}, "attempt_count": 1, "max_attempts": 2},
        worker_id="worker-1",
    )

    metadata = dict(captured_finish["metadata"] or {})
    assert captured_finish["status"] == "retrying"
    assert metadata["runtime_metadata"]["warmup_cookie_count"] == 0
    assert metadata["fetcher_runtime"]["warmup_cookie_count"] == 0
```

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py::test_posts_warmup_raises_when_no_cookies_are_bridged \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py::test_redirect_to_homepage_recovers_once \
  tests/socials/instagram/posts_scrapling/test_job_runner.py::test_job_runner_preserves_posts_warmup_runtime_metadata \
  -q
```

Expected: missing `InstagramPostsWarmupError`, missing `_recover_homepage_redirect`, and missing job-runner metadata preservation failures.

- [ ] **Step 3: Add posts warmup error and homepage recovery**

In `posts_scrapling/fetcher.py`, add:

```python
class InstagramPostsWarmupError(RuntimeError):
    error_code: str
    retryable: bool

    def __init__(self, message: str, *, error_code: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable
```

In `warmup()`, raise `InstagramPostsWarmupError` for auth failure and no-cookie warmup:

```python
if _status_code(response) in {401, 403} or _auth_failure_text(text):
    raise InstagramPostsWarmupError(
        "Instagram posts warmup failed because the session appears logged out or challenged.",
        error_code="instagram_posts_warmup_auth_failed",
        retryable=False,
    )
self._page_tokens = _extract_page_tokens(text)
self._merge_warmup_cookies(response)
if not self._warmup_cookie_delta and not str(self._raw_cookies.get("sessionid") or "").strip():
    raise InstagramPostsWarmupError(
        "Instagram posts warmup did not bridge any cookies.",
        error_code="instagram_posts_warmup_no_cookies",
        retryable=True,
    )
await self._rebuild_http_client()
```

Add homepage redirect recovery:

```python
async def _recover_homepage_redirect(self, *, referer: str) -> bool:
    recovery_url = str(referer or "").strip() or "https://www.instagram.com/"
    try:
        recovery_response = await self._fetch_page(recovery_url, referer=recovery_url)
    except Exception:
        logger.warning("Instagram posts homepage redirect recovery warmup failed for %s", recovery_url, exc_info=True)
        return False
    status = _status_code(recovery_response)
    text = _response_text(recovery_response)
    if status >= 400 or 300 <= status < 400 or _auth_failure_text(text):
        return False
    self._merge_warmup_cookies(recovery_response)
    await self._rebuild_http_client()
    return True
```

In the 3xx branch of `_fetch_json_response()`, mirror the comments-lane one-shot homepage recovery.

- [ ] **Step 4: Preserve posts warmup metadata in job runner**

In `posts_scrapling/job_runner.py`, import the warmup error:

```python
from trr_backend.socials.instagram.posts_scrapling.fetcher import (
    InstagramPostsScraplingFetcher,
    InstagramPostsWarmupError,
)
```

Wrap warmup like comments:

```python
try:
    await fetcher.warmup(account_handle)
except InstagramPostsWarmupError as exc:
    fetcher_metadata = dict(fetcher.runtime_metadata)
    raise PostsScraplingRuntimeError(
        str(exc),
        error_code=exc.error_code,
        retryable=exc.retryable,
        runtime_metadata=dict(fetcher.runtime_metadata),
    ) from exc
```

After each posts page fetch, refresh final metadata:

```python
result = await fetcher.fetch_posts_page(account_handle, cursor=cursor)
fetcher_metadata = dict(fetcher.runtime_metadata)
```

Immediately before `return auth_metadata, fetcher_metadata`, refresh again:

```python
fetcher_metadata = dict(fetcher.runtime_metadata)
return auth_metadata, fetcher_metadata
```

- [ ] **Step 5: Verify posts warmup tests**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  -q
```

Expected: selected posts tests pass.

- [ ] **Step 6: Commit**

```bash
git add \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py
git commit -m "fix: harden instagram posts scrapling warmup metadata"
```

## Task 4: Add Cooperative Cancellation Without Extra Comments-Lane Pool Pressure

**Files:**
- Modify: `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- Modify: `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- Modify: `tests/socials/test_instagram_comments_scrapling_retry.py`

- [ ] **Step 1: Add failing cancellation tests**

Append to `tests/socials/test_instagram_comments_scrapling_retry.py`:

```python
def test_comments_job_runner_stops_when_job_is_cancelled_before_persist_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    db_connection_calls = {"count": 0}

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        async def warmup(self) -> None:
            return None

        async def aclose(self) -> None:
            return None

    def _unexpected_db_connection(**_kwargs):
        db_connection_calls["count"] += 1
        raise AssertionError("cancelled job should stop before opening persist connection")

    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda session_key=None: None)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)
    monkeypatch.setattr(jr.pg, "db_connection", _unexpected_db_connection)
    monkeypatch.setattr(
        jr.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"job_status": "cancelled", "run_status": "running"},
    )

    result = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {"account": "bravotv", "target_source_ids": ["abc123"]},
        },
        worker_id="worker-1",
    )

    assert result["status"] == "cancelled"
    assert finish_calls[-1]["status"] == "cancelled"
    assert db_connection_calls["count"] == 0
```

Append to `tests/socials/instagram/posts_scrapling/test_job_runner.py`:

```python
def test_posts_job_runner_returns_degraded_summary_when_final_fetch_is_saturated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    class _FakeFetcher:
        runtime_metadata = {"request_count": 2}

        def __init__(self, **_kwargs) -> None:
            pass

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, cursor=None):
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "abc123"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies=[],
            browser_account_id="bravotv",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr("trr_backend.socials.instagram.posts_scrapling.job_runner.select_posts_proxy", lambda: None)
    monkeypatch.setattr("trr_backend.socials.instagram.posts_scrapling.job_runner.InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(posts_upserted=1, posts_skipped=0, posts_skipped_by_reason={}),
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            pg.DatabaseServiceUnavailableError("pool exhausted", reason="pool_exhausted")
        ),
    )

    result = run_instagram_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "job_type": "posts", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert result["status"] == "completed"
    assert result["metadata"]["degraded_summary"] is True
    assert result["metadata"]["database_service_unavailable"] is True
```

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/test_instagram_comments_scrapling_retry.py::test_comments_job_runner_stops_when_job_is_cancelled_before_persist_connection \
  tests/socials/instagram/posts_scrapling/test_job_runner.py::test_posts_job_runner_returns_degraded_summary_when_final_fetch_is_saturated \
  -q
```

Expected: failures because cooperative cancellation and posts degraded summary fallback are missing.

- [ ] **Step 3: Add cancellation exception and helper**

In both job runners, add:

```python
@dataclass(slots=True)
class ScraplingJobCancelled(Exception):
    job_id: str
    run_id: str | None
    cancel_scope: str
    runtime_metadata: dict[str, Any] | None = None

    @property
    def error_code(self) -> str:
        return "instagram_scrapling_job_cancelled"

    @property
    def retryable(self) -> bool:
        return False

    def __str__(self) -> str:
        return f"Instagram Scrapling job {self.job_id} was cancelled by {self.cancel_scope}."
```

Add a connection-aware helper to both job runners:

```python
def _raise_if_cancelled(
    job_id: str,
    run_id: str | None,
    *,
    runtime_metadata: dict[str, Any] | None = None,
    conn: Any | None = None,
) -> None:
    row = pg.fetch_one(
        """
        select
          j.status as job_status,
          r.status as run_status
        from social.scrape_jobs j
        left join social.scrape_runs r on r.id = j.run_id
        where j.id = %s
        limit 1
        """,
        [job_id],
        conn=conn,
    )
    job_status = str((row or {}).get("job_status") or "").strip().lower()
    run_status = str((row or {}).get("run_status") or "").strip().lower()
    if job_status == "cancelled":
        raise ScraplingJobCancelled(job_id=job_id, run_id=run_id, cancel_scope="job", runtime_metadata=runtime_metadata)
    if run_status == "cancelled":
        raise ScraplingJobCancelled(job_id=job_id, run_id=run_id, cancel_scope="run", runtime_metadata=runtime_metadata)
```

- [ ] **Step 4: Wire cancellation checks**

In `comments_scrapling/job_runner.py`, after warmup and before opening `pg.db_connection`, add:

```python
_raise_if_cancelled(job_id, run_id or None, runtime_metadata=dict(fetcher.runtime_metadata))
```

Inside the comments loop, call with the existing persist connection:

```python
_raise_if_cancelled(
    job_id,
    run_id or None,
    runtime_metadata=dict(fetcher.runtime_metadata),
    conn=persist_conn,
)
```

In `posts_scrapling/job_runner.py`, call before each page fetch:

```python
_raise_if_cancelled(job_id, run_id or None, runtime_metadata=dict(fetcher.runtime_metadata))
```

- [ ] **Step 5: Handle cancellation and degraded final summary**

In `comments_scrapling/job_runner.py`, handle `ScraplingJobCancelled` before retry handling with comments-specific counts:

```python
if isinstance(exc, ScraplingJobCancelled):
    items_found = processed_posts + comments_fetched
    repo._finish_job(
        job_id,
        status="cancelled",
        items_found=items_found,
        error_message=str(exc),
        metadata={
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "error_code": exc.error_code,
            "cancel_scope": exc.cancel_scope,
            "activity": {"phase": "cancelled", "last_progress_at": repo._iso(repo._now_utc())},
            "runtime_metadata": exc.runtime_metadata,
            "fetcher_runtime": fetcher_metadata,
        },
        last_error_code=exc.error_code,
        last_error_class=exc.__class__.__name__,
    )
    terminal_status = "cancelled"
    terminal_error_message = str(exc)
    return {
        "id": job_id,
        "run_id": run_id or None,
        "platform": "instagram",
        "job_type": str(job.get("job_type") or stage).strip() or stage,
        "status": "cancelled",
        "items_found": items_found,
        "error_message": str(exc),
        "metadata": {"error_code": exc.error_code, "cancel_scope": exc.cancel_scope},
    }
```

In `posts_scrapling/job_runner.py`, use the same control flow with posts-specific counts:

```python
if isinstance(exc, ScraplingJobCancelled):
    items_found = posts_fetched
    repo._finish_job(
        job_id,
        status="cancelled",
        items_found=items_found,
        error_message=str(exc),
        metadata={
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "error_code": exc.error_code,
            "cancel_scope": exc.cancel_scope,
            "activity": {"phase": "cancelled", "last_progress_at": repo._iso(repo._now_utc())},
            "runtime_metadata": exc.runtime_metadata,
            "fetcher_runtime": fetcher_metadata,
        },
        last_error_code=exc.error_code,
        last_error_class=exc.__class__.__name__,
    )
    terminal_status = "cancelled"
    terminal_error_message = str(exc)
    return {
        "id": job_id,
        "run_id": run_id or None,
        "platform": "instagram",
        "job_type": str(job.get("job_type") or stage).strip() or stage,
        "status": "cancelled",
        "items_found": items_found,
        "error_message": str(exc),
        "metadata": {"error_code": exc.error_code, "cancel_scope": exc.cancel_scope},
    }
```

In `posts_scrapling/job_runner.py`, add `terminal_status` and `terminal_error_message` near the counters:

```python
terminal_status = str(job.get("status") or "").strip().lower() or None
terminal_error_message: str | None = None
```

Set `terminal_status = "completed"` after successful `_finish_job()` and set it in generic exception handling. Wrap the final `pg.fetch_one()` return in `except pg.DatabaseServiceUnavailableError`, returning the same degraded shape as comments with `items_found=posts_fetched`.

- [ ] **Step 6: Verify job runner tests**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Expected: job runner tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py
git commit -m "fix: make instagram scrapling jobs cancellable and observable"
```

## Task 5: Share Scrapling Session Cookie Adapters

**Files:**
- Create: `trr_backend/socials/instagram/scrapling_session.py`
- Modify: `trr_backend/socials/instagram/posts_scrapling/session.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/session.py`
- Create: `tests/socials/instagram/test_scrapling_session.py`

- [ ] **Step 1: Add failing shared adapter tests**

Create `tests/socials/instagram/test_scrapling_session.py`:

```python
from __future__ import annotations

from types import SimpleNamespace


def test_cookies_to_scrapling_filters_blank_values() -> None:
    from trr_backend.socials.instagram.scrapling_session import cookies_to_scrapling

    result = cookies_to_scrapling({"sessionid": "abc", "csrftoken": "", " ds_user_id ": " 123 "})

    assert result == [
        {"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"},
        {"name": "ds_user_id", "value": "123", "domain": ".instagram.com", "path": "/"},
    ]


def test_posts_and_comments_sessions_share_adapter(monkeypatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
    from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

    auth_session = SimpleNamespace(
        cookies={"sessionid": "abc"},
        browser_account_id="bravotv",
        metadata={"source": "test"},
    )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.scrapling_session.resolve_instagram_auth_session",
        lambda **_kwargs: auth_session,
    )

    posts = resolve_posts_scrapling_session(browser_account_id="bravotv", caller_context="posts")
    comments = resolve_comments_scrapling_session(browser_account_id="bravotv", caller_context="comments")

    assert posts.cookies == comments.cookies
    assert posts.auth_session is auth_session
    assert comments.auth_session is auth_session
    assert posts.browser_account_id == "bravotv"
    assert comments.browser_account_id == "bravotv"
```

- [ ] **Step 2: Verify tests fail**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest tests/socials/instagram/test_scrapling_session.py -q
```

Expected: import failure because `trr_backend/socials/instagram/scrapling_session.py` does not exist.

- [ ] **Step 3: Create shared adapter and compatibility wrappers**

Create `trr_backend/socials/instagram/scrapling_session.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from trr_backend.socials.instagram.auth_resolver import InstagramAuthSession, resolve_instagram_auth_session


@dataclass(slots=True)
class InstagramScraplingSession:
    auth_session: InstagramAuthSession
    browser_account_id: str | None
    cookies: list[dict[str, Any]]


def cookies_to_scrapling(cookies: dict[str, str]) -> list[dict[str, Any]]:
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
                "domain": ".instagram.com",
                "path": "/",
            }
        )
    return payload


def resolve_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramScraplingSession:
    auth_session = resolve_instagram_auth_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
    )
    return InstagramScraplingSession(
        auth_session=auth_session,
        browser_account_id=auth_session.browser_account_id or browser_account_id,
        cookies=cookies_to_scrapling(auth_session.cookies),
    )
```

Replace `posts_scrapling/session.py` with a compatibility wrapper:

```python
from __future__ import annotations

from trr_backend.socials.instagram.scrapling_session import InstagramScraplingSession, resolve_scrapling_session

InstagramPostsScraplingSession = InstagramScraplingSession


def resolve_posts_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramPostsScraplingSession:
    return resolve_scrapling_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
    )
```

Replace `comments_scrapling/session.py` with a compatibility wrapper:

```python
from __future__ import annotations

from trr_backend.socials.instagram.scrapling_session import InstagramScraplingSession, resolve_scrapling_session

InstagramCommentsScraplingSession = InstagramScraplingSession


def resolve_comments_scrapling_session(
    *,
    browser_account_id: str | None,
    caller_context: str,
) -> InstagramCommentsScraplingSession:
    return resolve_scrapling_session(
        browser_account_id=browser_account_id,
        caller_context=caller_context,
    )
```

- [ ] **Step 4: Verify session adapter**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/test_instagram_comments_scrapling.py \
  -q
```

Expected: selected session and fetcher tests pass.

- [ ] **Step 5: Commit**

```bash
git add \
  trr_backend/socials/instagram/scrapling_session.py \
  trr_backend/socials/instagram/posts_scrapling/session.py \
  trr_backend/socials/instagram/comments_scrapling/session.py \
  tests/socials/instagram/test_scrapling_session.py
git commit -m "refactor: share instagram scrapling session adapter"
```

## Task 6: Update Runbooks

**Files:**
- Modify: `docs/workspace/instagram-posts-scrapling.md`
- Modify: `docs/workspace/instagram-comments-scrapling.md`

- [ ] **Step 1: Update posts runbook**

Add these rows to the existing failure table in `docs/workspace/instagram-posts-scrapling.md`:

```markdown
| `instagram_posts_warmup_no_cookies` | Scrapling warmup returned a page but did not bridge any usable cookies and no prior `sessionid` existed | Refresh the Instagram browser session, rerun `./scripts/setup_scrapling.sh` if browser deps are stale, then rerun a one-page smoke |
| `redirect_to_homepage` after one recovery attempt | Instagram redirected the GraphQL call to the profile/home surface and the second browser warmup did not restore an API-usable session | Treat as auth failure; refresh cookies and reduce concurrent jobs for that account |
| `transport_error` with `retryable: true` | httpx connect/read/proxy transport failure after browser warmup | Let queue retry once; if repeated, inspect proxy health and `selected_proxy_fingerprint` in job metadata |
| Job row is `cancelled` while worker logs still show active fetches | Worker predates cooperative cancellation hardening or is inside one in-flight API call | After this plan lands, workers check cancellation between pages and target posts; restart old worker processes |
```

Add this under `## Observability`, using four-backtick outer fences:

````markdown
Current Scrapling API verification:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python - <<'PY'
from scrapling.fetchers import StealthyFetcher
import inspect
print("StealthyFetcher.async_fetch", inspect.signature(StealthyFetcher.async_fetch))
PY
```

Expected: output includes `async_fetch(url: str, **kwargs: Unpack[StealthSession])`.
````

- [ ] **Step 2: Update comments runbook**

Add under `## Known failure modes and remediation`:

```markdown
### Cooperative cancellation

The comments worker checks `social.scrape_jobs.status` and the linked
`social.scrape_runs.status` after warmup and between target posts. The
between-post check reuses the existing persist connection, so it does not
add a second pool checkout while comment persistence is active. A
cancellation request does not interrupt an in-flight Instagram API call,
but it stops before the next shortcode is fetched and finishes the job as
`cancelled` with `metadata.error_code = "instagram_scrapling_job_cancelled"`.

### Retryable transport errors

`httpx.TimeoutException`, Python `TimeoutError`, and `httpx.TransportError`
are classified as retryable transport failures. The stable reasons are
`transport_timeout` and `transport_error`. Repeated `transport_error`
failures usually point to proxy connectivity or local network interruption,
not parser drift.
```

- [ ] **Step 3: Verify docs and touched Python compile**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m py_compile \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  trr_backend/socials/instagram/scrapling_session.py
```

Expected: no output and exit code `0`.

- [ ] **Step 4: Commit**

```bash
git add \
  docs/workspace/instagram-posts-scrapling.md \
  docs/workspace/instagram-comments-scrapling.md
git commit -m "docs: update instagram scrapling scraper runbooks"
```

## Task 7: Final Verification

**Files:**
- Verify all touched files.

- [ ] **Step 1: Run focused pytest**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run Ruff check**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/ruff check \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/runtimes/__init__.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  trr_backend/socials/instagram/scrapling_session.py \
  trr_backend/socials/instagram/posts_scrapling/session.py \
  trr_backend/socials/instagram/comments_scrapling/session.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  tests/socials/test_instagram_comments_scrapling.py
```

Expected: `All checks passed!`.

- [ ] **Step 3: Run Ruff format check**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/ruff format --check \
  trr_backend/socials/_scrapling_http_utils.py \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/runtimes/__init__.py \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  trr_backend/socials/instagram/comments_scrapling/fetcher.py \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  trr_backend/socials/instagram/scrapling_session.py \
  trr_backend/socials/instagram/posts_scrapling/session.py \
  trr_backend/socials/instagram/comments_scrapling/session.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  tests/socials/instagram/test_scrapling_session.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  tests/socials/test_instagram_comments_scrapling.py
```

Expected: no files would be reformatted.

- [ ] **Step 4: Verify metadata does not leak cookie values**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py::test_runtime_metadata_never_exposes_cookie_values \
  tests/socials/test_instagram_comments_scrapling.py::test_comments_fetcher_runtime_metadata_never_exposes_cookie_values \
  -q
```

Expected: `2 passed`.

- [ ] **Step 5: Verify whitespace**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git diff --check
```

Expected: no output.

- [ ] **Step 6: Reviewer handoff**

Record these facts in the PR or final implementation note:

- `ScraplingRuntime` is deliberately unsupported, not implemented.
- posts/comments Scrapling lanes still use browser warmup plus httpx API calls.
- cancellation is cooperative between work units, not an in-flight request interrupt.
- Scrapling MCP was not required for implementation; local package/API checks were used.

## Execution Notes

- Do not implement the full pluggable `ScraplingRuntime` in this plan; only make the scaffold safe and explicitly unsupported.
- Do not run a live Instagram scrape by default. Keep live verification to an operator-approved smoke after tests pass.
- Avoid extra DB pool checkout inside the comments persist loop by passing `conn=persist_conn` to cancellation checks.
- Restart stale worker processes before live verification so old workers do not keep the pre-cancellation behavior.
- Keep generated Plan Grader files as evidence until implementation and verification are complete.

## Recommended Subagent Split

- Runtime and dispatcher worker: owns `trr_backend/socials/instagram/runtimes/*` plus runtime tests.
- Fetcher worker: owns `_scrapling_http_utils.py`, posts fetcher, comments fetcher, and retry tests.
- Job-runner and cancellation worker: owns posts/comments job runners and their tests.
- Session adapter and docs worker: owns `scrapling_session.py`, session wrappers, shared session tests, and runbook updates.

## Cleanup Note

After this plan is completely implemented and verified, delete any temporary planning artifacts that are no longer needed, including generated audit, scorecard, suggestions, comparison, patch, benchmark, and validation files. Do not delete them before implementation is complete because they are part of the execution evidence trail.
