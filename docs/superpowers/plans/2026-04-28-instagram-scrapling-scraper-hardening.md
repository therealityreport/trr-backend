# Instagram Scrapling Scraper Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Instagram Scrapling scraper lanes safer to dispatch, easier to diagnose, and less likely to leave stale running jobs when Instagram, Scrapling, the proxy layer, or the database misbehaves.

**Architecture:** Keep the existing production scraper and queue contracts intact. Harden the opt-in Scrapling posts/comments lanes by fixing runtime dispatch semantics, sharing response classification, adding posts-lane parity with the comments-lane warmup/recovery behavior, preserving final runtime metadata, and adding cooperative cancellation checks inside long-running jobs.

**Tech Stack:** Python 3.11, Scrapling 0.4.7, httpx, FastAPI backend repository helpers, pytest, Ruff.

---

## Audit Summary

Current evidence:

- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py` reports healthy when `scrapling` imports, but every endpoint raises `NotImplementedError`. The dispatcher only falls through on `RuntimeUnsupported`, so an env order that reaches `scrapling` can crash instead of trying the next runtime.
- `posts_scrapling/fetcher.py` and `comments_scrapling/fetcher.py` independently classify redirects, JSON parse failures, transient statuses, retry-after, and timeout handling. They already diverge: comments has homepage redirect recovery and warmup no-cookie classification; posts does not.
- Both fetchers catch `httpx.TimeoutException`, but not broader `httpx.TransportError` failures such as connect errors and read errors. Those become generic job failures with no retry scheduling.
- Posts job completion metadata can under-report `fetcher_runtime.request_count` because `run_instagram_posts_scrapling_job()` snapshots `fetcher.runtime_metadata` immediately after warmup and does not refresh it at normal completion.
- Long-running posts/comments jobs do not perform a lane-local cooperative cancellation check between post/page units. Repository-level cancel functions update rows, but an already-running Python process may continue until the target list/page loop ends.
- `posts_scrapling/session.py` and `comments_scrapling/session.py` duplicate the same auth-to-Scrapling cookie adapter, increasing drift risk.

Scrapling context used:

- Installed backend venv package: `scrapling==0.4.7`.
- `StealthyFetcher.async_fetch` exists and accepts the current warmup kwargs through `StealthSession`.
- The Scrapling skill routing says to use `StealthyFetcher`/MCP `stealthy_fetch` for protected sites, persistent sessions for repeated protected fetches, and focused selector/parser guidance only after a page has been retrieved.

## File Structure

Modify:

- `trr_backend/socials/instagram/runtimes/scrapling_runtime.py` - make the scaffold explicitly unsupported until the runtime is actually implemented.
- `trr_backend/socials/instagram/runtimes/__init__.py` - update runtime status text so operators do not expect the scaffold to be callable.
- `trr_backend/socials/_scrapling_http_utils.py` - add shared response classification and bounded backoff helpers used by Scrapling lanes.
- `trr_backend/socials/instagram/posts_scrapling/fetcher.py` - adopt shared classification, add typed warmup errors, broader transport retry, and homepage redirect recovery.
- `trr_backend/socials/instagram/comments_scrapling/fetcher.py` - adopt shared classification for transport errors without changing the current public result shape.
- `trr_backend/socials/instagram/posts_scrapling/job_runner.py` - refresh final runtime metadata, handle DB-saturation summary fallback, and check cancellation between pages.
- `trr_backend/socials/instagram/comments_scrapling/job_runner.py` - check cancellation between target posts.
- `trr_backend/socials/instagram/scrapling_session.py` - create one shared auth-to-Scrapling session adapter.
- `trr_backend/socials/instagram/posts_scrapling/session.py` - re-export the shared adapter for compatibility.
- `trr_backend/socials/instagram/comments_scrapling/session.py` - re-export the shared adapter for compatibility.
- `docs/workspace/instagram-posts-scrapling.md` - document new failure modes and verification commands.
- `docs/workspace/instagram-comments-scrapling.md` - document cooperative cancellation and transport retry behavior.

Test:

- `tests/socials/instagram/runtimes/test_scrapling_runtime.py`
- `tests/socials/instagram/runtimes/test_dispatcher.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher.py`
- `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`
- `tests/socials/test_instagram_comments_scrapling.py`
- `tests/socials/instagram/test_scrapling_session.py`

## Implementation Tasks

### Task 1: Make the Scrapling Runtime Scaffold Safe to Dispatch

**Files:**
- Modify: `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`
- Modify: `trr_backend/socials/instagram/runtimes/__init__.py`
- Test: `tests/socials/instagram/runtimes/test_scrapling_runtime.py`
- Test: `tests/socials/instagram/runtimes/test_dispatcher.py`

- [ ] **Step 1: Write the failing runtime scaffold tests**

Add `tests/socials/instagram/runtimes/test_scrapling_runtime.py`:

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


def test_scrapling_runtime_methods_fall_through_in_dispatcher() -> None:
    runtime = ScraplingRuntime()

    with pytest.raises(RuntimeUnsupported, match="fetch_profile is not wired"):
        _run(runtime.fetch_profile("bravotv"))

    with pytest.raises(RuntimeUnsupported, match="fetch_posts is not wired"):
        _run(runtime.fetch_posts("bravotv", limit=3))

    with pytest.raises(RuntimeUnsupported, match="fetch_post_detail is not wired"):
        _run(runtime.fetch_post_detail("abc123"))
```

Append this test to `tests/socials/instagram/runtimes/test_dispatcher.py`:

```python
def test_dispatcher_skips_scrapling_scaffold_when_present() -> None:
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

- [ ] **Step 2: Run the scaffold tests to verify failure**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  -q
```

Expected: `test_scrapling_runtime_health_is_unsupported_scaffold` fails because `health.healthy` is currently `True`, and the method tests fail because the runtime raises `NotImplementedError`.

- [ ] **Step 3: Change the runtime scaffold to return unsupported instead of crashing**

In `trr_backend/socials/instagram/runtimes/scrapling_runtime.py`, replace the module docstring and method bodies with this shape:

```python
"""Scrapling-based Instagram runtime scaffold.

The production Scrapling lanes live under posts_scrapling/ and
comments_scrapling/. This pluggable runtime is not wired yet, so it must
be skipped by the dispatcher instead of reporting healthy and raising
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
        raise RuntimeUnsupported(
            f"ScraplingRuntime.fetch_posts is not wired for {username!r}, limit={limit}"
        )

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        raise RuntimeUnsupported(f"ScraplingRuntime.fetch_post_detail is not wired for {shortcode!r}")

    async def _fetch_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        raise RuntimeUnsupported(f"ScraplingRuntime._fetch_json is not wired for {url!r}")


assert isinstance(ScraplingRuntime(), InstagramRuntime)  # type: ignore[misc]
```

In `trr_backend/socials/instagram/runtimes/__init__.py`, replace the line that says scaffolded runtimes are `NotImplementedError` with:

```python
Runtimes that wrap external packages are scaffolded as unhealthy or
RuntimeUnsupported until their current-version APIs are verified via docs;
do not call them in production without completing the implementation plan
for each file.
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py \
  -q
```

Expected: `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add \
  trr_backend/socials/instagram/runtimes/scrapling_runtime.py \
  trr_backend/socials/instagram/runtimes/__init__.py \
  tests/socials/instagram/runtimes/test_scrapling_runtime.py \
  tests/socials/instagram/runtimes/test_dispatcher.py
git commit -m "fix: make instagram scrapling runtime scaffold unsupported"
```

### Task 2: Share Transport Classification Across Posts and Comments

**Files:**
- Modify: `trr_backend/socials/_scrapling_http_utils.py`
- Modify: `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- Test: `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`
- Test: `tests/socials/test_instagram_comments_scrapling_retry.py`

- [ ] **Step 1: Write failing transport-error tests**

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

- [ ] **Step 2: Run the new tests to verify failure**

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

Expected: failures from uncaught `httpx.ConnectError`.

- [ ] **Step 3: Add shared transport helpers**

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

- [ ] **Step 4: Use shared helpers in both fetchers**

In both `posts_scrapling/fetcher.py` and `comments_scrapling/fetcher.py`, add imports:

```python
from trr_backend.socials._scrapling_http_utils import transient_backoff_seconds as _transient_backoff_seconds
from trr_backend.socials._scrapling_http_utils import transport_failure_reason as _transport_failure_reason
```

In each `_fetch_json_response()` method, replace the current timeout-only except block:

```python
except (TimeoutError, httpx.TimeoutException):
    last_transient_reason = "transport_timeout"
    if attempt > self._MAX_TRANSIENT_RETRIES:
        return {
            "failed": True,
            "auth_failed": False,
            "reason": last_transient_reason,
            "retryable": True,
            "payload": None,
        }
    await asyncio.sleep(self._BASE_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    continue
```

with:

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

In each transient-status branch, replace the local exponential expression with:

```python
sleep_seconds = _transient_backoff_seconds(
    attempt=attempt,
    base_seconds=self._BASE_BACKOFF_SECONDS,
    retry_after=self._retry_after_seconds(response),
)
await asyncio.sleep(sleep_seconds)
```

- [ ] **Step 5: Run retry suites**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Expected: all selected retry tests pass.

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

### Task 3: Bring Posts Warmup and Redirect Recovery to Comments-Lane Parity

**Files:**
- Modify: `trr_backend/socials/instagram/posts_scrapling/fetcher.py`
- Test: `tests/socials/instagram/posts_scrapling/test_fetcher.py`
- Test: `tests/socials/instagram/posts_scrapling/test_fetcher_retry.py`

- [ ] **Step 1: Write failing warmup and homepage redirect tests**

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

    fetcher = InstagramPostsScraplingFetcher(
        cookies=[],
        raw_cookies={},
        browser_account_id="bravotv",
    )
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
        result = asyncio.run(fetcher._fetch_json_response(_URL, referer="https://www.instagram.com/bravotv/", data={}, headers={}))

    assert result["failed"] is False
    fetcher._recover_homepage_redirect.assert_awaited_once()
    assert fetcher._fetch_graphql.await_count == 2
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py::test_posts_warmup_raises_when_no_cookies_are_bridged \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py::test_redirect_to_homepage_recovers_once \
  -q
```

Expected: failures because `InstagramPostsWarmupError` and `_recover_homepage_redirect` do not exist.

- [ ] **Step 3: Add typed posts warmup error**

In `trr_backend/socials/instagram/posts_scrapling/fetcher.py`, add this class below `_auth_failure_text()`:

```python
class InstagramPostsWarmupError(RuntimeError):
    error_code: str
    retryable: bool

    def __init__(self, message: str, *, error_code: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retryable = retryable
```

In `warmup()`, replace the generic auth failure raise with:

```python
raise InstagramPostsWarmupError(
    "Instagram posts warmup failed because the session appears logged out or challenged.",
    error_code="instagram_posts_warmup_auth_failed",
    retryable=False,
)
```

After `self._merge_warmup_cookies(response)`, add:

```python
if not self._warmup_cookie_delta and not str(self._raw_cookies.get("sessionid") or "").strip():
    raise InstagramPostsWarmupError(
        "Instagram posts warmup did not bridge any cookies.",
        error_code="instagram_posts_warmup_no_cookies",
        retryable=True,
    )
```

- [ ] **Step 4: Add posts homepage redirect recovery**

In `posts_scrapling/fetcher.py`, add this method next to `_pace_api_requests()`:

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

At the start of `_fetch_json_response()`, set:

```python
homepage_redirect_recovery_attempted = False
```

In the 3xx branch, before returning the failure dict, add:

```python
auth_redirect = any(token in location for token in ("login", "challenge", "checkpoint"))
if reason == "redirect_to_homepage":
    if not homepage_redirect_recovery_attempted:
        homepage_redirect_recovery_attempted = True
        if await self._recover_homepage_redirect(referer=referer):
            continue
    auth_redirect = True
```

Then set the return field to:

```python
"auth_failed": auth_redirect,
```

- [ ] **Step 5: Run warmup and retry tests**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py \
  -q
```

Expected: selected posts fetcher tests pass.

- [ ] **Step 6: Commit**

```bash
git add \
  trr_backend/socials/instagram/posts_scrapling/fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher.py \
  tests/socials/instagram/posts_scrapling/test_fetcher_retry.py
git commit -m "fix: harden instagram posts scrapling warmup recovery"
```

### Task 4: Preserve Final Runtime Metadata and Add Cooperative Cancellation

**Files:**
- Modify: `trr_backend/socials/instagram/posts_scrapling/job_runner.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/job_runner.py`
- Test: `tests/socials/instagram/posts_scrapling/test_job_runner.py`
- Test: `tests/socials/test_instagram_comments_scrapling_retry.py`

- [ ] **Step 1: Write failing job-runner tests**

Append to `tests/socials/instagram/posts_scrapling/test_job_runner.py`:

```python
def test_job_runner_records_final_request_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    captured_finish: dict[str, object] = {}

    class _FakeFetcher:
        def __init__(self, **_kwargs) -> None:
            self.runtime_metadata = {"request_count": 1}

        async def warmup(self, _account_handle: str) -> None:
            self.runtime_metadata = {"request_count": 1}

        async def fetch_posts_page(self, _account_handle: str, cursor=None):
            self.runtime_metadata = {"request_count": 4}
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
            cookies={},
            browser_account_id="thetraitorsus",
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
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: {},
    )

    def _fake_finish_job(job_id, *, status, items_found, error_message=None, metadata=None, **_kwargs):
        captured_finish["metadata"] = metadata

    monkeypatch.setattr(repo, "_finish_job", _fake_finish_job)

    run_instagram_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus"}},
        worker_id="worker-1",
    )

    metadata = dict(captured_finish["metadata"] or {})
    assert metadata["fetcher_runtime"]["request_count"] == 4
```

Append to `tests/socials/test_instagram_comments_scrapling_retry.py`:

```python
def test_comments_job_runner_stops_when_job_is_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        runtime_metadata = {"request_count": 0}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, *, max_comments: int, fetch_replies: bool):
            return InstagramCommentsFetchResult(comments=[], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "select_comments_proxy", lambda session_key=None: None)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"job_status": "cancelled", "run_status": "running"})

    result = jr.run_instagram_comments_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {"account": "bravotv", "target_source_ids": ["abc123"]},
        },
        worker_id="worker-1",
    )

    assert finish_calls[-1]["status"] == "cancelled"
    assert result["status"] == "cancelled"
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_job_runner.py::test_job_runner_records_final_request_count \
  tests/socials/test_instagram_comments_scrapling_retry.py::test_comments_job_runner_stops_when_job_is_cancelled \
  -q
```

Expected: posts metadata assertion fails with stale `request_count`, and comments cancellation test fails because no cooperative check exists.

- [ ] **Step 3: Add lane-local cancellation helper to both job runners**

In both job runner files, add this dataclass beside the existing runtime error dataclass:

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

Add this helper to both job runner files:

```python
def _raise_if_cancelled(job_id: str, run_id: str | None, *, runtime_metadata: dict[str, Any] | None = None) -> None:
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
    )
    job_status = str((row or {}).get("job_status") or "").strip().lower()
    run_status = str((row or {}).get("run_status") or "").strip().lower()
    if job_status == "cancelled":
        raise ScraplingJobCancelled(job_id=job_id, run_id=run_id, cancel_scope="job", runtime_metadata=runtime_metadata)
    if run_status == "cancelled":
        raise ScraplingJobCancelled(job_id=job_id, run_id=run_id, cancel_scope="run", runtime_metadata=runtime_metadata)
```

In `comments_scrapling/job_runner.py`, call it at the top of each `for shortcode` loop:

```python
_raise_if_cancelled(job_id, run_id or None, runtime_metadata=dict(fetcher.runtime_metadata))
```

In `posts_scrapling/job_runner.py`, call it at the top of each page loop:

```python
_raise_if_cancelled(job_id, run_id or None, runtime_metadata=dict(fetcher.runtime_metadata))
```

- [ ] **Step 4: Handle cancellation as terminal cancelled**

In both `except Exception as exc` blocks, before calculating retry state, add:

```python
if isinstance(exc, ScraplingJobCancelled):
    repo._finish_job(
        job_id,
        status="cancelled",
        items_found=processed_posts + comments_fetched if "comments_fetched" in locals() else posts_fetched,
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
        "items_found": processed_posts + comments_fetched if "comments_fetched" in locals() else posts_fetched,
        "error_message": str(exc),
        "metadata": {"error_code": exc.error_code, "cancel_scope": exc.cancel_scope},
    }
```

For `posts_scrapling/job_runner.py`, also refresh metadata after every page and before successful return:

```python
fetcher_metadata = dict(fetcher.runtime_metadata)
```

Place it immediately after the `result = await fetcher.fetch_posts_page(account_handle, cursor=cursor)` call and again just before `return auth_metadata, fetcher_metadata`.

- [ ] **Step 5: Add degraded DB summary fallback to posts runner**

At the bottom of `posts_scrapling/job_runner.py`, replace the final database row return with the same try/except shape already used by comments:

```python
try:
    return (
        pg.fetch_one(
            """
            select
              id::text,
              run_id::text as run_id,
              platform,
              job_type,
              status,
              items_found,
              error_message,
              metadata
            from social.scrape_jobs
            where id = %s
            """,
            [job_id],
        )
        or {}
    )
except pg.DatabaseServiceUnavailableError as exc:
    return {
        "id": job_id,
        "run_id": run_id or None,
        "platform": "instagram",
        "job_type": str(job.get("job_type") or "posts").strip() or "posts",
        "status": terminal_status or "unknown",
        "items_found": posts_fetched,
        "error_message": terminal_error_message,
        "metadata": {
            "degraded_summary": True,
            "database_service_unavailable": True,
            "database_error_class": exc.__class__.__name__,
        },
    }
```

Add `terminal_status` and `terminal_error_message` near the top of the function, matching comments:

```python
terminal_status = str(job.get("status") or "").strip().lower() or None
terminal_error_message: str | None = None
```

Set `terminal_status = "completed"` after successful `_finish_job()`, and set `terminal_status` / `terminal_error_message` in the generic exception block.

- [ ] **Step 6: Run job-runner tests**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py \
  -q
```

Expected: selected job-runner tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  trr_backend/socials/instagram/posts_scrapling/job_runner.py \
  trr_backend/socials/instagram/comments_scrapling/job_runner.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/socials/test_instagram_comments_scrapling_retry.py
git commit -m "fix: make instagram scrapling jobs cancellable and observable"
```

### Task 5: Deduplicate Scrapling Session Cookie Adapters

**Files:**
- Create: `trr_backend/socials/instagram/scrapling_session.py`
- Modify: `trr_backend/socials/instagram/posts_scrapling/session.py`
- Modify: `trr_backend/socials/instagram/comments_scrapling/session.py`
- Test: `tests/socials/instagram/test_scrapling_session.py`

- [ ] **Step 1: Write failing shared-session tests**

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
    from trr_backend.socials.instagram.auth_resolver import InstagramAuthSession
    from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
    from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

    auth_session = InstagramAuthSession(
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

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest tests/socials/instagram/test_scrapling_session.py -q
```

Expected: import failure because `scrapling_session.py` does not exist.

- [ ] **Step 3: Create the shared adapter**

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

Replace `posts_scrapling/session.py` with:

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

Replace `comments_scrapling/session.py` with:

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

- [ ] **Step 4: Run session tests**

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

### Task 6: Update Runbooks and Add Current Scrapling Verification Commands

**Files:**
- Modify: `docs/workspace/instagram-posts-scrapling.md`
- Modify: `docs/workspace/instagram-comments-scrapling.md`

- [ ] **Step 1: Update posts runbook**

In `docs/workspace/instagram-posts-scrapling.md`, add this under `## Failure modes and recovery`:

```markdown
| Symptom | Likely cause | Action |
|---|---|---|
| `instagram_posts_warmup_no_cookies` | Scrapling warmup returned a page but did not bridge any usable cookies and no prior `sessionid` existed | Refresh the Instagram browser session, rerun `./scripts/setup_scrapling.sh` if browser deps are stale, then rerun a one-page smoke |
| `redirect_to_homepage` after one recovery attempt | Instagram redirected the GraphQL call to the profile/home surface and the second browser warmup did not restore an API-usable session | Treat as auth failure; refresh cookies and reduce concurrent jobs for that account |
| `transport_error` with `retryable: true` | httpx connect/read/proxy transport failure after browser warmup | Let queue retry once; if repeated, inspect proxy health and `selected_proxy_fingerprint` in job metadata |
| Job row is `cancelled` while worker logs still show active fetches | Worker predates cooperative cancellation hardening or is inside one in-flight API call | After this plan lands, workers check cancellation between pages and target posts; restart old worker processes |
```

Add this under `## Observability`:

```markdown
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
```

- [ ] **Step 2: Update comments runbook**

In `docs/workspace/instagram-comments-scrapling.md`, add this under `## Known failure modes and remediation`:

```markdown
### Cooperative cancellation

The comments worker checks `social.scrape_jobs.status` and the linked
`social.scrape_runs.status` between target posts. A cancellation request
does not interrupt an in-flight Instagram API call, but it stops before the
next shortcode is fetched and finishes the job as `cancelled` with
`metadata.error_code = "instagram_scrapling_job_cancelled"`.

### Retryable transport errors

`httpx.TimeoutException`, Python `TimeoutError`, and `httpx.TransportError`
are classified as retryable transport failures. The stable reasons are
`transport_timeout` and `transport_error`. Repeated `transport_error`
failures usually point to proxy connectivity or local network interruption,
not parser drift.
```

- [ ] **Step 3: Run docs lint surface**

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

### Task 7: Final Verification

**Files:**
- Verify all files touched in Tasks 1-6.

- [ ] **Step 1: Run focused Instagram Scrapling tests**

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

- [ ] **Step 2: Run Ruff on touched Python files**

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

- [ ] **Step 3: Run formatter check on touched Python files**

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

- [ ] **Step 4: Verify no cookie values leak through runtime metadata**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python -m pytest \
  tests/socials/instagram/posts_scrapling/test_fetcher.py::test_runtime_metadata_never_exposes_cookie_values \
  tests/socials/test_instagram_comments_scrapling.py::test_comments_fetcher_runtime_metadata_never_exposes_cookie_values \
  -q
```

Expected: `2 passed`.

- [ ] **Step 5: Verify git diff is clean of whitespace errors**

Run:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
git diff --check
```

Expected: no output.

## Self-Review

Spec coverage:

- Audit of `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram` is reflected in the audit summary and file structure.
- Scrapling skill guidance is reflected in the focus on `StealthyFetcher`, protected-site warmup, sessions/cookies, transport classification, and MCP/session runbook verification.
- Fix/improvement plan is broken into TDD tasks with exact files, test commands, expected failures, implementation snippets, validation, docs, and commit points.

Placeholder scan:

- No deferred implementation markers are present.
- Each code-changing task includes concrete code snippets and exact commands.

Type consistency:

- `ScraplingJobCancelled`, `InstagramPostsWarmupError`, `InstagramScraplingSession`, and `RuntimeUnsupported` names are introduced before use.
- Job metadata keys use the existing `fetcher_runtime`, `runtime_metadata`, `error_code`, `activity`, and `last_error_code` patterns already present in the repository.
