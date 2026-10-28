#!/usr/bin/env python3
"""Bounded, evaluation-only Modal comparison; never part of the production app.

Run: MODAL_PROFILE=admin-56995 MODAL_ENVIRONMENT=main .venv/bin/python
     scripts/modal/evaluate_fetcher_session.py --run
No secrets are attached. Output contains counts and timings, never response bodies.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib.metadata
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

_SOURCE_PATH = Path(__file__).resolve()
ROOT = _SOURCE_PATH.parents[2] if len(_SOURCE_PATH.parents) > 2 else _SOURCE_PATH.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class EvaluationStop(BaseException):
    """Cannot be swallowed by scraper retry/recovery handlers."""


class SampleComplete(BaseException):
    pass


class Budget:
    def __init__(self):
        self.count = 0
        self.records = []
        self.arm = ""
        self.phase = ""
        self.last_request = 0.0

    def before(self, method, url, headers, cookies):
        from trr_backend.socials.instagram.credential_guard import PublicCookieIdentity

        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname != "www.instagram.com" or parsed.username or parsed.password:
            raise EvaluationStop("origin_policy")
        if any(key.lower() in {"authorization", "proxy-authorization", "cookie"} for key in headers):
            raise EvaluationStop("credential_header_policy")
        try:
            PublicCookieIdentity({}).validate_snapshot(dict(cookies))
        except Exception:
            raise EvaluationStop("credential_cookie_policy") from None
        if self.count >= 60:
            raise EvaluationStop("request_budget")
        time.sleep(max(0, 2 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        self.count += 1
        return time.monotonic()

    def after(self, response, started):
        import requests

        from trr_backend.socials.instagram.public_response import PublicRequestClient

        status = response.status_code
        self.records.append(
            {
                "arm": self.arm,
                "phase": self.phase,
                "status": status,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "bytes": len(response.content),
            }
        )
        # Existing public classifier rejects challenge/login/throttle responses.
        try:
            # The classifier only reads the response; the session is never used for I/O.
            with requests.Session() as classifier_session:
                PublicRequestClient(session=classifier_session)._classify_response(response)
        except Exception:
            raise EvaluationStop("public_response_blocked") from None
        if status >= 300:
            raise EvaluationStop("http_status_" + str(status))


def _lock_curl(client, *, asynchronous=False):
    inner = getattr(client, "_async_curl_session" if asynchronous else "_curl_session", None)
    if inner is None:
        raise EvaluationStop("curl_session_unavailable")
    from curl_cffi import CurlOpt

    inner.trust_env = False
    inner.curl_options[CurlOpt.PROXY] = ""
    inner.proxies = {}
    inner.auth = None
    inner.cookies.clear()
    return inner


def _fetcher_options():
    return {"timeout": 45, "follow_redirects": False, "retries": 1, "stealthy_headers": False, "impersonate": "chrome"}


def _requests_response(raw, url):
    import requests

    result = requests.Response()
    result.status_code = raw.status
    result._content = raw.body
    result.headers.update(dict(raw.headers))
    result.url = str(raw.url or url)
    return result


@contextlib.contextmanager
def sync_transport(arm, budget):
    import requests
    from scrapling.fetchers import FetcherSession

    from trr_backend.socials.instagram.credential_guard import PublicCookieIdentity

    with contextlib.ExitStack() as stack:
        native = stack.enter_context(requests.Session())
        native.trust_env = False
        native.auth = None
        native.proxies.clear()
        curl = stack.enter_context(FetcherSession(**_fetcher_options())) if arm == "fetcher_session" else None
        inner = _lock_curl(curl) if curl else None
        guard = PublicCookieIdentity({})
        original = native.request

        def request(method, url, **kwargs):
            if kwargs.get("auth") or kwargs.get("proxies"):
                raise EvaluationStop("credential_transport_policy")
            headers = dict(native.headers) | dict(kwargs.get("headers") or {})
            try:
                cookies = guard.prepare_jar(native.cookies, kwargs.get("cookies") or {})
                if inner:
                    cookies = guard.prepare_jar(inner.cookies, cookies)
            except Exception:
                raise EvaluationStop("credential_cookie_policy") from None
            started = budget.before(method, url, headers, cookies)
            if curl:
                raw = getattr(curl, method.lower())(
                    url,
                    headers=headers,
                    cookies=cookies,
                    params=kwargs.get("params") or {},
                    **({"data": kwargs["data"]} if "data" in kwargs else {}),
                )
                response = _requests_response(raw, url)
                try:
                    assert inner is not None
                    native.cookies.update(guard.prepare_jar(inner.cookies, cookies))
                except Exception:
                    raise EvaluationStop("credential_cookie_policy") from None
            else:
                kwargs.update(
                    headers=headers, cookies=cookies, allow_redirects=False, timeout=45, proxies={}, auth=None
                )
                response = original(method, url, **kwargs)
            budget.after(response, started)
            try:
                guard.prepare_jar(native.cookies, cookies)
            except Exception:
                raise EvaluationStop("credential_cookie_policy") from None
            return response

        native.request = request  # pyright: ignore[reportAttributeAccessIssue]
        yield native


class AsyncTransport:
    def __init__(self, arm, budget, **kwargs):
        import httpx

        self.arm, self.budget = arm, budget
        self.cookies = httpx.Cookies()
        self.headers = dict(kwargs.get("headers") or {})
        self.posts = 0
        self.stack = contextlib.AsyncExitStack()

    async def __aenter__(self):
        import httpx
        from scrapling.fetchers import FetcherSession

        if self.arm == "fetcher_session":
            self.client = await self.stack.enter_async_context(FetcherSession(**_fetcher_options()))
            self.inner = _lock_curl(self.client, asynchronous=True)
        else:
            self.client = await self.stack.enter_async_context(
                httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=45, headers=self.headers)
            )
            self.inner = self.client
        return self

    async def __aexit__(self, *args):
        return await self.stack.__aexit__(*args)

    async def get(self, url, **kwargs):
        return await self.request("get", url, **kwargs)

    async def post(self, url, **kwargs):
        self.posts += 1
        if self.posts > 2:
            raise SampleComplete()
        return await self.request("post", url, **kwargs)

    async def request(self, method, url, **kwargs):
        import httpx

        from trr_backend.socials.instagram.credential_guard import PublicCookieIdentity

        guard = PublicCookieIdentity({})
        try:
            cookies = guard.prepare_jar(self.cookies, kwargs.pop("cookies", {}))
            cookies = guard.prepare_jar(self.inner.cookies, cookies)
        except Exception:
            raise EvaluationStop("credential_cookie_policy") from None
        headers = self.headers | dict(kwargs.pop("headers", {}))
        started = self.budget.before(method, url, headers, cookies)
        response = await getattr(self.client, method)(url, headers=headers, cookies=cookies, **kwargs)
        if self.arm == "fetcher_session":
            response = httpx.Response(
                response.status,
                headers=dict(response.headers),
                content=response.body,
                request=httpx.Request(method, url),
            )
        self.budget.after(response, started)
        try:
            self.cookies.update(guard.prepare_jar(self.inner.cookies, cookies))
        except Exception:
            raise EvaluationStop("credential_cookie_policy") from None
        return response


async def comments_sample(shortcode, arm, budget):
    import httpx

    from trr_backend.socials.instagram.comments_scrapling import fetcher as module

    original = module.httpx
    module.httpx = SimpleNamespace(
        **(vars(httpx) | {"AsyncClient": lambda **kwargs: AsyncTransport(arm, budget, **kwargs)})
    )
    try:
        fetcher = module.InstagramCommentsScraplingFetcher(
            cookies=[], raw_cookies={}, browser_account_id=None, public_only=True
        )
        comments, metadata = await fetcher._fetch_public_relay_coauthor_comments_for_status_only(
            shortcode,
            f"https://www.instagram.com/p/{shortcode}/",
            max_comments=30,
            allow_authenticated=False,
            use_proxy=False,
            fetch_replies=False,
            deadline=time.monotonic() + 120,
        )
        return {"count": len(comments), "reason": str(metadata.get("reason") or "complete")}
    except SampleComplete:
        return {"reason": "two_page_sample_cap"}
    finally:
        module.httpx = original


def evaluate():
    # This process is dedicated to evaluation and receives no Modal secrets.
    for key in list(os.environ):
        if key.lower().endswith("_proxy") or any(
            token in key.upper() for token in ("INSTAGRAM_COOKIE", "INSTAGRAM_SESSION", "INSTAGRAM_PASSWORD")
        ):
            os.environ.pop(key, None)
    os.environ["SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE"] = "file_lock"
    from trr_backend.socials.instagram.permalink_metadata import _DEFAULT_HEADERS, fetch_permalink_media_item
    from trr_backend.socials.instagram.public_probe import _posts_from_public_graphql_payload
    from trr_backend.socials.instagram.scraper import InstagramScraper

    budget = Budget()
    output = {
        "environment": "main",
        "workspace": "admin-56995",
        "request_limit": 60,
        "scrapling": importlib.metadata.version("scrapling"),
        "production_adoption": False,
        "arms": {},
        "records": budget.records,
    }
    sample_shortcodes = []
    try:
        for arm in ("baseline", "fetcher_session"):
            budget.arm = arm
            summary = {
                "impersonation": "chrome" if arm == "fetcher_session" else None,
                "listing_pages": 0,
                "details": 0,
                "comments": [],
            }
            output["arms"][arm] = summary
            with sync_transport(arm, budget) as session:
                scraper = InstagramScraper(
                    cookies={}, session=session, attach_auth_session=False, strict_public_credentials=True
                )
                cursor = None
                posts = []
                for _ in range(2):
                    budget.phase = "listing"
                    payload = scraper.fetch_posts_graphql(
                        "bravotv",
                        cursor=cursor,
                        delay=2,
                        allow_browser_fallback=False,
                        allow_recovery=False,
                        request_timeout=45,
                    )
                    rows, page_info, found = _posts_from_public_graphql_payload(payload)
                    if not found:
                        summary["listing_reason"] = "unresolved"
                        break
                    summary["listing_pages"] += 1
                    posts.extend(rows)
                    cursor = page_info.get("end_cursor")
                    if not page_info.get("has_next_page") or not cursor:
                        break
                if arm == "baseline":
                    sample_shortcodes = list(
                        dict.fromkeys(str(row.get("shortcode") or row.get("code") or "") for row in posts)
                    )[:3]
                for shortcode in sample_shortcodes:
                    if not shortcode:
                        continue
                    budget.phase = "detail"

                    class Transport:
                        def check_deadline(self):
                            pass

                        def request(self, url, *, text=False):
                            response = session.get(url, headers=_DEFAULT_HEADERS)
                            return response.text if text else response.json()

                    media = fetch_permalink_media_item(shortcode, session=session, cookies={}, transport=Transport())
                    summary["details"] += int(isinstance(media, dict))
                    budget.phase = "comments"
                    summary["comments"].append(asyncio.run(comments_sample(shortcode, arm, budget)))
        output["outcome"] = (
            "completed_samples" if all(v["listing_pages"] for v in output["arms"].values()) else "inconclusive"
        )
    except EvaluationStop as exc:
        output["outcome"] = "inconclusive"
        output["stop_reason"] = str(exc)
    except Exception as exc:
        output["outcome"] = "inconclusive"
        output["stop_reason"] = "runtime_" + type(exc).__name__
    output["request_count"] = budget.count
    return output


# Construct the ephemeral app only in Modal's module import. Unit tests exercise
# transport policy without initializing the large production image graph.
if os.environ.get("TRR_FETCHER_EVAL_MODAL") == "1":
    import modal

    from trr_backend.modal_jobs import _build_social_image_base

    app = modal.App("trr-fetcher-eval")
    probe = app.function(image=_build_social_image_base(include_browser_runtime=True), timeout=900, max_containers=1)(
        evaluate
    )

    @app.local_entrypoint()
    def main():
        print(json.dumps(probe.remote(), sort_keys=True))


if __name__ == "__main__" and "--run" in sys.argv:
    from scripts.modal.deploy_backend import pinned_modal_env, verify_required_workspace

    env = pinned_modal_env()
    verify_required_workspace(env=env)
    env["TRR_FETCHER_EVAL_MODAL"] = "1"
    subprocess.run([sys.executable, "-m", "modal", "run", "--env", "main", __file__], env=env, cwd=ROOT, check=True)
