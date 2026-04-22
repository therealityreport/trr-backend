#!/usr/bin/env python3
"""Run the Instagram comments fetch path inside Modal and print a compact diagnostic."""

from __future__ import annotations

import argparse
import json

import modal

from trr_backend.modal_jobs import _FUNCTION_IMAGE_BINDINGS, _secrets

app = modal.App("trr-instagram-comments-diagnostic")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", default="thetraitorsus", help="Instagram handle used for browser/account context")
    parser.add_argument("--shortcode", required=True, help="Instagram post shortcode to probe")
    return parser.parse_args()


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    timeout=5 * 60,
)
def diagnose_remote_comments(shortcode: str, account_handle: str) -> dict[str, object]:
    import asyncio

    from trr_backend.socials.instagram.comments_scrapling.fetcher import (
        InstagramCommentsScraplingFetcher,
        _response_text,
        _safe_location,
        _status_code,
    )
    from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
    from trr_backend.socials.instagram.constants import COMMENTS_URL
    from trr_backend.socials.instagram.permalink_metadata import _shortcode_to_media_id

    async def _run() -> dict[str, object]:
        session = resolve_comments_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"comments_scrapling:profile:{account_handle}",
        )
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=None,
        )
        payload: dict[str, object] = {
            "auth_session": {
                "source": session.auth_session.source,
                "validated": session.auth_session.validated,
                "validation_reason": session.auth_session.validation_reason,
                "validation_category": session.auth_session.validation_category,
                "browser_account_id": session.auth_session.browser_account_id,
                "session_account_id": session.auth_session.session_account_id,
                "cookie_count": len(session.auth_session.cookies),
                "fingerprint": (session.auth_session.metadata or {}).get("fingerprint"),
            }
        }
        try:
            await fetcher.warmup()
            payload["warmup"] = {
                "ok": True,
                "runtime": dict(fetcher.runtime_metadata),
            }
            media_id = _shortcode_to_media_id(shortcode)
            response = await fetcher._fetch_api(
                COMMENTS_URL.format(media_id=media_id),
                referer=f"https://www.instagram.com/p/{shortcode}/",
                params={"can_support_threading": "true", "permalink_enabled": "false"},
            )
            payload["api"] = {
                "status_code": _status_code(response),
                "location": _safe_location(response),
                "text_prefix": _response_text(response)[:200],
                "headers_subset": {
                    "content_type": response.headers.get("content-type"),
                    "location": response.headers.get("location"),
                },
            }
            payload["parsed"] = await fetcher._fetch_json_response(
                COMMENTS_URL.format(media_id=media_id),
                referer=f"https://www.instagram.com/p/{shortcode}/",
                params={"can_support_threading": "true", "permalink_enabled": "false"},
            )
        except Exception as exc:  # noqa: BLE001
            payload["error"] = {
                "class": type(exc).__name__,
                "message": str(exc),
            }
        finally:
            await fetcher.aclose()
        return payload

    return asyncio.run(_run())


def main() -> int:
    args = parse_args()
    with app.run():
        payload = diagnose_remote_comments.remote(args.shortcode, args.account)
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
