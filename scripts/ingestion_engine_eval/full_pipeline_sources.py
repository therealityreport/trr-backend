"""Shared deterministic external-I/O fixtures for the frozen resource probe."""

from __future__ import annotations

import hashlib
import io
import json
import os
import socket
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from full_pipeline_resource import ACCOUNT, CDN, CODES, OBSERVED


def event(channel, values):
    """One bounded O_APPEND record per operation; shared across private children."""
    row = {"pid": os.getpid(), "activity": os.getenv("EVAL_ACTIVITY_ID", "driver"), **values}
    path = Path(os.environ["EVAL_RUNTIME_ROOT"]) / (channel + ".jsonl")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        body = (json.dumps(row, separators=(",", ":")) + "\n").encode()
        assert len(body) < 4096
        assert os.write(descriptor, body) == len(body)
    finally:
        os.close(descriptor)


class Trace(list):
    def append(self, value):
        event("transport-events", value)
        super().append(value)


class Storage:
    """File-backed fake object service shared by serial local worker children."""

    def __init__(self):
        self.root = Path(os.environ["EVAL_RUNTIME_ROOT"]) / "objects"
        self.root.mkdir(exist_ok=True)
        self.calls = Counter()

    def record(self, operation, key=None, *, outcome="ok", size=None):
        self.calls[operation] += 1
        event("storage-events", {"operation": operation, "key": key, "outcome": outcome, "bytes": size})

    def path(self, key):
        return self.root / hashlib.sha256(key.encode()).hexdigest()

    def head_object(self, *, Bucket, Key):  # noqa: N803
        from botocore.exceptions import ClientError

        path = self.path(Key)
        if not path.exists():
            self.record("head", Key, outcome="not_found")
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        row = json.loads(path.read_text())
        self.record("head", Key, size=len(bytes.fromhex(row["data"])))
        return {"ContentLength": len(bytes.fromhex(row["data"])), "Metadata": row["metadata"]}

    def put_object(self, **kwargs):
        return self.store("put", kwargs)

    def store(self, operation, kwargs):
        from botocore.exceptions import ClientError

        path = self.path(kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and path.exists():
            self.record(operation, kwargs["Key"], outcome="precondition_failed")
            raise ClientError(
                {"Error": {"Code": "PreconditionFailed"}, "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject"
            )
        body = kwargs["Body"]
        body = body.read() if hasattr(body, "read") else body
        path.write_text(json.dumps({"key": kwargs["Key"], "data": body.hex(), "metadata": kwargs.get("Metadata", {})}))
        self.record(operation, kwargs["Key"], size=len(body))
        return {"ETag": "fixture"}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        from botocore.exceptions import ClientError

        if not self.path(Key).exists():
            self.record("get", Key, outcome="not_found")
            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        body = bytes.fromhex(json.loads(self.path(Key).read_text())["data"])
        self.record("get", Key, size=len(body))
        return {"Body": io.BytesIO(body)}

    def upload_fileobj(self, fileobj, bucket, key, ExtraArgs=None):  # noqa: N803
        self.store("upload_fileobj", {"Bucket": bucket, "Key": key, "Body": fileobj, **(ExtraArgs or {})})

    def close(self):
        self.record("close")


def post(code, *, detail=True):
    detail = detail or os.getenv("EVAL_STABLE_SOURCE") == "1"
    return {
        "code": code,
        "shortcode": code,
        "pk": str(100001 + CODES.index(code)),
        "taken_at": int(OBSERVED.timestamp()),
        "media_type": 1,
        "owner": {"pk": "100", "username": ACCOUNT, "profile_pic_url": CDN + "/owner.png"},
        "user": {"pk": "100", "username": ACCOUNT, "profile_pic_url": CDN + "/owner.png"},
        "caption": {"text": ("Detail " if detail else "Listed ") + code},
        "like_count": 7 if detail else 2,
        "comment_count": 2,
        "display_url": CDN + "/" + code + ".png",
        "image_versions2": {"candidates": [{"url": CDN + "/" + code + ".png", "width": 120, "height": 80}]},
        "original_width": 120,
        "original_height": 80,
    }


def comments(code):
    return {
        "status": "ok",
        "comments": [
            {
                "pk": code + "-parent",
                "text": "Parent " + code,
                "created_at": int(OBSERVED.timestamp()),
                "comment_like_count": 3,
                "child_comment_count": 1,
                "user": {"pk": "101", "username": "reader", "profile_pic_url": CDN + "/reader.png"},
                "comment_gif": {"url": CDN + "/" + code + "-comment.png"},
            }
        ],
        "comment_count": 2,
        "has_more_comments": False,
        "has_more_headload_comments": False,
    }


def replies(code):
    return {
        "status": "ok",
        "parent_comment_id": code + "-parent",
        "child_comments": [
            {
                "pk": code + "-reply",
                "text": "Reply " + code,
                "created_at": int(OBSERVED.timestamp()),
                "comment_like_count": 1,
                "child_comment_count": 0,
                "user": {"pk": "102", "username": "replier", "profile_pic_url": CDN + "/replier.png"},
            }
        ],
        "has_more_tail_child_comments": False,
    }


def listing(cursor):
    from trr_backend.socials.instagram.constants import PROFILE_POSTS_ROOT_FIELD_NAME

    assert cursor in (None, "", "after-first"), cursor
    codes = CODES[:6] if not cursor else CODES[6:]
    return {
        "data": {
            PROFILE_POSTS_ROOT_FIELD_NAME: {
                "edges": [{"node": post(code, detail=False)} for code in codes],
                "page_info": {"has_next_page": not cursor, "end_cursor": "after-first" if not cursor else None},
            }
        }
    }


def asset_identities():
    return [
        *(code + ".png" for code in CODES),
        *(code + "-comment.png" for code in CODES),
        "owner.png",
        "reader.png",
        "replier.png",
    ]


def png(asset="owner.png"):
    from PIL import Image

    output = io.BytesIO()
    identities = asset_identities()
    if asset not in identities:
        raise AssertionError("Unknown fixture asset identity")
    seed = hashlib.sha256(("full-resource-assets-v3:" + asset).encode()).digest()
    body = Image.new("RGB", (120, 80), tuple(seed[:3]))
    # A stable small pattern preserves similar dimensions/format without making
    # every original or derivative have the same bytes.
    for index, value in enumerate(seed):
        body.putpixel((index % 120, index // 120), (value, seed[(index + 1) % 32], seed[(index + 2) % 32]))
    body.save(output, "PNG")
    return output.getvalue()


def fixture_manifest(root):
    """Seal every source identity/body before the measured interval."""
    path = root / "fixture-assets"
    path.mkdir()
    rows = []
    for asset in asset_identities():
        body = png(asset)
        (path / asset).write_bytes(body)
        rows.append(
            {
                "url": CDN + "/" + asset,
                "identity": asset,
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "width": 120,
                "height": 80,
                "content_type": "image/png",
            }
        )
    assert len(rows) == len({row["sha256"] for row in rows}) == 27
    return {
        "version": "distinct-assets-v3",
        "assets": rows,
        "posts": [post(code) for code in CODES],
        "comments": {code: {"parent": comments(code), "reply": replies(code)} for code in CODES},
        "exact_url_cover_media_reuse": True,
        "expected_physical_objects": 147,
    }


def install_sources(*, legacy_mode=False, stage="all"):
    """Replace transport/session boundaries only, never SQL or business guards."""
    import httpx
    import requests

    scraper = None
    if stage != "media":
        from trr_backend.socials.instagram import auth_resolver, scraper

    trace = Trace()
    original_dns = socket.getaddrinfo

    def dns(host, port, *args, **kwargs):
        if host in ("scontent.cdninstagram.com", "media.example"):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port or 443))]
        if host not in ("localhost", "127.0.0.1", "::1"):
            raise RuntimeError("Evaluation denies external DNS")
        return original_dns(host, port, *args, **kwargs)

    socket.getaddrinfo = dns
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    media_ids = {}
    for code in CODES:
        value = 0
        for char in code:
            value = value * 64 + alphabet.index(char)
        media_ids[str(value)] = code
        media_ids[str(100001 + CODES.index(code))] = code

    detail_paths = {f"/api/v1/media/{key}/info/": code for key, code in media_ids.items()}
    comment_paths = {f"/api/v1/media/{key}/comments/": (code, False) for key, code in media_ids.items()}
    comment_paths.update(
        {f"/api/v1/media/{key}/comments/{code}-parent/child_comments/": (code, True) for key, code in media_ids.items()}
    )

    def respond(request):
        assert request.url.host in {"www.instagram.com", "i.instagram.com"}, "Unknown HTTP fixture host"
        path = request.url.path
        if path.rstrip("/") == "/graphql/query":
            body = request.content.decode()
            from urllib.parse import parse_qs

            values = parse_qs(body)
            variables = json.loads(values.get("variables", ["{}"])[0])
            cursor = variables.get("after") or variables.get("data", {}).get("after")
            trace.append({"kind": "listing", "cursor": cursor})
            return httpx.Response(200, json=listing(cursor), request=request)
        if path in comment_paths:
            code, is_reply = comment_paths[path]
            trace.append({"kind": "reply" if is_reply else "comments", "code": code})
            return httpx.Response(200, json=replies(code) if is_reply else comments(code), request=request)
        raise AssertionError("Unexpected HTTP fixture operation")

    original_client = httpx.AsyncClient

    class Client(original_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(respond)
            kwargs.pop("proxy", None)
            kwargs["trust_env"] = False
            super().__init__(*args, **kwargs)

    httpx.AsyncClient = Client

    class Browser:
        @classmethod
        async def async_fetch(cls, url, **kwargs):
            from urllib.parse import urlsplit

            parts = urlsplit(url)
            assert parts.hostname == "www.instagram.com" and parts.path.strip("/") == ACCOUNT
            trace.append({"kind": "browser_warmup"})
            text = '<html>"LSD",[],{"token":"fixture"} "id":"100" "username":"resource_fixture"</html>'
            return httpx.Response(200, text=text, request=httpx.Request("GET", url))

    if stage in {"all", "listing"}:
        from trr_backend.socials.instagram.posts_scrapling import fetcher as post_fetcher

        post_fetcher.build_stealthy_fetcher = lambda *a, **k: Browser
    if stage in {"all", "comments"}:
        from trr_backend.socials.instagram.comments_scrapling import fetcher as comment_fetcher

        comment_fetcher.build_stealthy_fetcher = lambda *a, **k: Browser
    cookies = {"sessionid": "synthetic-fixture", "ds_user_id": "100", "csrftoken": "fixture"}
    session = SimpleNamespace(
        cookies=[{"name": k, "value": v, "domain": ".instagram.com", "path": "/"} for k, v in cookies.items()],
        browser_account_id=ACCOUNT,
        auth_session=SimpleNamespace(
            cookies=cookies,
            metadata={},
            source="fixture",
            browser_account_id=ACCOUNT,
            session_account_id=ACCOUNT,
            validated=True,
            validation_reason=None,
            validation_category="fixture",
            stale_ok=False,
            cookie_file_path=None,
        ),
    )
    if legacy_mode:
        from trr_backend.socials.instagram.comments_scrapling import job_runner as comment_runner
        from trr_backend.socials.instagram.posts_scrapling import job_runner as post_runner

        comment_runner.resolve_comments_scrapling_session = lambda **kwargs: session
        post_runner.resolve_posts_scrapling_session = lambda **kwargs: session
        comment_runner.select_comments_proxy = lambda **kwargs: None
        post_runner.select_posts_proxy = lambda **kwargs: None
    # No credential resolver may read browser/profile files during construction.
    if stage != "media":
        auth_resolver.resolve_instagram_auth_session = lambda **kwargs: session.auth_session
        auth_resolver.resolve_instagram_comments_auth_session = lambda **kwargs: session.auth_session
        scraper.resolve_instagram_auth_session = lambda **kwargs: session.auth_session

    def get(self, url, **kwargs):
        from urllib.parse import urlsplit

        parts = urlsplit(url)
        assert parts.scheme == "https" and parts.hostname in {
            "www.instagram.com",
            "i.instagram.com",
            "scontent.cdninstagram.com",
        }, "Unknown request fixture host"
        if parts.path == "/api/v1/users/web_profile_info/":
            from urllib.parse import parse_qs

            assert parse_qs(parts.query) == {"username": [ACCOUNT]}
            trace.append({"kind": "profile_info"})
            response = requests.Response()
            response.status_code = 200
            response._content = json.dumps(
                {
                    "data": {
                        "user": {
                            "id": "100",
                            "username": ACCOUNT,
                            "profile_pic_url": CDN + "/owner.png",
                            "edge_owner_to_timeline_media": {
                                "count": 12,
                                "edges": [],
                                "page_info": {"has_next_page": True},
                            },
                        }
                    }
                }
            ).encode()
            response.headers["Content-Type"] = "application/json"
            response.url = url
            return response
        if url.rstrip("/") == "https://www.instagram.com/" + ACCOUNT:
            trace.append({"kind": "browser_warmup"})
            response = requests.Response()
            response.status_code = 200
            response._content = b'<html>"LSD",[],{"token":"fixture"} "id":"100" "username":"resource_fixture"</html>'
            response.headers["Content-Type"] = "text/html"
            response.url = url
            return response
        if url.startswith(CDN):
            asset = url.removeprefix(CDN + "/")
            assert asset in asset_identities(), "Unknown CDN fixture URL"
            trace.append({"kind": "cdn", "asset": asset})
            response = requests.Response()
            response.status_code = 200
            sealed = Path(os.environ["EVAL_RUNTIME_ROOT"]) / "fixture-assets" / asset
            response._content = sealed.read_bytes()
            assert response._content == png(asset), "Fixture asset drift"
            response.url = url
            response.headers["Content-Type"] = "image/png"
            response.raw = io.BytesIO(response._content)
            return response
        assert parts.path in detail_paths and not parts.query, "Unknown detail fixture path"
        code = detail_paths[parts.path]
        trace.append({"kind": "details", "code": code})
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({"items": [post(code)]}).encode()
        response.headers["Content-Type"] = "application/json"
        return response

    requests.Session.get = get

    def request(self, method, url, **kwargs):
        if method.upper() == "GET":
            return get(self, url, **kwargs)
        assert method.upper() == "POST", "Unexpected fixture request"
        from urllib.parse import urlsplit

        assert urlsplit(url).hostname == "www.instagram.com", "Unknown GraphQL fixture host"
        assert urlsplit(url).path.rstrip("/") == "/graphql/query", "Unknown GraphQL fixture path"
        variables = json.loads(kwargs.get("data", {}).get("variables", "{}"))
        cursor = variables.get("after")
        trace.append({"kind": "listing", "cursor": cursor})
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(listing(cursor)).encode()
        response.headers["Content-Type"] = "application/json"
        response.url = url
        return response

    requests.Session.request = request
    from trr_backend.media import s3_mirror

    client = Storage()
    s3_mirror.get_s3_client = lambda: client
    s3_mirror.get_object_storage_client = lambda: client
    s3_mirror.get_s3_bucket = lambda: "fixture"
    s3_mirror.get_object_storage_bucket = lambda: "fixture"
    s3_mirror.get_s3_prefix = lambda: ""
    s3_mirror.build_hosted_url = lambda key: "https://media.example/" + key
    s3_mirror.build_public_object_url = s3_mirror.build_hosted_url
    s3_mirror.get_public_base_url = lambda: "https://media.example"
    config = SimpleNamespace(
        profile_name=None,
        region="us-east-1",
        endpoint_url=None,
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        bucket="fixture",
        prefix="",
        cdn_base_url="https://media.example",
    )
    s3_mirror.get_hosted_media_storage_config = lambda: config
    s3_mirror.get_s3_config = lambda: config
    import boto3

    boto3.Session = lambda **kwargs: SimpleNamespace(client=lambda *a, **k: client)
    if legacy_mode:
        from trr_backend.socials import social_season_analytics_impl as legacy
        from trr_backend.socials.instagram import catalog_ingest

        def build_scraper(**kwargs):
            return scraper.InstagramScraper(cookies=cookies, browser_account_id=ACCOUNT, attach_auth_session=False)

        legacy._build_shared_instagram_scraper = build_scraper
        catalog_ingest._build_shared_instagram_scraper = build_scraper
    from trr_backend.socials import media_url_safety

    def send_pinned(prepared, **kwargs):
        return get(None, prepared.url)

    media_url_safety._send_pinned_request = send_pinned
    return trace
