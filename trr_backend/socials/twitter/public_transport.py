"""Credential-free provider reads in disposable, deadline-bound processes."""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests

MAX_BYTES = 2_000_000


def _validate(url: str) -> None:
    parsed = urlsplit(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    allowed = parsed.netloc == "lurkbird.com" and parsed.path.startswith("/search/")
    if parsed.netloc == "api.fxtwitter.com":
        if parsed.path == "/2/search":
            allowed = set(params) <= {"q", "feed", "cursor"} and bool(params.get("q"))
        elif re.fullmatch(r"/2/profile/(?:[A-Za-z0-9_]{1,15}|id:[0-9]{2,20})/statuses", parsed.path):
            allowed = set(params) <= {"cursor"}
        elif re.fullmatch(r"/2/(status|thread|conversation)/[0-9]{2,20}", parsed.path):
            allowed = set(params) <= {"cursor"}
    if parsed.netloc == "cdn.syndication.twimg.com" and parsed.path == "/tweet-result":
        allowed = set(params) <= {"id", "lang", "token"} and bool(
            re.fullmatch(r"[0-9]{2,20}", params.get("id", [""])[0])
        )
    if (
        parsed.scheme != "https"
        or not allowed
        or parsed.fragment
        or parsed.username
        or parsed.password
        or len(url) > 10000
        or any(len(values) != 1 for values in params.values())
    ):
        raise ValueError("Invalid public provider URL")


def _read(url: str, timeout: float) -> bytes:
    _validate(url)
    with requests.Session() as session:
        session.trust_env = False
        with session.get(
            url,
            timeout=(min(10, timeout), timeout),
            allow_redirects=False,
            stream=True,
            headers={"Accept": "application/json, text/html", "User-Agent": "TRR-public-research/1.0"},
        ) as response:
            if response.status_code != 200:
                raise ValueError(f"Mirror HTTP {response.status_code}")
            body = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                body.extend(chunk)
                if len(body) > MAX_BYTES:
                    raise ValueError("Mirror response exceeds size limit")
            return bytes(body)


def fetch(url: str, *, deadline: float, cancelled=None) -> bytes:
    """Bound DNS, headers and body together; kill and reap only our worker."""
    _validate(url)
    remaining = min(20.0, deadline - time.monotonic())
    if remaining <= 0:
        raise ValueError("Public provider deadline")
    # No environment credentials/proxies, HOME config, imported state or profiles.
    worker_env = {"PATH": os.defpath}
    with subprocess.Popen(
        [sys.executable, "-I", str(Path(__file__).resolve())],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=worker_env,
    ) as worker:
        try:
            payload = json.dumps({"url": url, "timeout": remaining}).encode()
            request_deadline = min(deadline, time.monotonic() + remaining)
            while True:
                if cancelled and cancelled():
                    worker.kill()
                    worker.communicate()
                    raise ValueError("Public collection cancelled")
                budget = request_deadline - time.monotonic()
                if budget <= 0:
                    raise subprocess.TimeoutExpired("public provider", remaining)
                try:
                    output, _ = worker.communicate(payload, timeout=min(0.2, budget) if cancelled else budget)
                    break
                except subprocess.TimeoutExpired:
                    payload = None
                    if not cancelled:
                        raise
        except subprocess.TimeoutExpired:
            worker.kill()
            worker.communicate()
            raise ValueError("Public provider deadline") from None
        if time.monotonic() > deadline:
            raise ValueError("Public provider deadline")
        if worker.returncode:
            raise ValueError("Public provider worker failed")
    try:
        payload = json.loads(output)
        if payload.get("error"):
            raise ValueError(payload["error"])
        body = base64.b64decode(payload["body"], validate=True)
        if len(body) > MAX_BYTES:
            raise ValueError("Mirror response exceeds size limit")
        return body
    except (KeyError, TypeError, json.JSONDecodeError):
        raise ValueError("Public provider worker failed") from None


if __name__ == "__main__":
    args = json.load(sys.stdin)
    try:
        record = {"body": base64.b64encode(_read(args["url"], args["timeout"])).decode()}
    except ValueError as exc:
        record = {"error": str(exc)}
    except requests.RequestException:
        record = {"error": "Public provider request failed"}
    json.dump(record, sys.stdout)
