"""Credential-free provider reads in disposable, deadline-bound processes."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import requests

MAX_BYTES = 2_000_000


def _validate(url: str) -> None:
    parsed = urlsplit(url)
    allowed = (parsed.netloc == "lurkbird.com" and parsed.path.startswith("/search/")) or (
        parsed.netloc == "api.fxtwitter.com" and parsed.path.startswith("/2/conversation/")
    )
    if parsed.scheme != "https" or not allowed or parsed.fragment or len(url) > 10000:
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


def fetch(url: str, *, deadline: float) -> bytes:
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
            output, _ = worker.communicate(json.dumps({"url": url, "timeout": remaining}).encode(), timeout=remaining)
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
