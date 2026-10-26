"""Isolated public reply transport; no fetcher or persistence mutations."""

import asyncio
import codecs
import json
import math
import os
import re
import tempfile
import zlib
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path

import httpx

from .reply_scheduler import DiskResponseSpool

response_observed_at: ContextVar[datetime | None] = ContextVar("instagram_response_observed_at", default=None)
ReplyProtocolLimitExceeded = DiskResponseSpool.ProtocolLimitError
RAW_CHUNK_BYTES = 64 * 1024


class ReplyClientPool:
    """At most two persistent clients; leases last through owner response close.

    Create and close within the scheduler's event loop. Clients never share a
    cookie jar concurrently, and each lease resets it to its immutable snapshot.
    """

    def __init__(self, *, timeout, client_factory=httpx.AsyncClient, slots=2):
        if type(slots) is not int or slots not in (1, 2):
            raise ValueError("Reply client slots must be 1 or 2")
        self.timeout, self.client_factory = timeout, client_factory
        self._available = asyncio.Queue()
        self._clients = {}
        self._leased = set()
        self._closed = False
        self._slots = slots
        for slot in range(slots):
            self._available.put_nowait(slot)

    async def acquire(self, snapshot):
        if self._closed:
            raise RuntimeError("Reply client pool is closed")
        slot = await self._available.get()
        if self._closed:
            self._available.put_nowait(slot)
            raise RuntimeError("Reply client pool is closed")
        self._leased.add(slot)
        try:
            if slot not in self._clients:
                self._clients[slot] = self.client_factory(
                    timeout=self.timeout,
                    follow_redirects=False,
                    trust_env=False,
                    headers={"accept-encoding": "gzip, deflate"},
                )
            client = self._clients[slot]
            client.cookies.clear()
            for name, value, domain, path in snapshot:
                client.cookies.set(name, value, domain=domain, path=path)
            return client, lambda: self._release(slot)
        except BaseException:
            self._release(slot)
            raise

    def _release(self, slot):
        if slot in self._leased:
            if slot in self._clients:
                self._clients[slot].cookies.clear()
            self._leased.remove(slot)
            self._available.put_nowait(slot)

    async def aclose(self):
        self._closed = True
        for _ in range(self._slots):
            self._available.put_nowait(None)
        # The owning scheduler closes its responses before leaving the context.
        # Still close every client if its shutdown is handling an exception.
        results = await asyncio.gather(*(client.aclose() for client in self._clients.values()), return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError("Reply client pool is closed")
        return self

    async def __aexit__(self, *_args):
        await self.aclose()


def write_timing_receipt(shortcode, generation, concurrency, metrics):
    """Optional local operator receipt containing fixed counters only."""
    directory = os.environ.get("TRR_INSTAGRAM_REPLY_TELEMETRY_DIR")
    if not directory:
        return
    if not re.fullmatch(r"[A-Za-z0-9_-]+", shortcode) or type(generation) is not int or generation < 0:
        raise ValueError("Invalid reply telemetry identity")
    allowed = {
        "requests",
        "network_seconds",
        "pacing_seconds",
        "queue_seconds",
        "owner_seconds",
        "admission_seconds",
        "max_outstanding",
        "completed_queue_seconds",
        "persistence_seconds",
        "elapsed_seconds",
        "committed_pages",
        "rate_limited_responses",
        "auth_failed_responses",
        "reply_access_rejected_responses",
    }
    safe = {
        key: value
        for key, value in metrics.items()
        if key in allowed and type(value) in (int, float) and math.isfinite(value) and value >= 0
    }
    root = Path(directory)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt = {
        "target": shortcode,
        "owner_generation": generation,
        "concurrency": concurrency,
        "observed_at": datetime.now(UTC).isoformat(),
        "metrics": safe,
    }
    # Each hydration has its own receipt so repeated calls cannot erase evidence.
    with tempfile.NamedTemporaryFile(
        mode="w", dir=root, prefix=f"{shortcode}-{generation}-", suffix=".pending", delete=False
    ) as handle:
        json.dump(receipt, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
        path = Path(handle.name)
    path.replace(path.with_suffix(".json"))


class SpoolResponse:
    def __init__(self, response, spool, observed_at, release=None, *, body_complete=True):
        self.status_code = response.status_code
        self.headers = response.headers
        self.cookies = response.cookies
        self.url = response.url
        self.spool = spool
        self.observed_at = observed_at
        self.body_size = spool.size
        self._release = release
        self.body_complete = body_complete
        self.cancelled_on_rejection = not body_complete

    def json(self):
        if not self.body_complete:
            raise ValueError("Rejected reply body was not read")
        return self.spool.json()

    def iter_text_chunks(self):
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        tail = ""
        for chunk in self.spool.iter_chunks():
            text = tail + decoder.decode(chunk)
            yield text
            tail = text[-4096:]
        yield tail + decoder.decode(b"", final=True)

    def close(self):
        try:
            self.spool.close()
        finally:
            if self._release is not None:
                release, self._release = self._release, None
                release()


async def _raw_chunks(response):
    # httpcore HTTP/1 and HTTP/2 read at most 64KiB. Reject a nonstandard
    # transport that hands us a larger raw chunk rather than copying it again.
    if response.is_stream_consumed:
        chunks = (response.content,)
        for chunk in chunks:
            if len(chunk) > RAW_CHUNK_BYTES:
                raise ReplyProtocolLimitExceeded("raw_chunk_limit")
            yield chunk
        return
    async for chunk in response.aiter_raw():
        if len(chunk) > RAW_CHUNK_BYTES:
            raise ReplyProtocolLimitExceeded("raw_chunk_limit")
        yield chunk


async def _spool_body(response, spool):
    encoding = response.headers.get("content-encoding", "identity").strip().lower()
    if encoding not in ("", "identity", "gzip", "deflate"):
        raise ReplyProtocolLimitExceeded("unsupported_content_encoding")
    # A pre-consumed explicit test response has already passed through HTTPX's
    # decoder. Real streamed responses always use the bounded raw path below.
    decoder = None
    if not response.is_stream_consumed and encoding in ("gzip", "deflate"):
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS if encoding == "gzip" else zlib.MAX_WBITS)
    deflate_header = b""
    async for chunk in _raw_chunks(response):
        if decoder is None:
            spool.write(chunk)
            continue
        if encoding == "deflate" and len(deflate_header) < 2:
            deflate_header += chunk
            if len(deflate_header) < 2:
                continue
            chunk = deflate_header
            header = int.from_bytes(chunk[:2], "big")
            wrapped = chunk[0] & 15 == 8 and chunk[0] >> 4 <= 7 and header % 31 == 0
            decoder = zlib.decompressobj(zlib.MAX_WBITS if wrapped else -zlib.MAX_WBITS)
            deflate_header = chunk[:2]
        while chunk:
            decoded = decoder.decompress(chunk, RAW_CHUNK_BYTES)
            spool.write(decoded)
            chunk = decoder.unconsumed_tail
            if decoder.unused_data:
                raise ReplyProtocolLimitExceeded("extra_compressed_member")
            await asyncio.sleep(0)
    if decoder is not None and not decoder.eof:
        raise ValueError("Incomplete compressed reply response")


async def fetch_reply(
    request, *, url, timeout, client_factory=httpx.AsyncClient, clock=lambda: datetime.now(UTC), pool=None
):
    spool = DiskResponseSpool()
    release = None
    owned_client = None
    try:
        if pool is None:
            owned_client = client_factory(
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                headers={"accept-encoding": "gzip, deflate"},
            )
            client = owned_client
            client.cookies.clear()
            for name, value, domain, path in request.public_cookie_snapshot:
                client.cookies.set(name, value, domain=domain, path=path)
        else:
            client, release = await pool.acquire(request.public_cookie_snapshot)
        async with client.stream(
            "POST", url, data=dict(request.request_data), headers=dict(request.request_headers), timeout=timeout
        ) as response:
            body_complete = response.status_code not in (401, 403, 429)
            if body_complete:
                await _spool_body(response, spool)
            result = SpoolResponse(response, spool, clock(), release, body_complete=body_complete)
            release = None  # owner close now owns the lease
            return result
    except BaseException:
        spool.close()
        raise
    finally:
        if release is not None:
            release()
        if owned_client is not None:
            await owned_client.aclose()
