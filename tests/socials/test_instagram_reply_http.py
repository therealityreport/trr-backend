"""Explicit mock streaming HTTP exercises disk bodies without live services."""

import asyncio
import gzip
import json
import tracemalloc
import zlib
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling import fetcher, reply_http
from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import ReplyRequest

CHILD = "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
OBSERVED = datetime(2026, 10, 24, tzinfo=UTC)
CHUNK_SIZE = 64 * 1024


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks, *, fail=False, block=False):
        self.chunks = chunks
        self.fail = fail
        self.block = block
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        self.started.set()
        if self.fail:
            raise httpx.ReadError("synthetic partial read")
        if self.block:
            await asyncio.Event().wait()

    async def aclose(self):
        self.closed = True


@pytest.fixture(autouse=True)
def no_real_http(monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("Reply HTTP test attempted an unmocked network transport")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)


def descriptor():
    return ReplyRequest(
        "parent",
        "doc",
        "popular",
        12,
        public_cookie_snapshot=(("csrftoken", "initial", ".instagram.com", "/"),),
        request_data=(("doc_id", "doc"), ("variables", "{}")),
    )


def transport(handler):
    clients = []

    def factory(**kwargs):
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        assert "cookies" not in kwargs and "proxy" not in kwargs
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)
        clients.append(client)
        return client

    return factory, clients


async def fetch(request, factory):
    return await reply_http.fetch_reply(
        request,
        url="https://www.instagram.com/graphql/query/",
        timeout=httpx.Timeout(5),
        client_factory=factory,
        clock=lambda: OBSERVED,
    )


class BoundedReader:
    """Fail if owner parsing tries to materialize the unbounded raw body."""

    def __init__(self, underlying):
        self.underlying = underlying
        self.largest_read = 0

    def read(self, size=-1):
        assert 0 <= size <= CHUNK_SIZE, "Unbounded raw response read"
        self.largest_read = max(self.largest_read, size)
        return self.underlying.read(size)

    def __getattr__(self, key):
        return getattr(self.underlying, key)


def test_more_than_eight_mib_native_inventory_and_last_sentinel_are_complete(monkeypatch):
    total = 18_000
    written_sizes = []
    original_write = reply_http.DiskResponseSpool.write

    def tracked_write(self, chunk):
        written_sizes.append(len(chunk))
        return original_write(self, chunk)

    monkeypatch.setattr(reply_http.DiskResponseSpool, "write", tracked_write)

    def chunks():
        yield ('{"data":{"' + CHILD + '":{"edges":[').encode()
        for index in range(total):
            identity = "last-native-sentinel" if index == total - 1 else str(index)
            node = {"node": {"pk": identity, "text": "x" * 512, "child_comment_count": None}}
            yield (b"," if index else b"") + json.dumps(node, separators=(",", ":")).encode()
        yield b'],"page_info":{"has_next_page":false,"end_cursor":null}}}}'

    stream = ChunkStream(chunks())
    factory, clients = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        assert response.body_size > 8 * 1024 * 1024 and response.observed_at == OBSERVED
        assert written_sizes and max(written_sizes) <= CHUNK_SIZE
        reader = BoundedReader(response.spool._file)
        response.spool._file = reader
        payload = response.json()
        nodes = payload["data"][CHILD]["edges"]
        assert len(nodes) == total and len({entry["node"]["pk"] for entry in nodes}) == total
        assert nodes[0]["node"]["pk"] == "0"
        assert nodes[-1]["node"]["pk"] == "last-native-sentinel"
        assert all(entry["node"]["text"] == "x" * 512 for entry in nodes)
        assert all(entry["node"]["child_comment_count"] is None for entry in nodes)
        assert reader.largest_read <= CHUNK_SIZE
    finally:
        response.close()
    assert stream.closed and response.spool.closed and all(client.is_closed for client in clients)


@pytest.mark.parametrize(
    "prefix",
    [b"", b"for (;;);", b" \n\tfor (;;);", b" \n\t" * 50_000 + b"for (;;);"],
    ids=["plain", "guard", "whitespace", "long-whitespace"],
)
def test_streaming_json_accepts_relay_xssi_and_preserves_final_value(prefix):
    stream = ChunkStream(
        [
            *(prefix[i : i + CHUNK_SIZE] for i in range(0, len(prefix), CHUNK_SIZE)),
            b'{"data":[1,',
            b'2],"sentinel":"last"}',
        ]
    )
    factory, _ = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        assert response.json() == {"data": [1, 2], "sentinel": "last"}
        assert fetcher._decode_graphql_json_payload(response) == {"data": [1, 2], "sentinel": "last"}
    finally:
        response.close()


@pytest.mark.parametrize("trailing", [b"garbage", b"{}", b"null", b"[1]", b"\xff"])
def test_streaming_json_rejects_trailing_garbage_or_second_values(trailing):
    stream = ChunkStream([b'{"data":{"sentinel":"complete"}} ', trailing])
    factory, _ = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        with pytest.raises(ValueError):
            response.json()
        assert fetcher._decode_graphql_json_payload(response) is None
    finally:
        response.close()


def test_late_challenge_marker_crossing_disk_chunk_boundary_is_classified():
    def chunks():
        yield from (b"x" * CHUNK_SIZE for _ in range(128))
        yield b"x" * (CHUNK_SIZE - 4) + b"chal"
        yield b"lenge_required"

    stream = ChunkStream(chunks())
    factory, _ = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        assert response.body_size > response.spool.MAX_MEMORY_BYTES
        assert fetcher._public_relay_block_reason_from_response(response, child_operation=True) == "checkpoint"
        # The scanner keeps only a bounded overlap and scans through the tail.
        assert max(map(len, response.iter_text_chunks())) <= CHUNK_SIZE + 4096
    finally:
        response.close()


@pytest.mark.parametrize("cancel", [False, True])
def test_partial_read_error_and_cancellation_close_spool_stream_and_client(monkeypatch, cancel):
    spools = []
    original_spool = reply_http.DiskResponseSpool

    def tracked_spool():
        spool = original_spool()
        spools.append(spool)
        return spool

    monkeypatch.setattr(reply_http, "DiskResponseSpool", tracked_spool)

    async def scenario():
        stream = ChunkStream([b"x" * CHUNK_SIZE, b"partial"], fail=not cancel, block=cancel)
        factory, clients = transport(lambda _request: httpx.Response(200, stream=stream))
        task = asyncio.create_task(fetch(descriptor(), factory))
        if cancel:
            await stream.started.wait()
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else httpx.ReadError):
            await task
        assert stream.closed and all(client.is_closed for client in clients)
        assert len(spools) == 1 and spools[0].closed and spools[0].size >= CHUNK_SIZE
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(scenario())


def test_public_cookie_snapshot_is_unchanged_and_response_cookie_never_crosses_requests():
    snapshots = []
    request = descriptor()

    def handler(outgoing):
        snapshots.append(outgoing.headers.get("cookie", ""))
        return httpx.Response(
            200,
            stream=ChunkStream([b'{"data":{}}']),
            headers={"set-cookie": "sessionid=forbidden-response-only; Path=/"},
        )

    factory, clients = transport(handler)

    async def scenario():
        for _ in range(2):
            response = await fetch(request, factory)
            try:
                assert response.cookies.get("sessionid") == "forbidden-response-only"
                assert response.json() == {"data": {}}
            finally:
                response.close()

    asyncio.run(scenario())
    assert request.public_cookie_snapshot == (("csrftoken", "initial", ".instagram.com", "/"),)
    assert len(snapshots) == 2 and all("csrftoken=initial" in snapshot for snapshot in snapshots)
    assert all("sessionid" not in snapshot and "ds_user_id" not in snapshot for snapshot in snapshots)
    assert len(clients) == 2 and all(client.is_closed for client in clients)


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "raw-deflate"])
def test_two_compressed_expansions_have_bounded_aggregate_raw_memory(monkeypatch, encoding):
    # Compress before tracing: the deliberately large test input is outside the
    # transport, whose two concurrent expansions must stay below the aggregate cap.
    body = b" " * (12 * 1024 * 1024) + b'{"sentinel":"last"}'
    if encoding == "gzip":
        encoded = gzip.compress(body)
    elif encoding == "deflate":
        encoded = zlib.compress(body)
    else:
        compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        encoded = compressor.compress(body) + compressor.flush()
    del body
    writes = []
    original_write = reply_http.DiskResponseSpool.write

    def bounded_write(self, chunk):
        writes.append(len(chunk))
        assert len(chunk) <= CHUNK_SIZE
        return original_write(self, chunk)

    monkeypatch.setattr(reply_http.DiskResponseSpool, "write", bounded_write)

    def handler(_request):
        # Split even the encoding header across reads to cover deflate sniffing.
        assert _request.headers["accept-encoding"] == "gzip, deflate"
        return httpx.Response(
            200,
            stream=ChunkStream([encoded[:1], encoded[1:]]),
            headers={"content-encoding": "deflate" if encoding == "raw-deflate" else encoding},
        )

    factory, clients = transport(handler)

    async def scenario():
        responses = await asyncio.gather(fetch(descriptor(), factory), fetch(descriptor(), factory))
        try:
            assert all(response.body_size > 12 * 1024 * 1024 for response in responses)
            assert all(response.json() == {"sentinel": "last"} for response in responses)
        finally:
            for response in responses:
                response.close()

    tracemalloc.start()
    try:
        asyncio.run(scenario())
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert writes and max(writes) <= CHUNK_SIZE
    assert peak < reply_http.DiskResponseSpool.MAX_MEMORY_BYTES
    assert all(client.is_closed for client in clients)


def test_giant_string_and_key_preserved_with_bounded_raw_reader():
    chunks = [b'{"long":"', *(b"x" * CHUNK_SIZE for _ in range(145)), b'\\ud83d\\ude00","', b"k" * 1024, b'":"last"}']
    stream = ChunkStream(chunks)
    factory, _ = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        reader = BoundedReader(response.spool._file)
        response.spool._file = reader
        parsed = response.json()
        assert parsed["long"] == "x" * (145 * CHUNK_SIZE) + "😀"
        assert parsed["k" * 1024] == "last"
        assert response.body_size == sum(map(len, chunks))
        assert response.spool.raw_buffer_peak_bytes < 1024 * 1024
        assert reader.largest_read <= CHUNK_SIZE
        assert not response.spool.closed
    finally:
        response.close()


@pytest.mark.parametrize("read_size", [1, 7, CHUNK_SIZE])
@pytest.mark.parametrize(
    "value",
    [
        False,
        None,
        {"quoted\\key": ['line\nslash\\quote"', "é中😀", "\ud800x\udc00"]},
        [0, -42, 1.25, 1e100, True, {}, []],
    ],
)
def test_bounded_parser_matches_standard_json_across_utf8_and_escape_boundaries(read_size, value):
    class ShortReader(BoundedReader):
        def read(self, size=-1):
            return super().read(min(size, read_size))

    # ensure_ascii=False cannot encode intentionally lone surrogates, so keep
    # those escaped while separately exercising literal multi-byte UTF-8.
    payload = json.dumps(value).replace("\\u00e9", "é").replace("\\u4e2d", "中").encode()
    with reply_http.DiskResponseSpool.from_chunks([payload]) as spool:
        spool._file = ShortReader(spool._file)
        assert spool.json() == json.loads(payload) == value


@pytest.mark.parametrize(
    "payload",
    [b"", b"[1,]", b'{"x":1,}', b"01", b"+1", b'"\\q"', b'"\\u12"', b'"bad\x00"', b"falsex"],
)
def test_bounded_parser_rejects_malformed_json(payload):
    with reply_http.DiskResponseSpool.from_chunks([payload]) as spool, pytest.raises(ValueError):
        spool.json()


@pytest.mark.parametrize("payload", [b"[" * 129 + b"0" + b"]" * 129, b'{"n":' + b"1" * 4097 + b"}"])
def test_protocol_depth_and_numeric_conversion_bounds_are_diagnostic(payload):
    stream = ChunkStream([payload])
    factory, _ = transport(lambda _request: httpx.Response(200, stream=stream))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        with pytest.raises(reply_http.ReplyProtocolLimitExceeded):
            response.json()
        assert response.body_size == len(payload) and not response.spool.closed
    finally:
        response.close()


@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_truncated_compression_rejects_and_cleans_up(monkeypatch, encoding):
    encoded = gzip.compress(b'{"sentinel":"last"}') if encoding == "gzip" else zlib.compress(b'{"sentinel":"last"}')
    stream = ChunkStream([encoded[:-3]])
    factory, clients = transport(
        lambda _request: httpx.Response(200, stream=stream, headers={"content-encoding": encoding})
    )
    spools = []
    original_spool = reply_http.DiskResponseSpool

    def tracked_spool():
        spool = original_spool()
        spools.append(spool)
        return spool

    monkeypatch.setattr(reply_http, "DiskResponseSpool", tracked_spool)
    with pytest.raises(ValueError, match="Incomplete compressed"):
        asyncio.run(fetch(descriptor(), factory))
    assert stream.closed and all(client.is_closed for client in clients)
    assert spools and all(spool.closed for spool in spools)


def test_pool_reuses_two_clients_resets_cookies_and_holds_lease_until_response_close():
    snapshots = []

    def handler(outgoing):
        snapshots.append(outgoing.headers.get("cookie", ""))
        assert outgoing.headers["accept-encoding"] == "gzip, deflate"
        return httpx.Response(
            200,
            stream=ChunkStream([b'{"sentinel":"last"}']),
            headers={"set-cookie": "sessionid=forbidden; Path=/"},
        )

    factory, clients = transport(handler)

    async def scenario():
        async with reply_http.ReplyClientPool(timeout=httpx.Timeout(5), client_factory=factory) as pool:
            assert not clients

            async def pooled(value):
                request = replace(descriptor(), public_cookie_snapshot=(("csrftoken", value, ".instagram.com", "/"),))
                return await reply_http.fetch_reply(
                    request, url="https://www.instagram.com/graphql/query/", timeout=httpx.Timeout(5), pool=pool
                )

            first, second = await asyncio.gather(pooled("first"), pooled("second"))
            assert len(clients) == 2 and len(snapshots) == 2
            third_task = asyncio.create_task(pooled("third"))
            await asyncio.sleep(0)
            assert not third_task.done() and len(snapshots) == 2
            first.close()
            third = await third_task
            first.close()  # release is idempotent, never creates a third slot
            assert len(clients) == 2
            assert third.json() == {"sentinel": "last"}
            second.close()
            third.close()
            fourth = await pooled("fourth")
            fourth.close()
            assert not any(client.is_closed for client in clients)
        assert all(client.is_closed for client in clients)

    asyncio.run(scenario())
    assert snapshots == ["csrftoken=first", "csrftoken=second", "csrftoken=third", "csrftoken=fourth"]
    assert all("sessionid" not in snapshot for snapshot in snapshots)


@pytest.mark.parametrize("cancel", [False, True])
def test_pool_read_failure_or_cancellation_releases_slot_and_preserves_cleanup(cancel):
    stream = ChunkStream([b"partial"], fail=not cancel, block=cancel)
    requests = 0

    def handler(_request):
        nonlocal requests
        requests += 1
        return httpx.Response(200, stream=stream if requests == 1 else ChunkStream([b"{} "]))

    factory, clients = transport(handler)

    async def scenario():
        async with reply_http.ReplyClientPool(timeout=httpx.Timeout(5), client_factory=factory, slots=1) as pool:

            async def pooled():
                return await reply_http.fetch_reply(
                    descriptor(), url="https://www.instagram.com/graphql/query/", timeout=httpx.Timeout(5), pool=pool
                )

            task = asyncio.create_task(pooled())
            if cancel:
                await stream.started.wait()
                task.cancel()
            with pytest.raises(asyncio.CancelledError if cancel else httpx.ReadError):
                await task
            response = await asyncio.wait_for(pooled(), 1)
            assert response.json() == {}
            response.close()
            assert len(clients) == 1 and stream.closed
        assert all(client.is_closed for client in clients)

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [401, 403, 429])
def test_rejection_headers_close_slow_body_without_reading_it(status):
    class NeverRead(ChunkStream):
        async def __aiter__(self):
            pytest.fail("Unequivocal rejection body must not be read")
            yield b""  # pragma: no cover

    stream = NeverRead([])
    factory, clients = transport(lambda _request: httpx.Response(status, stream=stream, headers={"retry-after": "60"}))
    response = asyncio.run(fetch(descriptor(), factory))
    try:
        assert response.status_code == status and response.headers["retry-after"] == "60"
        assert response.body_complete is False and response.cancelled_on_rejection is True
        assert response.body_size == 0 and response.observed_at == OBSERVED
        with pytest.raises(ValueError, match="not read"):
            response.json()
        assert stream.closed and all(client.is_closed for client in clients)
    finally:
        response.close()


def test_rejection_headers_allow_scheduler_to_cancel_other_slow_inflight_body():
    from trr_backend.socials.instagram.comments_scrapling.reply_scheduler import run_reply_lanes

    async def scenario():
        other = ChunkStream([b"partial"], block=True)
        rejected = ChunkStream([], block=True)
        stopped = False

        async def handler(outgoing):
            if outgoing.headers["x-test-lane"] == "reject":
                await other.started.wait()
                return httpx.Response(429, stream=rejected, headers={"retry-after": "60"})
            return httpx.Response(200, stream=other)

        factory, clients = transport(handler)

        def lane(name):
            nonlocal stopped
            response = yield replace(descriptor(), parent_id=name, request_headers=(("x-test-lane", name),))
            assert name == "reject" and response.status_code == 429
            stopped = True

        async with reply_http.ReplyClientPool(timeout=httpx.Timeout(5), client_factory=factory) as pool:

            async def request(command):
                return await reply_http.fetch_reply(
                    command, url="https://www.instagram.com/graphql/query/", timeout=httpx.Timeout(5), pool=pool
                )

            async def pace(_command):
                return True

            await asyncio.wait_for(
                run_reply_lanes(
                    {"reject": lane("reject"), "other": lane("other")},
                    request=request,
                    pace=pace,
                    concurrency=2,
                    stop=lambda: stopped,
                ),
                1,
            )
        assert stopped and other.closed and rejected.closed
        assert all(client.is_closed for client in clients)
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]

    asyncio.run(scenario())


def test_nonstrict_public_two_parent_cookie_rotation_stays_on_shared_client(monkeypatch):
    from tests.ingestion.test_public_comment_transport import page
    from tests.socials.test_instagram_comments_scrapling_retry import _comment

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_GLOBAL_THROTTLE", "0")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC", "0")
    monkeypatch.setattr(fetcher, "build_stealthy_fetcher", lambda *_args, **_kwargs: pytest.fail("Unexpected browser"))

    async def isolated_forbidden(*_args, **_kwargs):
        pytest.fail("Nonstrict public caller entered strict isolated transport")

    monkeypatch.setattr(reply_http, "fetch_reply", isolated_forbidden)
    snapshots = []

    def handler(outgoing):
        snapshots.append(outgoing.headers.get("cookie", ""))
        return httpx.Response(200, json=page([], child=True), headers={"set-cookie": "csrftoken=rotated; Path=/"})

    factory, clients = transport(handler)

    async def scenario():
        owner = fetcher.InstagramCommentsScraplingFetcher(
            cookies=[], raw_cookies={}, public_only=True, browser_account_id=None
        )
        # Model the anonymous bootstrap's cookie after the constructor guard.
        owner._raw_cookies = {"csrftoken": "initial"}
        owner._api_delay_seconds = owner._global_api_delay_seconds = 0
        async with factory(timeout=httpx.Timeout(5), trust_env=False, follow_redirects=False) as client:
            client.cookies.set("csrftoken", "initial", domain=".instagram.com", path="/")
            result = await owner._fetch_public_relay_child_comments_for_status_only(
                public_client=client,
                shortcode="fixture",
                post_url="https://www.instagram.com/p/fixture/",
                media_id="media",
                comments=[_comment("parent1", reply_count=1), _comment("parent2", reply_count=1)],
                graphql_headers={},
                common_body={"__user": "0", "av": "0"},
                target_count=None,
                max_comments=0,
                strict_native_exhaustion=False,
            )
            assert result["parent_attempts"] == 2
            assert owner._raw_cookies["csrftoken"] == "rotated"

    asyncio.run(scenario())
    assert len(snapshots) == 2 and snapshots[0] == "csrftoken=initial" and snapshots[1] == "csrftoken=rotated"
    assert len(clients) == 1 and clients[0].is_closed
