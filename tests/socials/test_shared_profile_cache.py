"""Real Redis contracts; isolated ephemeral server, no persistent state."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock

import pytest
import redis

from api.routers.socials import _profile_cache as routes
from trr_backend.socials import shared_profile_cache as cache

pytestmark = pytest.mark.live


@pytest.fixture
def server(tmp_path, monkeypatch):
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server is required for Redis integration tests")
    scratch = tempfile.TemporaryDirectory(prefix="trr-redis-", dir="/tmp")
    socket = scratch.name + "/redis.sock"
    proc = subprocess.Popen(
        [binary, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = redis.Redis(unix_socket_path=socket, decode_responses=True)
    try:
        for _ in range(100):
            try:
                if client.ping():
                    break
            except redis.ConnectionError:
                time.sleep(0.01)
        else:
            raise AssertionError("Redis failed to start")
        monkeypatch.setenv("TRR_PROFILE_REDIS_CACHE_ENABLED", "1")
        monkeypatch.setenv("TRR_CACHE_NAMESPACE", "test")
        monkeypatch.setattr(cache, "_client", lambda: client)
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait(timeout=5)
        scratch.cleanup()


def test_separate_clients_share_values_and_invalidate_inflight(server, monkeypatch):
    key = ("summary", "instagram", "bravotv")
    ticket, _ = cache.begin(key)
    cache.publish(ticket, {"value": "old"}, 600)
    other = redis.Redis(connection_pool=server.connection_pool)
    monkeypatch.setattr(cache, "_client", lambda: other)
    assert cache.begin(key)[1] == {"value": "old"}
    assert 0 < server.ttl(ticket.payload_key) <= 30
    cache.invalidate()
    assert cache.begin(key)[1] is None
    cache.publish(ticket, {"value": "late"}, 600)
    current, _ = cache.begin(key)
    cache.publish(current, {"value": "new"}, 600)
    assert cache.begin(key)[1] == {"value": "new"}


def test_generation_loss_does_not_resurrect_old_payload(server):
    ticket, _ = cache.begin(("summary", "x"))
    cache.publish(ticket, {"value": "old"}, 10)
    server.delete(ticket.generation_key)
    new, payload = cache.begin(("summary", "x"))
    assert new.generation != ticket.generation
    assert payload is None
    cache.publish(ticket, {"value": "late"}, 10)
    assert cache.begin(("summary", "x"))[1] is None


def test_payload_expiry_and_limits(server):
    ticket, _ = cache.begin(("summary", "x"))
    cache.publish(ticket, {"value": "small"}, 1)
    server.pexpire(ticket.payload_key, 1)
    time.sleep(0.01)
    assert cache.begin(("summary", "x"))[1] is None
    cache.publish(ticket, {"value": "x" * cache.MAX_PAYLOAD_BYTES}, 30)
    assert cache.begin(("summary", "x"))[1] is None


def test_environment_and_query_partition(server, monkeypatch):
    ticket, _ = cache.begin(("summary", "instagram", "bravo", "full"))
    cache.publish(ticket, {"value": 1}, 30)
    assert cache.begin(("summary", "instagram", "bravo", "fast"))[1] is None
    monkeypatch.setenv("TRR_CACHE_NAMESPACE", "other")
    assert cache.begin(("summary", "instagram", "bravo", "full"))[1] is None


def test_failure_uses_loader_and_never_l1(server, monkeypatch):
    def unavailable():
        raise redis.ConnectionError("secret-bearing error must not be logged")

    monkeypatch.setattr(cache, "_client", unavailable)
    local = {("summary", "x"): (float("inf"), {"value": "stale"})}
    result = routes._resolve_account_profile_singleflight(
        ("summary", "x"), lambda: {"value": "fresh"}, cache=local, cache_lock=Lock(), ttl_seconds=600, max_entries=10
    )
    assert result == {"value": "fresh"}
    assert local[("summary", "x")][1] == {"value": "stale"}
    cache.invalidate()


def test_remote_invalidation_separates_local_flights(server):
    started, release = Event(), Event()

    def old_loader():
        started.set()
        assert release.wait(5)
        return {"value": "old"}

    kwargs = {"ttl_seconds": 600, "cache": {}, "cache_lock": Lock(), "max_entries": 10}
    key = ("summary", "x")
    with ThreadPoolExecutor(max_workers=2) as pool:
        old = pool.submit(routes._resolve_account_profile_singleflight, key, old_loader, **kwargs)
        assert started.wait(5)
        cache.invalidate()
        assert routes._resolve_account_profile_singleflight(key, lambda: {"value": "new"}, **kwargs) == {"value": "new"}
        release.set()
        assert old.result() == {"value": "old"}
    assert cache.begin(key)[1] == {"value": "new"}


def test_disabled_and_nonpilot_reads(server, monkeypatch):
    assert not cache.enabled_for(("catalog-run-progress",))
    monkeypatch.setenv("TRR_PROFILE_REDIS_CACHE_ENABLED", "0")
    assert not cache.enabled_for(("summary",))


def test_native_database_values_match_uncached_api_response(server):
    from datetime import UTC, date, datetime
    from decimal import Decimal
    from typing import Any
    from uuid import UUID

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    payload = {
        "summary": {
            "first_post_at": datetime(2026, 10, 21, tzinfo=UTC),
            "catalog_last_post_at": datetime(2026, 10, 21, 12, 30, 42, 123456, tzinfo=UTC),
        },
        "recent_runs": [{"created_at": datetime(2026, 10, 21), "id": UUID("e9161955-6ee4-4985-865e-3386a0f670fb")}],
        "day": date(2026, 10, 21),
        "ratio": Decimal("1.25"),
        "empty": None,
    }
    app = FastAPI()

    @app.get("/uncached")
    def uncached() -> dict[str, Any]:
        return payload

    ticket, _ = cache.begin(("dashboard", "instagram", "bravo"))
    cache.publish(ticket, payload, 30)
    cached = cache.begin(("dashboard", "instagram", "bravo"))[1]

    @app.get("/cached")
    def cached_route() -> dict[str, Any]:
        assert cached is not None
        return cached

    with TestClient(app) as client:
        original = client.get("/uncached")
        restored = client.get("/cached")
        assert original.status_code == restored.status_code == 200
        assert original.content == restored.content
    assert payload["summary"]["first_post_at"] == datetime(2026, 10, 21, tzinfo=UTC)
