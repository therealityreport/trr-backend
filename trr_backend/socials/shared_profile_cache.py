"""Bounded, optional Redis cache for admin-only profile read models.

Generation checks make invalidation independent of pub/sub delivery. Redis is
never authoritative: an unavailable cache is a miss, and failed invalidation is
bounded by the 30-second payload TTL. No exception text or payload is logged.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any
from uuid import uuid4

import redis
from pydantic import TypeAdapter
from redis.backoff import NoBackoff
from redis.retry import Retry

logger = logging.getLogger(__name__)
MAX_PAYLOAD_BYTES = 512 * 1024
MAX_TTL_SECONDS = 30
# Match FastAPI response_model=dict[str, Any], including datetime/Decimal/UUID.
_RESPONSE_ADAPTER = TypeAdapter(dict[str, Any])
_READ = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return nil end
return redis.call('GET', KEYS[2])
"""
_WRITE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
return 1
"""


@dataclass(frozen=True)
class Ticket:
    generation_key: str
    payload_key: str
    generation: str


def enabled_for(key: tuple[Any, ...]) -> bool:
    return (
        os.getenv("TRR_PROFILE_REDIS_CACHE_ENABLED", "").lower() in {"1", "true"}
        and bool(key)
        and key[0] in {"summary", "dashboard"}
    )


def _prefix() -> str:
    namespace = os.getenv("TRR_CACHE_NAMESPACE", "")
    if not re.fullmatch(r"[a-z0-9_-]{1,64}", namespace):
        raise ValueError("An explicit cache environment namespace is required")
    return f"trr:{namespace}:admin:profile:v1"


@lru_cache(maxsize=1)
def _client() -> redis.Redis:
    url = os.getenv("REDIS_URL", "")
    if not url.startswith("rediss://"):
        raise ValueError("Profile Redis cache requires TLS")
    return redis.Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=0.3,
        socket_timeout=0.5,
        max_connections=8,
        retry=Retry(NoBackoff(), 0),
        health_check_interval=30,
    )


def begin(key: tuple[Any, ...]) -> tuple[Ticket | None, dict[str, Any] | None]:
    started_at = time.perf_counter()
    try:
        prefix = _prefix()
        client = _client()
        generation_key = f"{prefix}:generation"
        generation = client.get(generation_key)
        if generation is None:
            client.set(generation_key, uuid4().hex, nx=True)
            generation = client.get(generation_key)
        if not isinstance(generation, str):
            return None, None
        digest = hashlib.sha256(json.dumps(key, separators=(",", ":")).encode()).hexdigest()
        ticket = Ticket(generation_key, f"{prefix}:{generation}:{digest}", generation)
        raw = client.eval(_READ, 2, generation_key, ticket.payload_key, generation)
        if raw is not None and len(raw.encode()) <= MAX_PAYLOAD_BYTES:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                logger.debug("profile_cache hit")
                return ticket, payload
        logger.debug("profile_cache miss")
        return ticket, None
    except Exception:
        logger.warning("profile_cache read_unavailable")
        return None, None
    finally:
        logger.info("profile_cache phase=read elapsed_ms=%.1f", (time.perf_counter() - started_at) * 1000)


def publish(ticket: Ticket | None, payload: dict[str, Any], ttl_seconds: int) -> None:
    if ticket is None or ttl_seconds <= 0:
        return
    started_at = time.perf_counter()
    try:
        raw = _RESPONSE_ADAPTER.dump_json(payload)
        if len(raw) > MAX_PAYLOAD_BYTES:
            logger.debug("profile_cache oversized")
            return
        accepted = _client().eval(
            _WRITE,
            2,
            ticket.generation_key,
            ticket.payload_key,
            ticket.generation,
            raw,
            min(ttl_seconds, MAX_TTL_SECONDS),
        )
        logger.debug("profile_cache %s", "published" if accepted else "publication_rejected")
    except Exception:
        logger.warning("profile_cache write_unavailable")
    finally:
        logger.info("profile_cache phase=publish elapsed_ms=%.1f", (time.perf_counter() - started_at) * 1000)


def invalidate() -> None:
    if not enabled_for(("summary",)):
        return
    try:
        _client().set(f"{_prefix()}:generation", uuid4().hex)
    except Exception:
        logger.warning("profile_cache invalidation_unavailable ttl_bound_seconds=30")
