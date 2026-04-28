"""Dispatcher tests. Assert routing order + fall-through on RuntimeUnsupported."""

from __future__ import annotations

import asyncio

import pytest

from trr_backend.socials.instagram.runtimes.dispatcher import InstagramRuntimeDispatcher
from trr_backend.socials.instagram.runtimes.protocol import (
    Post,
    PostDetail,
    ProfileInfo,
    RuntimeHealth,
    RuntimeUnsupported,
)
from trr_backend.socials.instagram.runtimes.scrapling_runtime import ScraplingRuntime


def _run(coro):
    return asyncio.run(coro)


class _RuntimeStub:
    def __init__(
        self,
        name: str,
        *,
        healthy: bool = True,
        profile_behavior: str = "ok",
        posts_behavior: str = "ok",
        detail_behavior: str = "ok",
    ) -> None:
        self.name = name
        self.healthy = healthy
        self._profile_behavior = profile_behavior
        self._posts_behavior = posts_behavior
        self._detail_behavior = detail_behavior
        self.fetch_profile_calls = 0
        self.fetch_posts_calls = 0
        self.fetch_post_detail_calls = 0

    def healthcheck(self) -> RuntimeHealth:
        return RuntimeHealth(healthy=self.healthy, reason=None if self.healthy else "unhealthy")

    async def fetch_profile(self, username: str) -> ProfileInfo:
        self.fetch_profile_calls += 1
        if self._profile_behavior == "unsupported":
            raise RuntimeUnsupported(f"{self.name} profile unsupported")
        return ProfileInfo(username=username, user_id="1", is_private=False, is_verified=False)

    async def fetch_posts(self, username: str, *, limit: int) -> list[Post]:
        self.fetch_posts_calls += 1
        if self._posts_behavior == "unsupported":
            raise RuntimeUnsupported(f"{self.name} posts unsupported")
        return [Post(shortcode="abc")]

    async def fetch_post_detail(self, shortcode: str) -> PostDetail:
        self.fetch_post_detail_calls += 1
        if self._detail_behavior == "unsupported":
            raise RuntimeUnsupported(f"{self.name} detail unsupported")
        return PostDetail(post=Post(shortcode=shortcode))


def test_dispatches_to_first_runtime_in_order() -> None:
    first = _RuntimeStub("first")
    second = _RuntimeStub("second")
    disp = InstagramRuntimeDispatcher(
        factories={"first": lambda: first, "second": lambda: second},
        order=["first", "second"],
    )
    result = _run(disp.fetch_profile("bravotv"))
    assert result.username == "bravotv"
    assert first.fetch_profile_calls == 1
    assert second.fetch_profile_calls == 0


def test_falls_through_on_runtime_unsupported() -> None:
    first = _RuntimeStub("first", profile_behavior="unsupported")
    second = _RuntimeStub("second")
    disp = InstagramRuntimeDispatcher(
        factories={"first": lambda: first, "second": lambda: second},
        order=["first", "second"],
    )
    result = _run(disp.fetch_profile("bravotv"))
    assert result.username == "bravotv"
    assert first.fetch_profile_calls == 1
    assert second.fetch_profile_calls == 1


def test_skips_unhealthy_runtimes() -> None:
    sick = _RuntimeStub("sick", healthy=False)
    healthy = _RuntimeStub("healthy")
    disp = InstagramRuntimeDispatcher(
        factories={"sick": lambda: sick, "healthy": lambda: healthy},
        order=["sick", "healthy"],
    )
    _run(disp.fetch_profile("u"))
    assert sick.fetch_profile_calls == 0
    assert healthy.fetch_profile_calls == 1


def test_skips_unhealthy_scrapling_scaffold_and_uses_fallback() -> None:
    fallback = _RuntimeStub("fallback")
    disp = InstagramRuntimeDispatcher(
        factories={"scrapling": ScraplingRuntime, "fallback": lambda: fallback},
        order=["scrapling", "fallback"],
    )

    result = _run(disp.fetch_profile("bravotv"))

    assert result.username == "bravotv"
    assert fallback.fetch_profile_calls == 1


def test_respects_disabled_set() -> None:
    first = _RuntimeStub("first")
    second = _RuntimeStub("second")
    disp = InstagramRuntimeDispatcher(
        factories={"first": lambda: first, "second": lambda: second},
        order=["first", "second"],
        disabled={"first"},
    )
    _run(disp.fetch_profile("u"))
    assert first.fetch_profile_calls == 0
    assert second.fetch_profile_calls == 1


def test_raises_when_all_unsupported() -> None:
    first = _RuntimeStub("first", detail_behavior="unsupported")
    second = _RuntimeStub("second", detail_behavior="unsupported")
    disp = InstagramRuntimeDispatcher(
        factories={"first": lambda: first, "second": lambda: second},
        order=["first", "second"],
    )
    with pytest.raises(RuntimeUnsupported):
        _run(disp.fetch_post_detail("xyz"))
