from __future__ import annotations

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.instagram.network_policy import (
    STATIC_CDN_HOST,
    InstagramNetworkPolicy,
    InstagramNetworkPolicyRecorder,
    instagram_scrapling_network_kwargs,
)


class _FakeRequest:
    def __init__(self, url: str, resource_type: str) -> None:
        self.url = url
        self.resource_type = resource_type


class _FakeRoute:
    def __init__(self, url: str, resource_type: str) -> None:
        self.request = _FakeRequest(url, resource_type)
        self.aborted = False
        self.continued = False

    def abort(self) -> None:
        self.aborted = True

    def continue_(self) -> None:
        self.continued = True


class _FakeContext:
    def __init__(self) -> None:
        self.routes: list[tuple[str, object]] = []

    def route(self, pattern: str, handler: object) -> None:
        self.routes.append((pattern, handler))

    def add_cookies(self, cookies: list[dict[str, str]]) -> None:
        self.cookies_added = cookies

    def storage_state(self) -> dict[str, list[object]]:
        return {"cookies": [], "origins": []}

    def cookies(self) -> list[dict[str, str]]:
        return []

    def close(self) -> None:
        self.closed = True


class _FakeBrowser:
    def __init__(self, context: _FakeContext) -> None:
        self.context = context

    def new_context(self, **kwargs: object) -> _FakeContext:
        self.context_kwargs = kwargs
        return self.context

    def close(self) -> None:
        self.closed = True


def test_instagram_network_policy_blocks_static_cdn_and_extra_resources() -> None:
    policy = InstagramNetworkPolicy()

    static_decision = policy.decide(f"https://{STATIC_CDN_HOST}/rsrc.php/v4/yA/r/app.js", "script")
    api_decision = policy.decide("https://www.instagram.com/graphql/query", "xhr")
    media_decision = policy.decide("https://www.instagram.com/example.jpg", "image")

    assert static_decision.blocked
    assert static_decision.reason == "blocked_domain"
    assert not api_decision.blocked
    assert media_decision.blocked
    assert media_decision.reason == "blocked_resource_type"


def test_instagram_scrapling_network_kwargs_enable_native_resource_blocks() -> None:
    kwargs = instagram_scrapling_network_kwargs(policy=InstagramNetworkPolicy())

    assert kwargs["disable_resources"] is True
    assert kwargs["block_ads"] is True
    assert STATIC_CDN_HOST in kwargs["blocked_domains"]


def test_account_browser_session_installs_instagram_network_policy(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    fake_context = _FakeContext()
    fake_browser = _FakeBrowser(fake_context)
    monkeypatch.setattr(
        "trr_backend.socials.account_browser_sessions.launch_browser",
        lambda playwright, *, headless: fake_browser,
    )

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))

    with manager.account_context(playwright=object(), account_id="bravotv", headless=True) as handle:
        assert handle.network_policy_recorder is not None
        assert fake_context.routes
        handler = fake_context.routes[0][1]
        static_route = _FakeRoute(f"https://{STATIC_CDN_HOST}/bundle.js", "script")
        api_route = _FakeRoute("https://www.instagram.com/graphql/query", "xhr")
        handler(static_route)  # type: ignore[misc]
        handler(api_route)  # type: ignore[misc]

        metadata = handle.network_policy_recorder.to_metadata()

    assert static_route.aborted
    assert not static_route.continued
    assert api_route.continued
    assert not api_route.aborted
    assert metadata["blocked_request_count_by_host"][STATIC_CDN_HOST] == 1
    assert metadata["request_count_by_host"]["www.instagram.com"] == 1


def test_report_only_policy_records_but_does_not_abort() -> None:
    policy = InstagramNetworkPolicy(mode="report_only")
    recorder = InstagramNetworkPolicyRecorder(policy=policy)
    decision = policy.decide(f"https://{STATIC_CDN_HOST}/bundle.js", "script")

    recorder.record_decision(decision)

    assert decision.blocked
    assert not policy.enforcing
    assert recorder.to_metadata()["blocked_request_count_by_host"][STATIC_CDN_HOST] == 1
