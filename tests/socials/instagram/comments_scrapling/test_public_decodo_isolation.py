"""The public_relay comments lane must never use Decodo, cookies, or auth.

These cover the explicit hard guard (``assert_public_comments_isolation``) that
converts the previously-implicit structural isolation into an enforced,
fail-closed invariant. The guard checks RESOLVED state, not env presence, so
Decodo credentials configured for other lanes must not trip it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from trr_backend.socials.instagram.comments_scrapling.public_mode import (
    PUBLIC_PROXY_ENABLED_ENV,
    PublicCommentsModeViolation,
    assert_public_comments_isolation,
    public_proxy_enabled,
)


def _public_session() -> SimpleNamespace:
    return SimpleNamespace(
        cookies=[],
        browser_account_id="bravotv",
        auth_session=SimpleNamespace(cookies={}, metadata={}, source="public", browser_account_id=None),
    )


def test_clean_public_state_passes_and_returns_proof():
    proof = assert_public_comments_isolation(
        proxy_config=None,
        session=_public_session(),
        account_handle="bravotv",
    )
    assert proof["no_proxy"] is True
    assert proof["no_cookies"] is True
    assert proof["no_auth_fallback"] is True
    assert proof["proxy_state"] == "none"
    assert proof["auth_state"] == "public"


def test_clean_public_state_passes_even_with_decodo_env_present(monkeypatch):
    # Decodo creds exist for the authenticated lanes; the public guard must not
    # fail merely because they are configured — it checks resolved state.
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proof = assert_public_comments_isolation(
        proxy_config=None,
        session=_public_session(),
        account_handle="bravotv",
    )
    assert proof["no_proxy"] is True


def test_resolved_proxy_config_is_rejected():
    decodo_like = SimpleNamespace(
        fingerprint="gate.decodo.com:7000:decodo",
        api_proxy_url="http://user:pass@gate.decodo.com:7000",
    )
    with pytest.raises(PublicCommentsModeViolation) as excinfo:
        assert_public_comments_isolation(
            proxy_config=decodo_like,
            session=_public_session(),
            account_handle="bravotv",
        )
    assert "proxy_config is set" in str(excinfo.value)


def test_non_empty_cookies_are_rejected():
    leaked = SimpleNamespace(
        cookies=[{"name": "sessionid", "value": "x"}],
        browser_account_id="bravotv",
        auth_session=SimpleNamespace(
            cookies={"sessionid": "x"}, metadata={}, source="auth", browser_account_id="bravotv"
        ),
    )
    with pytest.raises(PublicCommentsModeViolation) as excinfo:
        assert_public_comments_isolation(proxy_config=None, session=leaked, account_handle="bravotv")
    message = str(excinfo.value)
    assert "session.cookies is non-empty" in message
    assert "auth_session.cookies is non-empty" in message


def test_authenticated_session_source_is_rejected():
    authed = SimpleNamespace(
        cookies=[],
        browser_account_id="bravotv",
        auth_session=SimpleNamespace(cookies={}, metadata={}, source="cookie_pool", browser_account_id="bravotv"),
    )
    with pytest.raises(PublicCommentsModeViolation) as excinfo:
        assert_public_comments_isolation(proxy_config=None, session=authed, account_handle="bravotv")
    assert "auth_session.source" in str(excinfo.value)


def test_public_proxy_enabled_reads_flag(monkeypatch):
    monkeypatch.delenv(PUBLIC_PROXY_ENABLED_ENV, raising=False)
    assert public_proxy_enabled() is False
    monkeypatch.setenv(PUBLIC_PROXY_ENABLED_ENV, "1")
    assert public_proxy_enabled() is True
    monkeypatch.setenv(PUBLIC_PROXY_ENABLED_ENV, "nope")
    assert public_proxy_enabled() is False


def test_allow_proxy_permits_proxy_on_public_lane():
    # Invariant B relaxed: a budgeted proxy is permitted when allow_proxy=True.
    decodo_like = SimpleNamespace(
        fingerprint="gate.decodo.com:7000:decodo:abc123def456",
        api_proxy_url="http://user:pass@gate.decodo.com:7000",
    )
    proof = assert_public_comments_isolation(
        proxy_config=decodo_like,
        session=_public_session(),
        account_handle="bravotv",
        allow_proxy=True,
    )
    assert proof["no_proxy"] is False
    assert proof["proxy_state"] == "budgeted_public_proxy"
    # Invariant A still proven: no cookies, no auth fallback.
    assert proof["no_cookies"] is True
    assert proof["no_auth_fallback"] is True


def test_allow_proxy_still_blocks_cookies_and_auth():
    # Invariant A is unconditional: cookies/auth are rejected even with allow_proxy.
    leaked = SimpleNamespace(
        cookies=[{"name": "sessionid", "value": "x"}],
        browser_account_id="bravotv",
        auth_session=SimpleNamespace(
            cookies={"sessionid": "x"}, metadata={}, source="auth", browser_account_id="bravotv"
        ),
    )
    decodo_like = SimpleNamespace(fingerprint="gw:decodo", api_proxy_url="http://u:p@gw")
    with pytest.raises(PublicCommentsModeViolation) as excinfo:
        assert_public_comments_isolation(
            proxy_config=decodo_like, session=leaked, account_handle="bravotv", allow_proxy=True
        )
    message = str(excinfo.value)
    assert "session.cookies is non-empty" in message
    # The permitted proxy must NOT be reported as a violation.
    assert "proxy_config is set" not in message
