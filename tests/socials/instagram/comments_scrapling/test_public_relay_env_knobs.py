"""PUBLIC-mode relay env knob resolvers (T3/T4).

T3: zero-reply parents are skipped entirely by default (probe limit 0) so a
reply-less parent never issues a child GraphQL probe; a positive override
probes at most that many zero-reply parents.

T4: PUBLIC requests fail fast on independent post/child timeouts (defaults
20.0s / 10.0s) rather than burning the authenticated pagination window.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    _PUBLIC_COMMENTS_CHILD_TIMEOUT_SEC_ENV,
    _PUBLIC_COMMENTS_DECODE_RETRY_LIMIT_ENV,
    _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV,
    _PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT_ENV,
    _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV,
    _RATE_SCOPE_ENV,
    _global_rate_limit_key,
    _resolve_public_child_timeout_seconds,
    _resolve_public_decode_retry_limit,
    _resolve_public_post_timeout_seconds,
    _resolve_public_zero_append_advance_limit,
    _resolve_public_zero_reply_probe_limit,
    _resolve_rate_scope,
)


def _clear_env() -> dict[str, str]:
    keep = dict(os.environ)
    for name in (
        _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV,
        _PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT_ENV,
        _PUBLIC_COMMENTS_DECODE_RETRY_LIMIT_ENV,
        _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV,
        _PUBLIC_COMMENTS_CHILD_TIMEOUT_SEC_ENV,
    ):
        keep.pop(name, None)
    return keep


def test_defaults_when_unset():
    with patch.dict(os.environ, _clear_env(), clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 0
        assert _resolve_public_zero_append_advance_limit() == 3
        assert _resolve_public_decode_retry_limit() == 2
        assert _resolve_public_post_timeout_seconds() == 20.0
        assert _resolve_public_child_timeout_seconds() == 10.0


def test_zero_reply_probe_limit_allows_explicit_zero_and_positive():
    base = _clear_env()
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV: "0"}, clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 0
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV: "3"}, clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 3
    # Negative / garbage clamps to a non-negative floor of 0.
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV: "-5"}, clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 0
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV: "nope"}, clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 0


def test_public_retry_limits_allow_explicit_zero_and_clamp_high_values():
    base = _clear_env()
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT_ENV: "0"}, clear=True):
        assert _resolve_public_zero_append_advance_limit() == 0
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_DECODE_RETRY_LIMIT_ENV: "0"}, clear=True):
        assert _resolve_public_decode_retry_limit() == 0
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT_ENV: "99"}, clear=True):
        assert _resolve_public_zero_append_advance_limit() == 50
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_DECODE_RETRY_LIMIT_ENV: "99"}, clear=True):
        assert _resolve_public_decode_retry_limit() == 10
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT_ENV: "nope"}, clear=True):
        assert _resolve_public_zero_append_advance_limit() == 3
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_DECODE_RETRY_LIMIT_ENV: "nope"}, clear=True):
        assert _resolve_public_decode_retry_limit() == 2


def test_timeout_overrides():
    base = _clear_env()
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV: "5.5"}, clear=True):
        assert _resolve_public_post_timeout_seconds() == 5.5
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_CHILD_TIMEOUT_SEC_ENV: "2.25"}, clear=True):
        assert _resolve_public_child_timeout_seconds() == 2.25
    # Garbage falls back to defaults.
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV: "abc"}, clear=True):
        assert _resolve_public_post_timeout_seconds() == 20.0
    with patch.dict(os.environ, {**base, _PUBLIC_COMMENTS_CHILD_TIMEOUT_SEC_ENV: "abc"}, clear=True):
        assert _resolve_public_child_timeout_seconds() == 10.0


def _clear_rate_scope_env() -> dict[str, str]:
    keep = dict(os.environ)
    keep.pop(_RATE_SCOPE_ENV, None)
    return keep


def test_rate_scope_resolves_to_global_by_default_and_on_garbage():
    base = _clear_rate_scope_env()
    with patch.dict(os.environ, base, clear=True):
        assert _resolve_rate_scope() == "global"
    with patch.dict(os.environ, {**base, _RATE_SCOPE_ENV: "nonsense"}, clear=True):
        assert _resolve_rate_scope() == "global"
    with patch.dict(os.environ, {**base, _RATE_SCOPE_ENV: "per_container"}, clear=True):
        assert _resolve_rate_scope() == "per_container"


def test_global_rate_limit_key_is_unchanged_without_scope_token():
    # Backward-compat guarantee: omitting scope_token must hash identically to the
    # pre-scoping behavior so existing per-account global pacing is untouched.
    no_token = _global_rate_limit_key("bravotv", "none")
    explicit_none = _global_rate_limit_key("bravotv", "none", scope_token=None)
    empty = _global_rate_limit_key("bravotv", "none", scope_token="")
    assert no_token == explicit_none == empty


def test_per_container_token_diversifies_the_no_proxy_key():
    # The core per-egress fix: two containers on the same account+no-proxy must
    # get DISTINCT keys when a per-container token is folded in, and a stable
    # token must be deterministic.
    account, proxy = "bravotv", "none"
    key_a = _global_rate_limit_key(account, proxy, scope_token="container-a")
    key_b = _global_rate_limit_key(account, proxy, scope_token="container-b")
    key_a_again = _global_rate_limit_key(account, proxy, scope_token="container-a")
    base = _global_rate_limit_key(account, proxy)
    assert key_a != key_b
    assert key_a == key_a_again
    assert key_a != base and key_b != base
