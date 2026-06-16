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
    _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV,
    _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV,
    _resolve_public_child_timeout_seconds,
    _resolve_public_post_timeout_seconds,
    _resolve_public_zero_reply_probe_limit,
)


def _clear_env() -> dict[str, str]:
    keep = dict(os.environ)
    for name in (
        _PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT_ENV,
        _PUBLIC_COMMENTS_POST_TIMEOUT_SEC_ENV,
        _PUBLIC_COMMENTS_CHILD_TIMEOUT_SEC_ENV,
    ):
        keep.pop(name, None)
    return keep


def test_defaults_when_unset():
    with patch.dict(os.environ, _clear_env(), clear=True):
        assert _resolve_public_zero_reply_probe_limit() == 0
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
