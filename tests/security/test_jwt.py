from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest

from trr_backend.security.jwt import (
    InvalidTokenError,
    describe_supabase_jwt_context,
    verify_jwt_token,
)


def _make_token(
    secret: str,
    subject: str,
    expires_delta: timedelta,
    *,
    issuer: str | None = None,
    extra_claims: dict[str, object] | None = None,
) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
    }
    if issuer:
        payload["iss"] = issuer
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, secret, algorithm="HS256")


def test_verify_jwt_token_accepts_legacy_supabase_service_role_issuer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="supabase",
        extra_claims={"ref": "project123", "role": "service_role"},
    )

    payload = verify_jwt_token(token)

    assert payload["role"] == "service_role"
    assert payload["iss"] == "supabase"
    assert payload["ref"] == "project123"


def test_verify_jwt_token_accepts_expected_project_issuer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.delenv("SUPABASE_JWT_ISSUER", raising=False)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "user-1",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "role": "authenticated"},
    )

    payload = verify_jwt_token(token)

    assert payload["iss"] == "https://project123.supabase.co/auth/v1"
    assert payload["ref"] == "project123"


def test_verify_jwt_token_does_not_enforce_audience_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.delenv("SUPABASE_JWT_ISSUER", raising=False)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "user-1",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"aud": "unexpected-audience", "ref": "project123", "role": "authenticated"},
    )

    payload = verify_jwt_token(token)

    assert payload["aud"] == "unexpected-audience"


def test_verify_jwt_token_reports_bad_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")

    token = _make_token("wrong-secret-32-bytes-minimum-abcdef", "user-1", timedelta(minutes=5))

    with pytest.raises(InvalidTokenError, match="signature does not match"):
        verify_jwt_token(token)


def test_verify_jwt_token_reports_wrong_issuer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="https://other.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "role": "service_role"},
    )

    with pytest.raises(InvalidTokenError, match="issuer does not match"):
        verify_jwt_token(token)


def test_verify_jwt_token_reports_wrong_project_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "wrong-project", "role": "service_role"},
    )

    with pytest.raises(InvalidTokenError, match="project ref does not match"):
        verify_jwt_token(token)


def test_describe_supabase_jwt_context_warns_on_legacy_supabase_issuer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_ISSUER", "supabase")
    warnings = describe_supabase_jwt_context()

    assert any("legacy compatibility" in warning for warning in warnings)


def test_describe_supabase_jwt_context_warns_on_conflicting_project_refs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.setenv("TRR_CORE_SUPABASE_URL", "https://project456.supabase.co")

    warnings = describe_supabase_jwt_context()

    assert any("Conflicting Supabase project ref candidates" in warning for warning in warnings)
