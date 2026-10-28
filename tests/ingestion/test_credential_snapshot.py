"""Explicit fixture credentials only; reject any ambient auth or HTTP creation."""

import json

import httpx
import pytest
import requests

from trr_backend.ingestion import composition
from trr_backend.ingestion.credential_snapshot import CredentialSnapshot
from trr_backend.ingestion.post_transport import SourceFailure


@pytest.fixture(autouse=True)
def no_http_clients(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unexpected HTTP client construction")

    monkeypatch.setattr(requests, "Session", forbidden)
    monkeypatch.setattr(httpx, "Client", forbidden)
    monkeypatch.setattr(httpx, "AsyncClient", forbidden)


def environment():
    return {
        "SOCIAL_INSTAGRAM_COOKIES_JSON": json.dumps(
            {"sessionid": "synthetic-private-session", "csrftoken": "synthetic-csrf", "ds_user_id": "123456"}
        ),
        "TRR_INSTAGRAM_INGESTION_AUTH_USER_ID": "123456",
        "TRR_INSTAGRAM_INGESTION_AUTH_USERNAME": "fixture_ingest_account",
    }


def test_explicit_snapshot_preserves_source_shape_without_exposing_credentials():
    snapshot = CredentialSnapshot.from_environ(environment())
    kwargs = snapshot.source_kwargs()
    assert kwargs["browser_account_id"] == "fixture_ingest_account"
    assert kwargs["raw_cookies"]["ds_user_id"] == "123456"
    assert kwargs["cookies"][0] == {
        "name": "sessionid",
        "value": "synthetic-private-session",
        "domain": ".instagram.com",
        "path": "/",
    }
    assert "synthetic-private-session" not in repr(snapshot) + repr(kwargs) + str(kwargs["raw_cookies"])
    kwargs["raw_cookies"]["sessionid"] = "mutated"
    kwargs["cookies"][0]["value"] = "mutated"
    assert snapshot.source_kwargs()["raw_cookies"]["sessionid"] == "synthetic-private-session"


@pytest.mark.parametrize(
    "key,value",
    [
        ("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME", ""),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME", "someone@thereality.report"),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME", "@fixture_ingest_account"),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USERNAME", "Livestnation"),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USER_ID", ""),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USER_ID", "654321"),
        ("TRR_INSTAGRAM_INGESTION_AUTH_USER_ID", "not-numeric"),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", ""),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", "synthetic-private-malformed-json"),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", "[]"),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", '{"sessionid":"private"}'),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", '{"sessionid":"private","sessionid":"duplicate"}'),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", '{"sessionid":123,"csrftoken":"x","ds_user_id":"123456"}'),
        ("SOCIAL_INSTAGRAM_COOKIES_JSON", '{"sessionid":"private\\nheader","csrftoken":"x","ds_user_id":"123456"}'),
    ],
)
def test_missing_or_mismatched_explicit_identity_fails_closed(key, value, caplog):
    env = environment() | {key: value}
    with pytest.raises(SourceFailure) as caught:
        CredentialSnapshot.from_environ(env)
    assert caught.value.failure.code == "credential_snapshot_unavailable"
    assert caught.value.failure.identity_blocked
    assert "private" not in str(caught.value) + repr(caught.value) + caplog.text


def test_composition_credentials_never_select_resolver_or_environment_alias(monkeypatch):
    from trr_backend.socials.instagram import auth_resolver

    monkeypatch.setattr(
        auth_resolver, "resolve_instagram_auth_session", lambda **kwargs: pytest.fail("ambient resolver selected")
    )
    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("INSTAGRAM_COOKIES_JSON", '{"sessionid":"forbidden-default"}')
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "forbidden@thereality.report")
    assert composition._credentials("bravotv")["browser_account_id"] == "fixture_ingest_account"
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COOKIES_JSON")
    with pytest.raises(SourceFailure, match="credential_snapshot_unavailable"):
        composition._credentials("bravotv")


def test_all_rebuilt_lane_factories_receive_pinned_identity_and_details_use_explicit_client(monkeypatch):
    from trr_backend.ingestion import comment_transport, post_transport
    from trr_backend.ingestion.media_refresh import ExplicitDetailClient

    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    captured = {}
    for module, name in [
        (post_transport, "ScraplingListingSource"),
        (post_transport, "DetailSource"),
        (comment_transport, "ScraplingCommentSource"),
    ]:
        monkeypatch.setattr(module, name, lambda _name=name, **kwargs: captured.setdefault(_name, kwargs))
    monkeypatch.setattr(composition, "_gate_source", lambda source, *args, **kwargs: source)
    owned = {"account": "bravotv", "target_key": "ABC"}
    context = {"run": {"intent": {"account": "bravotv"}}, "post": {"comments_count": 0}}
    composition._listing(context, owned)
    composition._details(context, owned)
    composition._comments(context, owned)
    assert {kwargs["browser_account_id"] for kwargs in captured.values()} == {"fixture_ingest_account"}
    assert captured["DetailSource"]["scraper_factory"] is ExplicitDetailClient
    assert captured["DetailSource"]["cookies"]["ds_user_id"] == "123456"


def test_media_storage_composes_lazy_target_bound_refresh_without_credentials_or_provider_calls(monkeypatch):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    import boto3

    from trr_backend.ingestion.domain import Stage
    from trr_backend.ingestion.media_refresh import MediaRefresh
    from trr_backend.media import s3_mirror

    def unexpected(*args, **kwargs):
        pytest.fail("storage composition selected source credentials")

    calls = []
    client = object()
    monkeypatch.setattr(composition, "_credentials", unexpected)
    monkeypatch.setattr(
        boto3,
        "Session",
        lambda **kwargs: SimpleNamespace(client=lambda service, **options: calls.append((service, options)) or client),
    )
    monkeypatch.setattr(
        s3_mirror,
        "get_hosted_media_storage_config",
        lambda: SimpleNamespace(
            profile_name=None,
            region="us-east-1",
            endpoint_url="https://fixture-storage.invalid",
            access_key_id=None,
            secret_access_key=None,
            session_token=None,
            bucket="fixture",
            prefix="snapshot",
        ),
    )
    owned = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.MEDIA,
        "target_key": "ABC",
        "owner_token": "private",
    }
    expires = datetime.now(UTC) + timedelta(minutes=5)
    storage = composition._media_storage(
        {"run": {"intent": {"account": "bravotv"}}, "target": {"owner_expires_at": expires}}, owned
    )
    # Storage I/O runs in a bounded child process (98ccb8ed); composition builds
    # no in-process SDK client and never selects source credentials.
    from trr_backend.ingestion.media_storage_io import BoundedS3Client

    assert isinstance(storage.client, BoundedS3Client) and isinstance(storage.refresh, MediaRefresh)
    assert storage.refresh.owned == owned and storage.refresh.owner_expires_at == expires
    assert storage.refresh._snapshot is None and storage.refresh._credentials_snapshot is None
    assert calls == [] and client is not None
    spec = storage.client._spec
    assert spec["bucket"] == "fixture" and spec["config"]["retries"] == {"total_max_attempts": 1}
    assert spec["client_options"]["endpoint_url"] == "https://fixture-storage.invalid"
    assert "aws_access_key_id" not in spec["client_options"]
