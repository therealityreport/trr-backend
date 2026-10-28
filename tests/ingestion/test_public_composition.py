"""Frozen public/authenticated adapter routing; no environment or source access."""

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from tests.ingestion.test_composition import avatar_media_handoff as avatar_case_fixture
from trr_backend.ingestion import comment_transport, composition, post_transport, public_transport
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import RunIntent, SourceProfile

avatar_case = avatar_case_fixture


def context(mode=None) -> dict[str, Any]:
    intent = RunIntent(account="bravotv")
    payload = intent.payload()
    if mode is not None:
        payload["source_profile"] = asdict(SourceProfile.for_mode(mode))
    return {
        "run": {"intent": payload},
        "target": {"owner_expires_at": datetime.now(UTC) + timedelta(minutes=5)},
        "post": {"comments_count": 7},
    }


@pytest.mark.parametrize("mode", [None, "public", "authenticated"])
def test_frozen_adapter_routing_ignores_worker_environment(monkeypatch, mode):
    # Deliberately opposite process mode; workers must use only frozen intent.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated" if mode == "public" else "public_first")
    captures = []
    for module, name in (
        (public_transport, "PublicListingSource"),
        (public_transport, "PublicDetailSource"),
        (comment_transport, "PublicRelayCommentSource"),
        (post_transport, "ScraplingListingSource"),
        (post_transport, "DetailSource"),
        (comment_transport, "ScraplingCommentSource"),
    ):
        monkeypatch.setattr(
            module, name, lambda _name=name, **kw: captures.append((_name, kw)) or SimpleNamespace(**kw)
        )
    monkeypatch.setattr(
        composition, "Store", lambda: SimpleNamespace(transaction=object(), check_owner=lambda **kw: None)
    )
    monkeypatch.setattr(composition, "_public_source_parts", lambda *_: {"gate": "public-gate"})
    monkeypatch.setattr(composition, "_gate_source", lambda source, *args, **kw: source)
    credentials = []

    def credential(account) -> dict[str, Any]:
        assert mode != "public", "public lane selected credentials"
        credentials.append(account)
        return {
            "cookies": [],
            "raw_cookies": {"sessionid": "synthetic", "ds_user_id": "123"},
            "browser_account_id": "fixture_ingest_account",
        }

    monkeypatch.setattr(composition, "_credentials", credential)
    owned = {"account": "bravotv", "target_key": "ABC"}
    actual_context = context(mode)
    composition._listing(actual_context, owned)
    composition._details(actual_context, owned)
    composition._comments(actual_context, owned)
    assert [name for name, _ in captures] == (
        ["PublicListingSource", "PublicDetailSource", "PublicRelayCommentSource"]
        if mode == "public"
        else ["ScraplingListingSource", "DetailSource", "ScraplingCommentSource"]
    )
    assert credentials == ([] if mode == "public" else ["bravotv"] * 3)
    assert captures[-1][1]["expected_count"] == 7


def test_ordered_public_factory_uses_only_explicit_frozen_profile_and_same_gate(monkeypatch):
    from trr_backend.ingestion import ordered_listing_transport

    captured = []
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")
    monkeypatch.setattr(composition, "_public_source_parts", lambda *_: {"gate": "same-public-gate"})
    monkeypatch.setattr(
        composition, "_credentials", lambda *_: pytest.fail("ordered public family must not load credentials")
    )
    monkeypatch.setattr(
        ordered_listing_transport,
        "OrderedListingSource",
        lambda **kw: captured.append(kw) or SimpleNamespace(**kw),
    )
    actual = context("public")
    actual["run"]["intent"]["source_profile"] = asdict(SourceProfile.for_public_ordered())
    actual["run"]["intent"].update(
        scope="bounded_window", date_start="2026-01-01T00:00:00Z", date_end="2026-02-01T00:00:00Z"
    )
    actual["target"]["checkpoint"] = {"ordered_profile": {"id": "opaque-native-id", "pk": "123"}}
    result = composition._listing(actual, {"account": "bravotv", "target_key": "page:root"})
    assert result.gate == "same-public-gate"
    assert captured == [
        {"account": "bravotv", "profile": {"id": "opaque-native-id", "pk": "123"}, "gate": "same-public-gate"}
    ]


@pytest.mark.parametrize("value", [None, "public_first", "public", "logged_out", "no-login", ""])
def test_new_launch_uses_maintained_public_default_and_aliases(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", raising=False)
    else:
        monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", value)
    assert composition._configured_source_profile() == SourceProfile.for_mode("public")


@pytest.mark.parametrize("value", ["authenticated", "cookies", "logged_in"])
def test_new_launch_authentication_requires_explicit_maintained_alias(monkeypatch, value):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", value)
    assert composition._configured_source_profile() == SourceProfile.for_mode("authenticated")


def test_invalid_source_mode_fails_closed_before_run_creation(monkeypatch):
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "unknown-policy")
    with pytest.raises(ServiceUnavailableError, match="Unsupported Instagram source mode"):
        composition._configured_source_profile()


def test_authenticated_www_relay_is_explicit_opt_in_and_worker_uses_frozen_family(monkeypatch):
    from trr_backend.ingestion import authenticated_comment_transport

    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")
    monkeypatch.setenv("TRR_INSTAGRAM_INGESTION_AUTH_COMMENTS_TRANSPORT", "www_relay")
    assert composition._configured_source_profile() == SourceProfile.for_authenticated_relay()
    captures = []
    monkeypatch.setattr(
        authenticated_comment_transport,
        "AuthenticatedRelayCommentSource",
        lambda **kw: captures.append(kw) or "www-source",
    )
    monkeypatch.setattr(composition, "_credentials", lambda account: {"raw_cookies": {}})
    monkeypatch.setattr(composition, "_gate_source", lambda source, *a, **kw: source)
    ctx = context("authenticated")
    ctx["run"]["intent"]["source_profile"] = asdict(SourceProfile.for_authenticated_relay())
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "public_first")
    assert composition._comments(ctx, {"account": "bravotv"}) == "www-source"
    assert captures[0]["expected_count"] == 7
    assert composition._configured_source_profile() == SourceProfile.for_mode("public")


def test_public_gate_is_shared_public_identity_and_reuses_owner_transaction(monkeypatch):
    from trr_backend.ingestion import identity_policy

    calls = []
    transaction = object()
    store = SimpleNamespace(transaction=transaction, check_owner=lambda **kw: calls.append(kw))
    monkeypatch.setattr(composition, "Store", lambda: store)
    monkeypatch.setattr(identity_policy, "SourceRequestGate", lambda **kw: SimpleNamespace(**kw))
    owned = {"account": "bravotv", "stage": "listing", "target_key": "page:root"}
    parts = composition._public_source_parts(context("public"), owned)
    assert parts["gate"].public_only is True and parts["gate"].cookies == {}
    assert parts["gate"].transaction is transaction
    parts["check_cancelled"]()
    connection = object()
    parts["gate"].check_cancelled_in_transaction(connection)
    assert calls == [owned, dict(owned, connection=connection)]


@pytest.mark.parametrize("placement", [None, "modal", "local"])
def test_public_avatar_resolution_uses_no_credentials(avatar_case, placement):
    # The existing read/resolve/read fixture also asserts source closure and fencing.
    case = avatar_case
    case.context["run"]["intent"]["source_profile"] = asdict(SourceProfile.for_mode("public"))
    if placement is not None:
        case.context["run"]["execution_placement"] = placement
    assert composition._load_media(case.loader, case.store, case.context, case.owned, **case.callbacks) is case.result
    assert case.state.credentials == []
    assert case.state.sources[0].kwargs["cookies"] == {}
    assert case.state.gates[0]["public_only"] is True
    # Logged-out avatar discovery shares the egress lane of listing/comments.
    assert case.state.gates[0]["network_lane"] == (placement or "modal")


def test_authenticated_avatar_resolution_keeps_no_network_lane(avatar_case):
    case = avatar_case
    case.context["run"]["execution_placement"] = "local"
    assert composition._load_media(case.loader, case.store, case.context, case.owned, **case.callbacks) is case.result
    assert case.state.gates[0]["public_only"] is False
    assert case.state.gates[0]["network_lane"] is None


@pytest.mark.parametrize("placement", ["modal", "local"])
def test_public_commenter_profile_gate_uses_run_placement_lane(monkeypatch, placement):
    from trr_backend.ingestion import avatar_transport, identity_policy

    store = SimpleNamespace(transaction=object(), check_owner=lambda **kw: None)
    monkeypatch.setattr(composition, "_worker_store", lambda ctx: store)
    monkeypatch.setattr(identity_policy, "SourceRequestGate", lambda **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(avatar_transport, "AvatarProfileSource", lambda **kw: SimpleNamespace(**kw))
    ctx = context("public")
    ctx["run"]["execution_placement"] = placement
    ctx["target"]["checkpoint"] = {}
    _, gate = composition._profile_provider(ctx, {"account": "bravotv", "stage": "profiles", "target_key": "writer"})
    assert gate.public_only is True and gate.cookies == {}  # pyright: ignore[reportAttributeAccessIssue]
    assert gate.network_lane == placement


@pytest.mark.parametrize(
    "mode,placement,lane",
    [("public", "local", "local"), ("public", None, "modal"), ("authenticated", None, None)],
)
def test_media_storage_refresh_uses_run_placement_lane_only_when_public(monkeypatch, mode, placement, lane):
    import boto3

    from trr_backend.ingestion import media, media_refresh
    from trr_backend.media import s3_mirror

    captured = {}
    config = SimpleNamespace(
        profile_name=None,
        region="auto",
        endpoint_url=None,
        access_key_id=None,
        secret_access_key=None,
        session_token=None,
        bucket="bucket",
        prefix="prefix",
    )
    monkeypatch.setattr(s3_mirror, "get_hosted_media_storage_config", lambda: config)
    monkeypatch.setattr(boto3, "Session", lambda **kw: SimpleNamespace(client=lambda *a, **k: object()))
    monkeypatch.setattr(composition, "_worker_store", lambda ctx: object())
    monkeypatch.setattr(media_refresh, "MediaRefresh", lambda **kw: captured.update(kw) or "refresh")
    monkeypatch.setattr(media, "MediaStorage", lambda **kw: kw)
    ctx = context(mode)
    if placement is not None:
        ctx["run"]["execution_placement"] = placement
    storage = composition._media_storage(ctx, {"account": "bravotv"})
    assert isinstance(storage, dict)
    assert storage["refresh"] == "refresh"
    assert captured["public_only"] is (mode == "public")
    assert captured["network_lane"] == lane
