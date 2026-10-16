"""Frozen public/authenticated adapter routing; no environment or source access."""

from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from tests.ingestion.test_composition import avatar_media_handoff as avatar_case_fixture
from trr_backend.ingestion import comment_transport, composition, post_transport, public_transport
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import RunIntent, SourceProfile

avatar_case = avatar_case_fixture


def context(mode=None):
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

    def credential(account):
        assert mode != "public", "public lane selected credentials"
        credentials.append(account)
        return {
            "cookies": [],
            "raw_cookies": {"sessionid": "synthetic", "ds_user_id": "123"},
            "browser_account_id": "livestnation",
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


def test_public_avatar_resolution_uses_no_credentials(avatar_case):
    # The existing read/resolve/read fixture also asserts source closure and fencing.
    case = avatar_case
    case.context["run"]["intent"]["source_profile"] = asdict(SourceProfile.for_mode("public"))
    assert composition._load_media(case.loader, case.store, case.context, case.owned, **case.callbacks) is case.result
    assert case.state.credentials == []
    assert case.state.sources[0].kwargs["cookies"] == {}
    assert case.state.gates[0]["public_only"] is True
