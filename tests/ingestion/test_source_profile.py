"""Frozen transport selection and historical intent compatibility."""

from dataclasses import FrozenInstanceError, asdict
from datetime import UTC, datetime

import pytest
from test_command_service import launch, persisted, service
from test_persistence import database as database_fixture

from trr_backend.ingestion.domain import RunIntent, SourceProfile, fingerprint

database = database_fixture


def test_historical_intent_retains_exact_payload_and_digest():
    original = {
        "account": "bravotv",
        "action": "backfill",
        "source_scope": "network",
        "date_start": None,
        "date_end": None,
        "selected_tasks": ["post_details", "comments", "media"],
        "scope": "full_history",
        "metadata_ttl_days": 30,
        "metrics_ttl_hours": 24,
        "force": False,
        "policy_version": 1,
        "detail_worker_count": None,
        "comments_worker_count": None,
        "comments_enable_media_followups": True,
        "enable_cap4_canary": False,
        "listing_max_attempts": 12,
        "comments_max_attempts": 12,
        "media_max_attempts": 3,
        "platform": "instagram",
    }
    restored = RunIntent.from_payload(original)
    assert restored.payload() == original
    assert restored.digest == fingerprint(original)
    assert restored.effective_source_profile == SourceProfile.for_mode("authenticated")


def test_ordered_profile_is_explicit_versioned_and_does_not_change_public_default():
    old = SourceProfile.for_mode("public")
    profile = SourceProfile.for_public_ordered()
    assert old.listing_family == "instagram_profile_graphql_public"
    assert old.source_contract_version == old.adapter_version == 1
    assert profile.listing_family == "instagram_profile_ordered_public"
    assert profile.source_contract_version == 2 and profile.adapter_version == 1
    assert profile.details_family == old.details_family and profile.comments_family == old.comments_family
    original = RunIntent(account="bravotv", source_profile=old)
    ordered = RunIntent(
        account="bravotv",
        source_profile=profile,
        scope="bounded_window",
        date_start=datetime(2026, 1, 1, tzinfo=UTC),
        date_end=datetime(2026, 2, 1, tzinfo=UTC),
    )
    assert RunIntent.from_payload(ordered.payload()) == ordered
    assert ordered.digest != original.digest
    with pytest.raises(ValueError):
        SourceProfile(**(asdict(profile) | {"source_contract_version": 1}))
    with pytest.raises(ValueError):
        RunIntent(account="bravotv", source_profile=profile)


@pytest.mark.parametrize("mode", ["public", "authenticated"])
def test_profile_round_trip_and_immutability(mode):
    profile = SourceProfile.for_mode(mode)
    intent = RunIntent(account="bravotv", source_profile=profile)
    restored = RunIntent.from_payload(intent.payload())
    assert restored == intent
    assert restored.digest == intent.digest
    assert intent.digest != RunIntent(account="bravotv").digest
    with pytest.raises(FrozenInstanceError):
        profile.access_mode = "other"


@pytest.mark.parametrize(
    "field,value",
    [
        ("access_mode", "invalid"),
        ("comments_family", "instagram_comments_rest"),
        ("listing_family", "instagram_posts_authenticated"),
        ("details_family", "instagram_detail_authenticated"),
        ("adapter_version", 2),
        ("source_contract_version", 2),
        ("adapter_version", True),
    ],
)
def test_mixed_or_unknown_profile_rejected(field, value):
    payload = RunIntent(account="bravotv", source_profile=SourceProfile.for_mode("public")).payload()
    payload["source_profile"][field] = value
    with pytest.raises(ValueError):
        RunIntent.from_payload(payload)


@pytest.mark.parametrize("value", [None, "public", []])
def test_explicit_invalid_profile_does_not_become_historical_auth(value):
    with pytest.raises(ValueError):
        RunIntent.from_payload({"account": "bravotv", "source_profile": value})


def test_idempotent_retry_preserves_first_profile_after_policy_change(database):
    public = service(database, source_profile=SourceProfile.for_mode("public"))
    first = launch(public, key="profile-stays-public")
    authenticated = service(database, source_profile=SourceProfile.for_mode("authenticated"))
    repeated = launch(authenticated, key="profile-stays-public")
    assert repeated.run_id == first.run_id
    assert persisted(database, first.run_id)["source_profile"] == asdict(SourceProfile.for_mode("public"))
    other = launch(authenticated, key="new-auth-profile")
    assert persisted(database, other.run_id)["source_profile"] == asdict(SourceProfile.for_mode("authenticated"))


@pytest.mark.parametrize("mode", ["public", "authenticated"])
def test_api_projects_internal_profile_without_changing_frozen_intent(database, mode):
    subject = service(database, source_profile=SourceProfile.for_mode(mode))
    created = launch(subject)
    before = persisted(database, created.run_id)
    progress = subject.progress("bravotv", created.run_id)
    recent = subject.recent("bravotv", source_scope="network", limit=100)
    selected = next(run for run in recent.runs if run.run_id == created.run_id)
    assert "source_profile" not in progress.intent.model_dump()
    assert selected.intent == progress.intent
    assert progress.intent_hash == RunIntent.from_payload(before).digest
    assert persisted(database, created.run_id) == before
    internal = subject.queries.progress(str(created.run_id), "bravotv")
    assert internal["intent"]["source_profile"] == asdict(SourceProfile.for_mode(mode))


def test_ordered_successor_profile_reads_through_queries_and_api_without_changing_projection(database):
    profile = SourceProfile.for_public_ordered()
    subject = service(database, source_profile=profile)
    created = launch(
        subject,
        {
            "backfill_scope": "bounded_window",
            "date_start": "2026-01-01T00:00:00Z",
            "date_end": "2026-02-01T00:00:00Z",
        },
    )
    internal = subject.queries.progress(str(created.run_id), "bravotv")
    public = subject.progress("bravotv", created.run_id)
    assert internal["intent"]["source_profile"] == asdict(profile)
    assert "source_profile" not in public.intent.model_dump()
    assert public.intent_hash == RunIntent.from_payload(internal["intent"]).digest
    assert public.intent.force is False
