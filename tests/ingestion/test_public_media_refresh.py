"""Public refresh retains fresh evidence under the media fence; isolated PG."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.ingestion.test_media_refresh import (
    AVATAR_FRESH,
    AVATAR_OLD,
    FRESH,
    SECOND,
    SECOND_FRESH,
    SOURCE,
    avatar_payload,
    fixture_snapshot,
    lanes,
    native_payload,
    no_ambient_http,
    row,
)
from tests.ingestion.test_media_refresh import (
    database as database_fixture,
)
from tests.ingestion.test_public_transport import SequenceSession, html
from trr_backend.ingestion.avatar_transport import AvatarProfileSource
from trr_backend.ingestion.domain import RunIntent, Stage
from trr_backend.ingestion.media import AvatarSource, MediaIncomplete, source_reference
from trr_backend.ingestion.media_refresh import MediaRefresh
from trr_backend.ingestion.public_transport import PublicDetailSource

database = database_fixture


def owned_fixture(store):
    run = store.start(
        RunIntent(account="bravotv", selected_tasks=(Stage.MEDIA,)),
        idempotency_key=str(uuid4()),
        engine="dbos",
        workflow_version="fixture",
        initiated_by="fixture",
    )
    store.discover(
        run["run_id"],
        "bravotv",
        generation=run["generation"],
        stage=Stage.MEDIA,
        target_keys=["AAAABC"],
        max_attempts=3,
        cursor_state={},
        page_id="public-media-seed",
        expected_cursor_state={},
        seal=True,
    )
    owned = {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": run["generation"],
        "stage": Stage.MEDIA,
        "target_key": "AAAABC",
    }
    reservation = store.reserve(**owned)
    owned["owner_token"] = str(reservation["owner_token"])
    store.acquire(**owned)
    return owned


@pytest.mark.parametrize("changed", [False, True])
def test_public_native_refresh_has_no_credential_selection_and_preserves_storage_identity(lanes, changed):
    store, tx = lanes
    owned = owned_fixture(store)
    node = native_payload(shortcode="AAAABC")["items"][0]
    if changed:
        node["carousel_media"].reverse()
    envelope = {"data": {"xdt_api__v1__media__shortcode__web_info": {"items": [node]}}}
    session = SequenceSession([], [{"body": html(envelope), "headers": {"content-type": "text/html"}}])
    sources = []

    def factory(**kwargs):
        sources.append(kwargs)
        return PublicDetailSource(**kwargs, session_factory=lambda: session)

    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: pytest.fail("public refresh selected authentication"),
        public_only=True,
        public_detail_source_factory=factory,
    )
    original = replace(fixture_snapshot(), shortcode="AAAABC")
    original_keys = (original.post_id, original.canonical_post_id, original.show_id, original.storage_keys())
    refresh.bind_snapshot(original)
    callbacks = {
        "check_cancelled": lambda: store.check_owner(**owned),
        "on_request": lambda: store.check_owner(**owned, record_request=True),
    }
    if changed:
        with pytest.raises(MediaIncomplete, match="media_refresh_source_invalid"):
            refresh(source_reference=source_reference(SOURCE), **callbacks)
    else:
        assert refresh(source_reference=source_reference(SOURCE), **callbacks) == FRESH
        assert refresh(source_reference=source_reference(SECOND), **callbacks) == SECOND_FRESH
    assert len(sources) == len(session.sent) == 1 and session.closed
    assert sources[0]["gate"].public_only is True
    assert "Cookie" not in session.sent[0][0].headers and "Authorization" not in session.sent[0][0].headers
    assert refresh._snapshot is original
    assert (original.post_id, original.canonical_post_id, original.show_id, original.storage_keys()) == original_keys
    assert original.media_urls == (SOURCE, SECOND) and original.shortcode == "AAAABC"
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1, "unresolved")
    retained = row(
        tx,
        "select stage,source_identity,payload from social.instagram_ingestion_evidence where run_id=%s",
        (owned["run_id"],),
    )
    assert retained[:2] == ("media", "instagram:post:AAAABC")
    assert "fresh-private-signature" not in retained[2] and "image_versions2" in retained[2]
    assert row(tx, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],)) == (
        0,
    )


__all__ = ["lanes", "no_ambient_http"]


def test_public_avatar_refresh_uses_anonymous_profile_once_without_credentials(lanes):
    store, tx = lanes
    owned = owned_fixture(store)
    session = SequenceSession([], [{"payload": avatar_payload()}])
    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: pytest.fail("public avatar selected authentication"),
        public_only=True,
        avatar_source_factory=lambda **kw: AvatarProfileSource(**kw, session=session),
    )
    original = replace(fixture_snapshot(avatars=(AvatarSource("writer", AVATAR_OLD),)), shortcode="AAAABC")
    refresh.bind_snapshot(original)
    callbacks = {
        "check_cancelled": lambda: store.check_owner(**owned),
        "on_request": lambda: store.check_owner(**owned, record_request=True),
    }
    for _ in range(2):
        assert refresh(source_reference=source_reference(AVATAR_OLD), **callbacks) == AVATAR_FRESH
    assert len(session.sent) == 1 and session.closed and refresh._snapshot is original
    assert "Cookie" not in session.sent[0][0].headers
    retained = row(
        tx,
        "select source_identity,payload from social.instagram_ingestion_evidence where run_id=%s",
        (owned["run_id"],),
    )
    assert retained[0] == "instagram:profile:writer" and "fresh-signature" not in retained[1]
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1, "unresolved")
