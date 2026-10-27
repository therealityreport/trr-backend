"""Native refresh with fake HTTP and isolated PostgreSQL; never real credentials."""

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import requests

from tests.ingestion.test_avatar_transport import RecordingSession
from tests.ingestion.test_media import SOURCE, snapshot
from tests.ingestion.test_persistence import database as database_fixture
from trr_backend.ingestion.domain import LostOwnership, RunIntent, Stage
from trr_backend.ingestion.media import AvatarSource, MediaIncomplete, source_reference
from trr_backend.ingestion.media_refresh import ExplicitDetailClient, MediaRefresh
from trr_backend.ingestion.post_transport import DetailSource
from trr_backend.socials.instagram.detail_transport import DetailDeferred
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

database = database_fixture
FRESH = SOURCE.replace("private-signature", "fresh-private-signature")
SECOND = "https://scontent.cdninstagram.com/second.png?oh=old-private-signature"
SECOND_FRESH = SECOND.replace("old-private", "fresh-private")


@pytest.fixture(autouse=True)
def no_ambient_http(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unexpected HTTP client construction or network send")

    monkeypatch.setattr(requests, "Session", forbidden)
    monkeypatch.setattr(requests.sessions.Session, "send", forbidden)
    monkeypatch.setattr(httpx, "Client", forbidden)
    monkeypatch.setattr(httpx, "AsyncClient", forbidden)


def native_payload(*, urls=(FRESH, SECOND_FRESH), shortcode="ABC", native_id="66"):
    def image(url):
        return {"image_versions2": {"candidates": [{"url": url, "width": 100, "height": 100}]}}

    return {
        "items": [
            {
                "pk": native_id,
                "code": shortcode,
                "user": {"username": "collaborating_owner", "pk": "99"},
                "caption": {"text": "fixture"},
                "like_count": 0,
                "comment_count": 0,
                "media_type": 8,
                "carousel_media_count": len(urls),
                **image(urls[0]),
                "carousel_media": [
                    {"pk": str(index + 100), "media_type": 1, **image(url)} for index, url in enumerate(urls)
                ],
            }
        ]
    }


def fixture_snapshot(**kwargs):
    return snapshot(shortcode="ABC", media_urls=(SOURCE, SECOND), expected_media_count=2, **kwargs)


def cookies(subject="123456"):
    return {
        "raw_cookies": {"sessionid": "synthetic-session", "csrftoken": "synthetic-csrf", "ds_user_id": subject},
        "browser_account_id": "fixture_ingest_account",
    }


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
        target_keys=["ABC"],
        max_attempts=3,
        cursor_state={},
        page_id="media-seed",
        expected_cursor_state={},
        seal=True,
    )
    owned = {
        "run_id": run["run_id"],
        "account": "bravotv",
        "generation": run["generation"],
        "stage": Stage.MEDIA,
        "target_key": "ABC",
    }
    reservation = store.reserve(**owned)
    owned["owner_token"] = str(reservation["owner_token"])
    store.acquire(**owned)
    return owned


def row(tx, statement, params=()):
    with tx() as conn, conn.cursor() as cur:
        cur.execute(statement, params)
        return cur.fetchone() if cur.description else None


@pytest.fixture(scope="module")
def lanes(database):
    store, tx = database
    row(
        tx,
        (
            Path(__file__).parents[2] / "supabase/migrations/20260911121000_instagram_detail_request_lanes.sql"
        ).read_text()
        + (
            Path(__file__).parents[2] / "supabase/migrations/20261022120000_instagram_server_retry_after.sql"
        ).read_text(),
    )
    return store, tx


def adapter(store, owned, *, source_factory, provider=None, expires=None):
    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=expires or datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=provider or (lambda: cookies()),
        detail_source_factory=source_factory,
    )
    refresh.bind_snapshot(fixture_snapshot())
    return refresh


def fake_factory(session, captured):
    def factory(**kwargs):
        captured.append(kwargs.copy())
        kwargs["scraper_factory"] = lambda **args: ExplicitDetailClient(**args, session=session)
        return DetailSource(**kwargs)

    return factory


def test_native_refresh_counts_once_retains_sanitized_evidence_and_preserves_snapshot(lanes):
    store, tx = lanes
    owned = owned_fixture(store)
    events, factories, credentials = [], [], []
    session = RecordingSession(events, payload=native_payload())
    subject = str(int(uuid4().hex[:12], 16))
    refresh = adapter(
        store,
        owned,
        source_factory=fake_factory(session, factories),
        provider=lambda: credentials.append(1) or cookies(subject),
    )
    original = refresh._snapshot

    def count():
        store.check_owner(**owned, record_request=True)

    def check():
        store.check_owner(**owned)

    assert refresh(source_reference=source_reference(SOURCE), check_cancelled=check, on_request=count) == FRESH
    assert refresh(source_reference=source_reference(SECOND), check_cancelled=check, on_request=count) == SECOND_FRESH
    assert len(credentials) == len(factories) == len(session.sent) == 1
    assert session.closed and events == ["get", "send", "close"]
    assert refresh._snapshot is original and original.media_urls == (SOURCE, SECOND)
    assert original.observed_at == fixture_snapshot().observed_at and original.storage_keys() == {}
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1, "unresolved")
    assert row(tx, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],)) == (
        0,
    )
    retained = row(
        tx,
        "select source_identity,parser_version,captured_at,payload "
        "from social.instagram_ingestion_evidence where run_id=%s",
        (owned["run_id"],),
    )
    assert retained[:2] == ("instagram:post:ABC", "instagram-ingestion-v1")
    assert retained[2] > original.observed_at and "fresh-private-signature" not in retained[3]
    identity = hashlib.sha256(subject.encode()).hexdigest()
    assert row(
        tx,
        "select cardinality(request_times),blocked from social.instagram_detail_request_lanes where identity_key=%s",
        (identity,),
    ) == (1, False)
    request, options = session.sent[0]
    assert request.url == "https://www.instagram.com/api/v1/media/66/info/"
    assert options["allow_redirects"] is False and options["proxies"] == {}
    assert factories[0]["scraper_factory"] is ExplicitDetailClient


@pytest.mark.parametrize("status,code,blocked", [(429, "rate_limited", False), (401, "unauthorized", True)])
def test_native_rejection_preserves_shared_gate_and_next_refresh_sends_nothing(lanes, status, code, blocked):
    store, tx = lanes
    owned = owned_fixture(store)
    subject = str(int(uuid4().hex[:12], 16))
    session = RecordingSession([], status=status, body="private-provider-body", headers={"retry-after": "120"})
    refresh = adapter(store, owned, source_factory=fake_factory(session, []), provider=lambda: cookies(subject))
    callbacks = {
        "check_cancelled": lambda: store.check_owner(**owned),
        "on_request": lambda: store.check_owner(**owned, record_request=True),
    }
    with pytest.raises(InstagramRequestFailure, match=code):
        refresh(source_reference=source_reference(SOURCE), **callbacks)
    assert session.closed and len(session.sent) == 1
    next_session = RecordingSession([], payload=native_payload())
    retry = adapter(store, owned, source_factory=fake_factory(next_session, []), provider=lambda: cookies(subject))
    with pytest.raises(DetailDeferred):
        retry(source_reference=source_reference(SOURCE), **callbacks)
    assert next_session.closed and next_session.sent == []
    identity = hashlib.sha256(subject.encode()).hexdigest()
    assert row(
        tx,
        "select cardinality(request_times),blocked from social.instagram_detail_request_lanes where identity_key=%s",
        (identity,),
    ) == (1, blocked)
    assert row(
        tx,
        "select requests from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1,)


@pytest.mark.parametrize("case", ["wrong_account", "cancelled", "expired_deadline", "unbound", "unsupported"])
def test_invalid_refresh_boundaries_construct_no_credentials_or_source(lanes, case):
    store, tx = lanes
    owned = owned_fixture(store)
    if case == "wrong_account":
        owned = owned | {"account": "other_account"}
    if case == "cancelled":
        store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))

    def unexpected(*args, **kwargs):
        pytest.fail("invalid refresh constructed credentials/source")

    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(seconds=-1 if case == "expired_deadline" else 30),
        credential_provider=unexpected,
        detail_source_factory=unexpected,
    )
    if case != "unbound":
        refresh.bind_snapshot(fixture_snapshot())
    with pytest.raises((LostOwnership, LookupError, MediaIncomplete)):
        refresh(
            source_reference=source_reference(
                "https://scontent.cdninstagram.com/avatar.png" if case == "unsupported" else SOURCE
            ),
            check_cancelled=lambda: None,
            on_request=unexpected,
        )
    assert row(tx, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (owned["run_id"],)) == (
        0,
    )


@pytest.mark.parametrize("change", ["wrong_shortcode", "wrong_native_id", "reordered", "changed_path", "partial"])
def test_native_identity_or_asset_mismatch_retains_evidence_without_guessing(change):
    evidence, sent = [], []
    store = SimpleNamespace(
        check_owner=lambda **kwargs: None,
        transaction=lambda: None,
        retain_evidence=lambda **kwargs: evidence.append(kwargs["evidence"]),
    )
    owned = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.MEDIA,
        "target_key": "ABC",
        "owner_token": "private",
    }
    payload = native_payload()
    if change == "wrong_shortcode":
        payload["items"][0]["code"] = "XYZ"
    elif change == "wrong_native_id":
        payload["items"][0]["pk"] = "999"
    elif change == "reordered":
        payload["items"][0]["carousel_media"].reverse()
    elif change == "changed_path":
        payload["items"][0]["carousel_media"][0]["image_versions2"]["candidates"][0]["url"] = FRESH.replace(
            "image.png", "different.png"
        )
    else:
        payload["items"][0].pop("like_count")

    def factory(**kwargs):
        def fetch(**callbacks):
            callbacks["on_request"]()
            callbacks["source_observer"]("details", payload)
            return payload

        return fetch

    refresh = adapter(store, owned, source_factory=factory)
    with pytest.raises(MediaIncomplete):
        refresh(
            source_reference=source_reference(SOURCE), check_cancelled=lambda: None, on_request=lambda: sent.append(1)
        )
    assert len(evidence) == len(sent) == 1
    assert "fresh-private-signature" not in evidence[0].payload_json
    with pytest.raises(MediaIncomplete):
        refresh(
            source_reference=source_reference(SOURCE), check_cancelled=lambda: None, on_request=lambda: sent.append(1)
        )
    assert sent == [1]


@pytest.mark.parametrize("change", ["host_only", "changed_path", "untrusted_host", "wrong_native_id", "non_public"])
def test_public_post_refresh_checks_cdn_relocation_against_pinned_post(change):
    old = "https://scontent-lax3-1.cdninstagram.com/video/asset.mp4?old=private"
    fresh = "https://scontent-mia5-1.cdninstagram.com/video/asset.mp4?fresh=private"
    if change == "changed_path":
        fresh = fresh.replace("/video/asset.mp4", "/video/different.mp4")
    if change == "untrusted_host":
        fresh = fresh.replace("cdninstagram.com", "cdninstagram.com.example")
    payload = native_payload(urls=(fresh, SECOND_FRESH), native_id="999" if change == "wrong_native_id" else "66")
    retained, sent = [], []
    store = SimpleNamespace(
        check_owner=lambda **kwargs: None,
        transaction=lambda: None,
        retain_evidence=lambda **kwargs: retained.append(kwargs["evidence"]),
    )
    owned = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.MEDIA,
        "target_key": "ABC",
        "owner_token": "private",
    }

    class PublicSource:
        def __call__(self, **callbacks):
            callbacks["on_request"]()
            callbacks["source_observer"]("details", payload)
            return payload

        def finish_success(self):
            pass

        def finish_failure(self, error):
            pass

        def close(self):
            pass

    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=cookies
        if change == "non_public"
        else lambda: pytest.fail("public refresh selected credentials"),
        public_only=change != "non_public",
        public_detail_source_factory=lambda **kwargs: PublicSource(),
        detail_source_factory=lambda **kwargs: PublicSource(),
    )
    original = snapshot(shortcode="ABC", media_urls=(old, SECOND), thumbnail_url=old, expected_media_count=2)
    refresh.bind_snapshot(original)
    callbacks = {
        "source_reference": source_reference(old),
        "check_cancelled": lambda: None,
        "on_request": lambda: sent.append(1),
    }
    if change == "host_only":
        assert refresh(**callbacks) == fresh
        assert refresh(**callbacks) == fresh
    else:
        with pytest.raises(MediaIncomplete, match="media_refresh_source_invalid"):
            refresh(**callbacks)
    assert len(sent) == len(retained) == 1
    assert refresh._snapshot is original and original.media_urls == (old, SECOND)
    assert "fresh=private" not in retained[0].payload_json


def test_narrow_detail_protocol_cannot_merge_ambient_or_response_credentials(monkeypatch):
    session = RecordingSession([])
    session.cookies.set("ambient", "forbidden")
    session.auth = ("ambient", "forbidden")
    session.headers["Authorization"] = "forbidden"
    monkeypatch.setenv("SOCIAL_INSTAGRAM_IDENTITY_POOL_ENABLED", "true")
    client = ExplicitDetailClient(cookies=cookies()["raw_cookies"], browser_account_id="fixture_ingest_account", session=session)
    try:
        assert client.session.trust_env is False and client.session.auth is None
        assert "Authorization" not in client.session.headers and not session.cookies
        session.cookies.set("ds_user_id", "forbidden-replacement")
        assert client._request_cookies()["ds_user_id"] == "123456" and not session.cookies
        assert client.session.get_adapter("https://www.instagram.com").max_retries.total == 0
    finally:
        client.session.close()


AVATAR_OLD = "https://scontent.cdninstagram.com/avatar.png?oh=old-signature"
AVATAR_FRESH = "https://scontent.cdninstagram.com/avatar.png?oh=fresh-signature"


def avatar_adapter(store, owned, session, *, subject, payload=None, cancel_on_construct=False):
    from trr_backend.ingestion.avatar_transport import AvatarProfileSource

    credentials = []

    def source_factory(**kwargs):
        if cancel_on_construct:
            store.control(owned["run_id"], owned["account"], action="cancel", idempotency_key=str(uuid4()))
        return AvatarProfileSource(session=session, **kwargs)

    def unexpected(**kwargs):
        pytest.fail("avatar refresh invoked native post details")

    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: credentials.append(1) or cookies(subject),
        detail_source_factory=unexpected,
        avatar_source_factory=source_factory,
    )
    refresh.bind_snapshot(fixture_snapshot(avatars=(AvatarSource("writer", AVATAR_OLD),)))
    return refresh, credentials


def avatar_payload(username="writer", url=AVATAR_FRESH):
    return {"status": "ok", "data": {"user": {"username": username, "profile_pic_url_hd": url}}}


def test_avatar_expiry_refresh_is_counted_once_per_username_without_registry_or_receipt_write(lanes):
    store, tx = lanes
    owned = owned_fixture(store)
    events = []
    subject = str(int(uuid4().hex[:12], 16))
    session = RecordingSession(events, payload=avatar_payload())
    refresh, credentials = avatar_adapter(store, owned, session, subject=subject)
    original = refresh._snapshot

    def count():
        events.append("count")
        store.check_owner(**owned, record_request=True)

    for _ in range(2):
        assert (
            refresh(
                source_reference=source_reference(AVATAR_OLD),
                check_cancelled=lambda: store.check_owner(**owned),
                on_request=count,
            )
            == AVATAR_FRESH
        )
    assert credentials == [1] and len(session.sent) == 1 and session.closed
    assert events == ["count", "get", "send", "close"]
    assert refresh._snapshot is original and original.avatars[0].source_url == AVATAR_OLD
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1, "unresolved")
    retained = row(
        tx,
        "select source_identity,parser_version,payload from social.instagram_ingestion_evidence where run_id=%s",
        (owned["run_id"],),
    )
    assert retained[:2] == ("instagram:profile:writer", "instagram-profile-avatar-v1")
    assert "fresh-signature" not in retained[2] and "avatar.png" in retained[2]
    assert row(tx, "select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],)) == (
        0,
    )
    # The private schema has no avatar_registry: success proves no registry write.


@pytest.mark.parametrize("case", ["identity_mismatch", "changed_path", "explicit_missing"])
def test_avatar_refresh_requires_same_username_and_asset_and_never_marks_unavailable(lanes, case):
    from trr_backend.ingestion.post_transport import SourceFailure

    store, tx = lanes
    owned = owned_fixture(store)
    payload = avatar_payload()
    if case == "identity_mismatch":
        payload = avatar_payload(username="other_user")
    elif case == "changed_path":
        payload = avatar_payload(url=AVATAR_FRESH.replace("avatar.png", "different.png"))
    else:
        payload = avatar_payload(url=None)
    subject = str(int(uuid4().hex[:12], 16))
    session = RecordingSession([], payload=payload)
    refresh, _ = avatar_adapter(store, owned, session, subject=subject)
    with pytest.raises((MediaIncomplete, SourceFailure)) as caught:
        refresh(
            source_reference=source_reference(AVATAR_OLD),
            check_cancelled=lambda: store.check_owner(**owned),
            on_request=lambda: store.check_owner(**owned, record_request=True),
        )
    assert not caught.value.failure.source_unavailable
    assert session.closed and len(session.sent) == 1
    assert row(tx, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (owned["run_id"],)) == (
        1,
    )
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1, "unresolved")


def test_avatar_cancel_between_construction_and_send_closes_without_count_or_evidence(lanes):
    store, tx = lanes
    owned = owned_fixture(store)
    session = RecordingSession([], payload=avatar_payload())
    refresh, credentials = avatar_adapter(store, owned, session, subject="123456", cancel_on_construct=True)
    with pytest.raises(LostOwnership):
        refresh(
            source_reference=source_reference(AVATAR_OLD),
            check_cancelled=lambda: store.check_owner(**owned),
            on_request=lambda: pytest.fail("cancelled avatar counted a request"),
        )
    assert credentials == [1] and session.closed and session.sent == []
    assert row(
        tx,
        "select requests from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (0,)
    assert row(tx, "select count(*) from social.instagram_ingestion_evidence where run_id=%s", (owned["run_id"],)) == (
        0,
    )


@pytest.mark.parametrize("status,code,blocked", [(429, "rate_limited", False), (401, "unauthorized", True)])
def test_avatar_native_rejection_updates_exactly_one_shared_gate(lanes, status, code, blocked):
    from trr_backend.ingestion.post_transport import SourceFailure

    store, tx = lanes
    owned = owned_fixture(store)
    subject = str(int(uuid4().hex[:12], 16))
    session = RecordingSession([], status=status, body="private-provider-body", headers={"retry-after": "120"})
    refresh, _ = avatar_adapter(store, owned, session, subject=subject)
    with pytest.raises(SourceFailure, match=code):
        refresh(
            source_reference=source_reference(AVATAR_OLD),
            check_cancelled=lambda: store.check_owner(**owned),
            on_request=lambda: store.check_owner(**owned, record_request=True),
        )
    assert session.closed and len(session.sent) == 1
    identity = hashlib.sha256(subject.encode()).hexdigest()
    assert row(
        tx,
        "select cardinality(request_times),blocked from social.instagram_detail_request_lanes where identity_key=%s",
        (identity,),
    ) == (1, blocked)
    assert row(
        tx,
        "select requests from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (1,)


@pytest.mark.parametrize("case", ["two_users", "post", "comment"])
def test_avatar_path_ambiguity_constructs_no_credentials(case):
    from trr_backend.ingestion.media import CommentMedia

    def unexpected(*args, **kwargs):
        pytest.fail("ambiguous source constructed credentials or source")

    store = SimpleNamespace(check_owner=lambda **kwargs: None)
    owned = {
        "run_id": "run",
        "account": "bravotv",
        "generation": 1,
        "stage": Stage.MEDIA,
        "target_key": "ABC",
        "owner_token": "private",
    }
    original = fixture_snapshot(avatars=(AvatarSource("writer", AVATAR_OLD),))
    if case == "two_users":
        original = replace(original, avatars=(*original.avatars, AvatarSource("other", AVATAR_OLD)))
    elif case == "post":
        original = replace(original, thumbnail_url=AVATAR_OLD)
    else:
        original = replace(
            original, comments=(CommentMedia("comment", str(uuid4()), (AVATAR_OLD,), "unknown-" + "a" * 24),)
        )
    refresh = MediaRefresh(
        store=store,
        owned=owned,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=1),
        credential_provider=unexpected,
    )
    refresh.bind_snapshot(original)
    with pytest.raises(MediaIncomplete, match="media_refresh_role_ambiguous"):
        refresh(source_reference=source_reference(AVATAR_OLD), check_cancelled=lambda: None, on_request=unexpected)


def test_expired_cdn_recovers_with_one_native_refresh_and_counts_all_three_sends(lanes):
    from tests.ingestion.test_media import Response, image_bytes
    from trr_backend.ingestion.media import MediaStorage

    store, tx = lanes
    owned = owned_fixture(store)
    session = RecordingSession([], payload=native_payload())
    refresh = adapter(
        store, owned, source_factory=fake_factory(session, []), provider=lambda: cookies(str(int(uuid4().hex[:12], 16)))
    )
    downloads = []

    def opener(client, url, **kwargs):
        downloads.append(url)
        return Response(status=403) if url == SOURCE else Response()

    storage = MediaStorage(
        client=object(),
        bucket="fixture",
        build_hosted_url=lambda key: "https://media.invalid/" + key,
        opener=opener,
        refresh=refresh,
    )
    data, content_type = storage._download(
        SOURCE,
        check=lambda: store.check_owner(**owned),
        request=lambda: store.check_owner(**owned, record_request=True),
    )
    assert downloads == [SOURCE, FRESH] and data == image_bytes() and content_type == "image/png"
    assert len(session.sent) == 1 and session.closed
    assert row(
        tx,
        "select requests,outcome from social.instagram_ingestion_targets where run_id=%s and stage='media'",
        (owned["run_id"],),
    ) == (3, "unresolved")
