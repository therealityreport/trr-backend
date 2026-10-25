"""Full avatar delivery through owned media, fake HTTP and immutable storage."""

import hashlib
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import requests

from tests.ingestion import test_media as media
from tests.ingestion.test_public_avatar_graphql_recovery import Session, native
from trr_backend.ingestion import composition
from trr_backend.ingestion.avatar_resolution import AvatarResolutionActivity
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
from trr_backend.ingestion.media import AvatarSource, MediaActivity, MediaIncomplete, source_reference
from trr_backend.ingestion.media_refresh import MediaRefresh
from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing
from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource
from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, _decode_media_result

database = media.database
media_database = media.media_database


@pytest.fixture(scope="module")
def delivery_database(media_database):
    _, tx = media_database
    root = Path(__file__).parents[2] / "supabase/migrations"
    with tx() as conn, conn.cursor() as cur:
        for name in (
            "20260911121000_instagram_detail_request_lanes.sql",
            "20261022120000_instagram_server_retry_after.sql",
        ):
            cur.execute((root / name).read_text())
    return media_database


@pytest.fixture(autouse=True)
def no_live_http(monkeypatch):
    def denied(*args, **kwargs):
        pytest.fail("unmocked HTTP send")

    monkeypatch.setattr(requests.sessions.Session, "send", denied)


def fresh_url(handle):
    return f"https://scontent.cdninstagram.com/{handle}.png?oh=fresh-avatar-signature&oe=expiry&unknown-token=private-ephemeral"


def setup(delivery_database, handles, *, acquire=False):
    store, tx = delivery_database
    owned, token, source = media.launch_media(delivery_database)
    if acquire:
        store.acquire(**owned, owner_token=token)
    fenced = {**owned, "owner_token": token}
    stamp = media.OBSERVED
    source = replace(
        source,
        media_urls=tuple(map(source_reference, source.media_urls)),
        thumbnail_url=source_reference(source.thumbnail_url),
        avatars=tuple(
            AvatarSource(name, source_reference(fresh_url(name)), "mention", observed_at=stamp) for name in handles
        ),
    )
    with tx() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_detail_request_lanes")
    return store, tx, owned, token, fenced, source


def profile_factory(sessions, *, changed=False, fail_handle=None, signature=None):
    def create(username, deadline):
        url = fresh_url(username)
        if signature is not None:
            url = url.replace("fresh-avatar-signature", signature)
        if changed:
            url = url.replace(".png?", "-different.png?")
        session = Session(native(username, profile_pic_url_hd=url))
        source = PublicGraphQLProfileSource(user_id="123", deadline=deadline, session=session)
        if username == fail_handle:
            source.deadline = time.monotonic() - 1
        sessions.append(session)
        return source

    return create


def refresh_for(store, fenced, factory):
    return MediaRefresh(
        store=store,
        owned=fenced,
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: pytest.fail("public avatar selected credentials"),
        public_only=True,
        network_lane="local",
        public_avatar_source_factory=factory,
    )


def retained(tx, run_id):
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select source_identity,captured_at,parser_version,payload,content_hash
            from social.instagram_ingestion_evidence where run_id=%s order by captured_at""",
            (run_id,),
        )
        return cur.fetchall()


def assert_result_replays_without_ephemeral_queries(tx, run_id, handles, stamps):
    rows = retained(tx, run_id)
    assert rows and all(
        "fresh-avatar-signature" not in row[3]
        and "private-ephemeral" not in row[3]
        and "discarded-first-signature" not in row[3]
        for row in rows
    )
    row = next(item for item in rows if item[2] == MEDIA_RESULT_REPLAY_VERSION)
    evidence = Evidence(row[0], row[1], row[2], row[3], row[4])
    result = _decode_media_result(evidence.read(pinned=True))
    assert {avatar.username for avatar in result.snapshot.avatars} == set(handles)
    assert all(
        avatar.download_url is None and avatar.role == "mention" and avatar.observed_at == stamps[avatar.username]
        for avatar in result.snapshot.avatars
    )
    assert all("?" not in entry["source_url"] for entry in result.avatars)
    return result


def test_queryless_avatar_403_refreshes_with_two_counted_graphql_requests_then_hosts_and_replays(delivery_database):
    handle = "nbc"
    store, tx, owned, token, fenced, source = setup(delivery_database, [handle])
    sessions, downloads = [], []
    refresh = refresh_for(store, fenced, profile_factory(sessions))
    client = media.Storage()
    reference = source_reference(fresh_url(handle))

    def opener(_requests, url, **kwargs):
        downloads.append(url)
        if url == reference:
            return media.Response(status=403)
        if source_reference(url) == reference:
            assert url == fresh_url(handle)
        return media.Response()

    receipt = MediaActivity(store, storage=media.storage(client, opener=opener, refresh=refresh)).execute(
        **owned, owner_token=token, load=lambda **kwargs: source
    )
    assert receipt["outcome"] == "committed"
    assert [call[0] for call in sessions[0].calls] == ["GET", "POST"] and sessions[0].closed
    assert reference in downloads and fresh_url(handle) in downloads
    result = assert_result_replays_without_ephemeral_queries(tx, owned["run_id"], [handle], {handle: media.OBSERVED})
    assert result.avatars[0]["sha256"] == hashlib.sha256(media.image_bytes()).hexdigest()
    assert refresh._snapshot is source and source.avatars[0].source_url == reference
    with tx() as conn, conn.cursor() as cur:
        cur.execute("select source_url,status from social.avatar_registry where account_handle=%s", (handle,))
        assert cur.fetchone() == (reference, "mirrored")


def test_partial_avatar_resolution_retry_uses_new_graphql_refresh_for_pending_registry(delivery_database, monkeypatch):
    handles = ["partial_a" + uuid4().hex[:7], "partial_b" + uuid4().hex[:7]]
    store, tx, owned, token, fenced, base = setup(delivery_database, handles, acquire=True)
    sessions = []
    gate = SourceRequestGate(
        cookies={},
        public_only=True,
        network_lane="local",
        transaction=store.transaction,
        check_cancelled=lambda: store.check_owner(**fenced),
        deadline=time.monotonic() + 60,
    )
    first = profile_factory(sessions, fail_handle=handles[1], signature="discarded-first-signature")
    with pytest.raises(IdentityDeferred):
        AvatarResolutionActivity(store, gate=gate).execute(
            **{k: v for k, v in fenced.items() if k != "stage"},
            handles=handles,
            source_factory=lambda username: first(username, time.monotonic() + 60),
        )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select account_handle,source_url,status,last_checked_at from social.avatar_registry
            where account_handle=any(%s)""",
            (handles,),
        )
        initial = cur.fetchall()
    assert len(initial) == 1 and initial[0][0] == handles[0] and initial[0][2] == "pending" and "?" not in initial[0][1]
    store.checkpoint_failure(**fenced, failure=Failure("target_deadline"), checkpoint={}, requests=0)
    token = str(store.reserve(**owned)["owner_token"])
    fenced = {**owned, "owner_token": token}
    current = profile_factory(sessions)
    monkeypatch.setattr(composition, "_source_profile", lambda context: SimpleNamespace(access_mode="public"))
    monkeypatch.setattr(
        composition, "_public_avatar_source", lambda store, owned, username, deadline: current(username, deadline)
    )
    stamps = {}

    class Loader:
        def load(self, **kwargs):
            with tx() as conn, conn.cursor() as cur:
                cur.execute(
                    """select account_handle,source_url,last_checked_at,status from social.avatar_registry
                        where account_handle=any(%s)""",
                    (handles,),
                )
                saved = {row[0]: row for row in cur.fetchall()}
            missing = set(handles) - set(saved)
            if missing:
                raise AvatarSourcesMissing(missing)
            stamps.update({name: saved[name][2] for name in handles})
            return replace(
                base,
                avatars=tuple(
                    AvatarSource(name, saved[name][1], "mention", observed_at=saved[name][2]) for name in handles
                ),
            )

    loader = Loader()
    downloads = []

    def opener(_requests, url, **kwargs):
        downloads.append(url)
        if url == source_reference(fresh_url(handles[0])):
            return media.Response(status=403)
        if source_reference(url) in {source_reference(fresh_url(name)) for name in handles}:
            assert url in {fresh_url(name) for name in handles}
        return media.Response()

    refresh = refresh_for(store, fenced, current)
    context = {
        "run": {"execution_placement": "local"},
        "target": {"owner_expires_at": datetime.now(UTC) + timedelta(minutes=5)},
    }
    receipt = MediaActivity(store, storage=media.storage(opener=opener, refresh=refresh)).execute(
        **owned,
        owner_token=token,
        load=lambda **kwargs: composition._load_media(loader, store, context, fenced, **kwargs),
    )
    assert receipt["outcome"] == "committed"
    assert [len(session.calls) for session in sessions] == [2, 0, 2, 2] and all(session.closed for session in sessions)
    assert fresh_url(handles[0]) in downloads and fresh_url(handles[1]) in downloads
    assert stamps[handles[0]] == initial[0][3]
    assert_result_replays_without_ephemeral_queries(tx, owned["run_id"], handles, stamps)


def test_changed_current_avatar_cannot_replace_old_pending_reference_or_clock(delivery_database):
    handle = "changed" + uuid4().hex[:8]
    store, tx, owned, token, fenced, source = setup(delivery_database, [handle], acquire=True)
    reference = source.avatars[0].source_url
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,status,last_checked_at)
            values('instagram',%s,%s,'pending',%s)""",
            (handle, reference, media.OBSERVED),
        )
    sessions = []
    refresh = refresh_for(store, fenced, profile_factory(sessions, changed=True))
    refresh.bind_snapshot(source)
    with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
        refresh(
            source_reference=reference,
            check_cancelled=lambda: store.check_owner(**fenced),
            on_request=lambda: store.check_owner(**fenced, record_request=True),
        )
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select source_url,status,last_checked_at from social.avatar_registry where account_handle=%s", (handle,)
        )
        assert cur.fetchone() == (reference, "pending", media.OBSERVED)
        cur.execute("select count(*) from social.instagram_ingestion_receipts where run_id=%s", (owned["run_id"],))
        assert cur.fetchone() == (0,)
    assert len(sessions[0].calls) == 2 and sessions[0].closed
    assert refresh._snapshot is source and source.avatars[0].observed_at == media.OBSERVED


def test_refresh_control_database_failure_preserves_origin_without_source_health_penalty(delivery_database):
    import psycopg2

    handle = "control" + uuid4().hex[:8]
    store, tx, owned, token, fenced, source = setup(delivery_database, [handle], acquire=True)
    sessions = []
    refresh = refresh_for(store, fenced, profile_factory(sessions))
    refresh.bind_snapshot(source)
    error = psycopg2.OperationalError("synthetic media request-accounting failure")

    def fail():
        raise error

    with pytest.raises(psycopg2.OperationalError) as caught:
        refresh(source_reference=source.avatars[0].source_url, check_cancelled=lambda: None, on_request=fail)
    assert caught.value is error and sessions[0].calls == [] and sessions[0].closed
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select failure_generation,consecutive_failures,last_error_code from social.instagram_detail_request_lanes"
        )
        assert cur.fetchall() == [(0, 0, None)]


def test_refresh_honors_actual_execution_soft_cap_before_constructing_source(delivery_database):
    from trr_backend.ingestion.reply_context import execution_deadline

    handle = "deadline" + uuid4().hex[:8]
    store, tx, owned, token, fenced, source = setup(delivery_database, [handle], acquire=True)
    sessions = []
    refresh = refresh_for(store, fenced, profile_factory(sessions))
    refresh.bind_snapshot(source)
    token = execution_deadline.set(time.monotonic() + 20.5)
    try:
        with pytest.raises(IdentityDeferred) as caught:
            refresh(
                source_reference=source.avatars[0].source_url,
                check_cancelled=lambda: None,
                on_request=lambda: pytest.fail("expired source counted request"),
            )
        assert caught.value.failure.code == "target_deadline" and sessions == []
    finally:
        execution_deadline.reset(token)
