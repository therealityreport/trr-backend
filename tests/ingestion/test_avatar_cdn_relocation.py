"""Avatar-only CDN delivery relocation preserves sealed source identities."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.ingestion import test_public_avatar_graphql_recovery as public
from tests.ingestion.test_media import OBSERVED, Response, snapshot, storage
from trr_backend.ingestion.composition import _bind_avatar_downloads
from trr_backend.ingestion.domain import LostOwnership, Stage
from trr_backend.ingestion.media import AvatarSource, MediaIncomplete, avatar_delivery_equivalent, mirror_snapshot
from trr_backend.ingestion.media_refresh import MediaRefresh

database = public.database
avatars = public.avatars

PATH = "/v/t51.82787-19/587786285_18547472062056067_4240899138933184409_n.jpg"
OLD = "https://scontent-lax3-1.cdninstagram.com" + PATH
NEW = "https://scontent-mia5-1.cdninstagram.com" + PATH


@pytest.mark.parametrize("fresh", [NEW, NEW + "?oh=private", NEW.replace(".com/", ".com:443/")])
def test_actual_observed_path_allows_only_delivery_host_relocation(fresh):
    assert avatar_delivery_equivalent(OLD, fresh, native_id="1063480066")
    assert OLD != fresh


@pytest.mark.parametrize(
    "fresh",
    [
        NEW.replace("587786285_", "123_"),
        NEW.replace("/v/t51.82787-19/", "/v/other/"),
        NEW.replace("scontent-mia5-1.cdninstagram.com", "foreign.example"),
        NEW.replace("cdninstagram.com", "cdninstagram.com.evil.example"),
        NEW.replace("https:", "http:"),
        NEW.replace(".com/", ".com:444/"),
        NEW.replace("https://", "https://user:password@"),
        NEW.replace(PATH, "/"),
        NEW + "#fragment",
        NEW.replace("/v/", "/%76/"),
    ],
)
def test_relocation_rejects_changed_full_path_or_untrusted_delivery(fresh):
    assert not avatar_delivery_equivalent(OLD, fresh, native_id="1063480066")


def test_exact_reference_behavior_is_preserved():
    exact = "https://scontent.cdninstagram.com/image.jpg"
    assert avatar_delivery_equivalent(exact + "?old=1", exact + "?new=2")
    assert not avatar_delivery_equivalent(exact, NEW)


def observation(username="bravowwhl", url=NEW, pin="1063480066"):
    return SimpleNamespace(username=username, source_url=url, unavailable=False, validated_native_id=pin)


def test_ephemeral_binding_keeps_role_source_clock_keys_and_rejects_other_user():
    avatar = AvatarSource("bravowwhl", OLD, role="owner", observed_at=OBSERVED)
    initial = snapshot(avatars=(avatar,))
    assert _bind_avatar_downloads(initial, [observation("someone_else")]).avatars == initial.avatars
    bound = _bind_avatar_downloads(initial, [observation()])
    assert bound.avatars[0].download_url == NEW
    assert bound.avatars[0].source_url == OLD and bound.avatars[0].role == "owner"
    assert bound.avatars[0].observed_at == OBSERVED and bound.observed_at == initial.observed_at
    assert bound.storage_keys() == initial.storage_keys()
    assert bound == initial  # private delivery URL is excluded from equality
    with pytest.raises(MediaIncomplete, match="avatar_download_identity_mismatch"):
        replace(initial, avatars=(replace(avatar, download_url=NEW.replace("/v/", "/other/")),)).validate()


def test_binder_refuses_distinct_eligible_delivery_identities_and_preserves_cached():
    initial = snapshot(avatars=(AvatarSource("bravowwhl", OLD, observed_at=OBSERVED),))
    for observations in ([observation(), observation(url=OLD)], [observation(url=OLD), observation()]):
        assert _bind_avatar_downloads(initial, observations).avatars[0].download_url is None
    cached = replace(
        initial,
        avatars=(replace(initial.avatars[0], cached_hosted_url="https://media.example/a", cached_sha256="a" * 64),),
    )
    assert _bind_avatar_downloads(cached, [observation()]).avatars[0].download_url is None


def refresh_for(initial, fresh=NEW):
    refresh = MediaRefresh(
        store=SimpleNamespace(check_owner=Mock()),
        owned={
            "run_id": "run",
            "account": "bravotv",
            "generation": 1,
            "target_key": initial.shortcode,
            "owner_token": "owner",
            "stage": Stage.MEDIA,
        },
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: pytest.fail("credential selection"),
        public_only=True,
    )
    refresh.bind_snapshot(initial)
    refresh._fetch_avatar = Mock(return_value=fresh)
    refresh._avatar_native_ids["bravowwhl"] = "1063480066"
    return refresh


def test_relocation_download_preserves_canonical_source_and_full_hash_decode():
    initial = snapshot(avatars=(AvatarSource("bravowwhl", OLD, role="owner", observed_at=OBSERVED),))
    refresh = refresh_for(initial)
    urls = []

    def opener(_session, url, **kwargs):
        urls.append(url)
        return Response(status=403 if url == OLD else 200)

    result = mirror_snapshot(initial, storage(opener=opener, refresh=refresh), check=lambda: None, request=lambda: None)
    assert OLD in urls and NEW in urls and refresh._fetch_avatar.call_count == 1
    assert result.snapshot.avatars[0].source_url == OLD and result.snapshot.avatars[0].observed_at == OBSERVED
    assert result.avatars[0]["source_url"] == OLD and len(result.avatars[0]["sha256"]) == 64
    assert result.avatars[0]["observed_at"] == OBSERVED.isoformat()

    from trr_backend.ingestion.domain import stable_json
    from trr_backend.ingestion.replay import serialize_media_result

    sealed = stable_json(serialize_media_result(result))
    assert NEW not in sealed and OLD in sealed


def test_changed_avatar_stays_unresolved_and_cancellation_keeps_its_origin():
    initial = snapshot(avatars=(AvatarSource("bravowwhl", OLD, observed_at=OBSERVED),))
    refresh = refresh_for(initial, NEW.replace("/v/", "/changed/"))
    with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
        refresh(source_reference=OLD, check_cancelled=lambda: None, on_request=lambda: None)
    assert refresh._fetch_avatar.call_count == 1
    with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
        refresh(source_reference=OLD, check_cancelled=lambda: None, on_request=lambda: None)
    assert refresh._fetch_avatar.call_count == 1
    refresh = refresh_for(initial)
    with pytest.raises(LostOwnership):
        refresh(
            source_reference=OLD, check_cancelled=Mock(side_effect=LostOwnership("fixture")), on_request=lambda: None
        )
    refresh._fetch_avatar.assert_not_called()


def test_unpinned_relocation_refused_and_provenance_ambiguity_not_selected():
    initial = snapshot(avatars=(AvatarSource("bravowwhl", OLD, observed_at=OBSERVED),))
    assert not avatar_delivery_equivalent(OLD, NEW)
    assert not avatar_delivery_equivalent(OLD, NEW, native_id="bad")
    assert _bind_avatar_downloads(initial, [observation(pin=None)]).avatars[0].download_url is None
    assert _bind_avatar_downloads(initial, [observation(), observation(pin="999")]).avatars[0].download_url is None
    for proof in (("someone_else", "1063480066"), ("bravowwhl", "bad"), None):
        with pytest.raises(MediaIncomplete, match="avatar_download_identity_mismatch"):
            replace(
                initial, avatars=(replace(initial.avatars[0], download_url=NEW, download_provenance=proof),)
            ).validate()
    refresh = refresh_for(initial)
    refresh._avatar_native_ids.clear()
    with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
        refresh(source_reference=OLD, check_cancelled=lambda: None, on_request=lambda: None)


def test_strict_pinned_observation_proof_survives_execute(avatars):
    import time
    from uuid import uuid4

    from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource

    owned, activity, _, _ = public.setup(avatars)
    username = "p" + uuid4().hex[:10]
    native = public.native(username)
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=public.Session(native))
    result = activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert result[0].validated_native_id == "123"
    assert "validated_native_id" not in repr(result[0])


@pytest.mark.parametrize("mismatch", ["handle", "native_id"])
def test_wrong_pinned_identity_never_produces_delivery_observation(avatars, mismatch):
    import time
    from uuid import uuid4

    from trr_backend.ingestion.avatar_resolution import AvatarResolutionError
    from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource

    owned, activity, _, _ = public.setup(avatars)
    username = "p" + uuid4().hex[:10]
    native = public.native("someone_else" if mismatch == "handle" else username)
    if mismatch == "native_id":
        native["data"]["user"]["id"] = "999"
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=public.Session(native))
    with pytest.raises(AvatarResolutionError):
        activity.execute(**owned, handles=[username], source_factory=lambda _: source)
    assert source.session.closed


def test_prebound_alias_real_profile_refresh_roundtrip_preserves_original_role(avatars):
    from trr_backend.ingestion.profile_transport import PublicGraphQLProfileSource
    from trr_backend.ingestion.replay import serialize_media_result

    owned, activity, _, _ = public.setup(avatars)
    initial = snapshot(
        shortcode=owned["target_key"], avatars=(AvatarSource("bravowwhl", OLD, role="owner", observed_at=OBSERVED),)
    )
    bound = _bind_avatar_downloads(initial, [observation()])
    fresh = NEW.replace("scontent-mia5-1", "scontent-ord5-1") + "?oh=private-fresh-signature"
    session = public.Session(public.native("bravowwhl", "1063480066", profile_pic_url_hd=fresh))
    sources = []

    def factory(*, username, deadline):
        assert username == "bravowwhl"
        source = PublicGraphQLProfileSource(user_id="1063480066", deadline=deadline, session=session)
        sources.append(source)
        return source

    refresh = MediaRefresh(
        store=activity.store,
        owned={**owned, "stage": Stage.MEDIA},
        owner_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        credential_provider=lambda: pytest.fail("credential selection"),
        public_only=True,
        network_lane="local",
        public_avatar_source_factory=factory,
    )
    refresh.bind_snapshot(bound)
    urls = []

    def opener(_session, url, **kwargs):
        urls.append(url)
        return Response(status=403 if url == NEW else 200)

    result = mirror_snapshot(bound, storage(opener=opener, refresh=refresh), check=lambda: None, request=lambda: None)
    assert NEW in urls and fresh in urls and OLD not in urls
    assert len(sources) == 1 and len(session.calls) == 2 and session.closed
    assert result.snapshot.storage_keys() == initial.storage_keys()
    assert result.snapshot.avatars[0].source_url == OLD and result.snapshot.avatars[0].role == "owner"
    assert result.snapshot.avatars[0].observed_at == OBSERVED
    sealed = serialize_media_result(result)
    assert sealed["media_result"]["avatars"][0]["source_url"] == OLD
    assert sealed["media_result"]["avatars"][0]["observed_at"] == OBSERVED.isoformat()
    assert "download_provenance" not in repr(sealed)


@pytest.mark.parametrize(
    "collision", ["post_alias", "comment_alias", "post_original", "canonical_avatar", "alias_avatar"]
)
def test_prebound_alias_rejects_role_and_delivery_ambiguity(collision):
    from trr_backend.ingestion.media import CommentMedia

    initial = _bind_avatar_downloads(snapshot(avatars=(AvatarSource("bravowwhl", OLD),)), [observation()])
    if collision == "post_alias":
        initial = replace(initial, thumbnail_url=NEW)
    elif collision == "post_original":
        initial = replace(initial, thumbnail_url=OLD)
    elif collision == "comment_alias":
        initial = replace(
            initial,
            comments=(
                CommentMedia(
                    "comment",
                    "00000000-0000-4000-8000-000000000001",
                    (NEW,),
                    storage_source_key="unknown-000000000000000000000000",
                ),
            ),
        )
    elif collision == "canonical_avatar":
        initial = replace(initial, avatars=(*initial.avatars, AvatarSource("another", NEW)))
    else:
        initial = replace(
            initial,
            avatars=(
                *initial.avatars,
                replace(initial.avatars[0], username="another", download_provenance=("another", "123")),
            ),
        )
    refresh = refresh_for(initial)
    with pytest.raises(MediaIncomplete, match="media_refresh_role_ambiguous"):
        refresh(source_reference=NEW, check_cancelled=lambda: None, on_request=lambda: None)
    refresh._fetch_avatar.assert_not_called()


def test_alias_unpinned_refused_and_repeated_failure_budget_and_cancellation_unchanged():
    initial = _bind_avatar_downloads(snapshot(avatars=(AvatarSource("bravowwhl", OLD),)), [observation()])
    unpinned = replace(initial, avatars=(replace(initial.avatars[0], download_provenance=None),))
    with pytest.raises(MediaIncomplete, match="avatar_download_identity_mismatch"):
        refresh_for(unpinned)
    refresh = refresh_for(initial, NEW.replace("/v/", "/changed/"))
    for _ in range(2):
        with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
            refresh(source_reference=NEW, check_cancelled=lambda: None, on_request=lambda: None)
    assert refresh._fetch_avatar.call_count == 1
    refresh = refresh_for(initial)
    refresh._avatar_native_ids["bravowwhl"] = "999"
    with pytest.raises(MediaIncomplete, match="avatar_refresh_asset_unresolved"):
        refresh(source_reference=NEW, check_cancelled=lambda: None, on_request=lambda: None)
    refresh = refresh_for(initial)
    with pytest.raises(LostOwnership):
        refresh(
            source_reference=NEW, check_cancelled=Mock(side_effect=LostOwnership("fixture")), on_request=lambda: None
        )
    refresh._fetch_avatar.assert_not_called()
