"""Historical cached alias proofs: isolated PostgreSQL and explicit mock bytes."""

import hashlib
import json
import time
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from psycopg2.extras import RealDictCursor

from scripts.ingestion.january_audit import common
from scripts.ingestion.january_audit.media import verify_cached_aliases
from tests.ingestion import test_media_snapshot as fixtures
from tests.ingestion.test_media import OBSERVED, Response, Storage, image_bytes, storage
from trr_backend.ingestion import cached_avatar_alias as alias
from trr_backend.ingestion.domain import LostOwnership, fingerprint, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import MEDIA_VERSION, AvatarSource, MediaActivity, MediaIncomplete, MediaOwnershipCheck
from trr_backend.ingestion.media_snapshot import MediaSnapshotLoader

database = fixtures.database
media_database = fixtures.media_database
snapshot_database = fixtures.snapshot_database
snapshot_workflow_database = fixtures.snapshot_workflow_database

PATH = "/v/t51.82787-19/717993431_18178460515405264_4871378796624596130_n.jpg"
OLD = "https://scontent-lax3-1.cdninstagram.com" + PATH
REGIONAL = "https://scontent-iad6-1.cdninstagram.com" + PATH
DATA = image_bytes()
SHA = hashlib.sha256(DATA).hexdigest()
BYTE_CLOCK = OBSERVED - timedelta(days=90)


def row(**kwargs):
    return {
        "id": str(uuid4()),
        "platform": "instagram",
        "account_handle": "owner",
        "source_url": REGIONAL,
        "last_checked_at": BYTE_CLOCK,
        "status": "mirrored",
        "content_hash": SHA,
        "hosted_url": "https://media.example/asset.jpg",
        **kwargs,
    }


@pytest.mark.parametrize(
    "url",
    [
        OLD.replace("https:", "http:"),
        OLD.replace(".com/", ".com:443/"),
        OLD + "#x",
        OLD.replace("https://", "https://a:b@"),
        OLD.replace("cdninstagram.com", "cdninstagram.com.evil.org"),
        OLD.replace("scontent-lax3-1", "scontent"),
        OLD.replace("/v/", "/%76/"),
        OLD.replace("/v/", "/v/../v/"),
        OLD.replace("_n.jpg", "_n.jpg/"),
        OLD.replace("_n.jpg", "_n.jpg%2f"),
    ],
)
def test_regional_identity_rejects_ambiguous_or_untrusted_url(url):
    assert alias.regional_reference(url) is None


def test_exact_full_path_and_immutable_query_free_reference():
    assert alias.regional_reference(OLD + "?oh=signed") == OLD
    assert alias.regional_reference(REGIONAL) != OLD
    rows = [
        row(),
        row(source_url=REGIONAL.replace("/v/", "/other/")),
        row(source_url=REGIONAL.replace("717993431", "845832610")),
        row(account_handle="other"),
    ]
    cohort = alias.registry_cohort(rows, "owner", OLD)
    assert len(cohort) == 1
    select_candidate = alias.select_candidate(cohort, OLD, OBSERVED)
    assert select_candidate is not None
    assert select_candidate["sha256"] == SHA


@pytest.mark.parametrize(
    "changed,code",
    [
        ({"content_hash": "b" * 64}, "hash_ambiguous"),
        ({"hosted_url": "https://media.example/else.jpg"}, "hosted_ambiguous"),
        ({"content_hash": "invalid"}, "hash_ambiguous"),
        (
            {"status": "failed", "content_hash": None, "last_checked_at": OBSERVED + timedelta(seconds=1)},
            "avatar_source_newer",
        ),
    ],
)
def test_all_alias_hash_and_newer_unproved_guards(changed, code):
    rows = [row(), row(source_url=REGIONAL.replace("iad6", "ams1"), **changed)]
    with pytest.raises(MediaIncomplete, match=code):
        alias.select_candidate(alias.registry_cohort(rows, "owner", OLD), OLD, OBSERVED)


def test_latest_per_reference_cannot_bypass_failed_row_and_never_replaces_exact():
    good = row()
    failed = row(status="failed", content_hash=None, hosted_url=None, last_checked_at=BYTE_CLOCK + timedelta(days=1))
    assert alias.select_candidate(alias.registry_cohort([good, failed], "owner", OLD), OLD, OBSERVED) is None
    exact = row(source_url=OLD, status="failed", content_hash=None, hosted_url=None)
    assert alias.select_candidate(alias.registry_cohort([good, exact], "owner", OLD), OLD, OBSERVED) is None


def byte_storage(monkeypatch, *, data=DATA, response=None):
    client = SimpleNamespace(digest_object=Mock(return_value={"sha256": SHA, "bytes": len(DATA)}), close=Mock())
    backend = storage(client=client)
    response = response or Response(data=data, headers={"Content-Length": str(len(data))})
    opener = Mock(return_value=response)
    monkeypatch.setattr(alias, "safe_requests_get", opener)
    candidate = row(hosted_url=backend._url(alias.expected_key("owner", SHA, OLD)))
    candidate = alias.registry_cohort([candidate], "owner", OLD)[0]
    check = MediaOwnershipCheck(lambda: None, deadline=time.monotonic() + 30)
    return backend, candidate, check, opener


def test_full_storage_cdn_digest_decode_and_three_clocks(monkeypatch):
    backend, candidate, check, opener = byte_storage(monkeypatch)
    backend.prefix = "post-only-prefix"  # avatars use their independent maintained root
    requests = Mock()
    proof = alias.verify_cached_bytes(
        backend,
        username="owner",
        reference=OLD,
        candidate=candidate,
        check=check,
        request=requests,
        clock=lambda: OBSERVED + timedelta(days=1),
    )
    assert proof["sha256"] == SHA and proof["bytes"] == len(DATA)
    assert proof["format"] == "PNG" and proof["width"] == 12 and proof["height"] == 8 and proof["frames"] == 1
    assert proof["verified_at"] == utc_timestamp(OBSERVED + timedelta(days=1))
    assert requests.call_count == 2
    args = opener.call_args.kwargs
    assert args["max_bytes"] == 50 * 1024 * 1024 and args["allow_redirects"] is False
    assert args["headers"] == {"Accept-Encoding": "identity"}
    assert 0 < args["body_timeout"] <= args["total_timeout"] <= 30


@pytest.mark.parametrize(
    "response",
    [
        Response(status=206),
        Response(status=302),
        Response(headers={"Content-Range": "bytes 0-1/3"}),
        Response(headers={"Content-Encoding": "gzip"}),
        Response(headers={"Content-Length": "999"}),
        Response(data=b"different"),
    ],
)
def test_cdn_partial_redirect_encoded_truncated_or_mismatched_bytes_rejected(monkeypatch, response):
    backend, candidate, check, _ = byte_storage(monkeypatch, response=response)
    with pytest.raises(MediaIncomplete, match="cached_avatar_alias_cdn"):
        alias.verify_cached_bytes(
            backend, username="owner", reference=OLD, candidate=candidate, check=check, request=lambda: None
        )


def test_external_host_or_wrong_content_key_never_requested(monkeypatch):
    backend, candidate, check, opener = byte_storage(monkeypatch)
    for url in ("https://external.example/asset.jpg", candidate["hosted_url"] + "?q=1"):
        with pytest.raises(MediaIncomplete, match="storage_binding"):
            alias.verify_cached_bytes(
                backend,
                username="owner",
                reference=OLD,
                candidate={**candidate, "hosted_url": url},
                check=check,
                request=lambda: None,
            )
    opener.assert_not_called()
    backend.client.digest_object.assert_not_called()


def test_deadline_before_request_stops_and_storage_hash_mismatch(monkeypatch):
    backend, candidate, _, opener = byte_storage(monkeypatch)
    check = MediaOwnershipCheck(lambda: None, deadline=time.monotonic() - 1)
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        alias.verify_cached_bytes(
            backend, username="owner", reference=OLD, candidate=candidate, check=check, request=lambda: None
        )
    opener.assert_not_called()
    backend.client.digest_object.return_value = {"sha256": "f" * 64, "bytes": len(DATA)}
    with pytest.raises(MediaIncomplete, match="storage_bytes"):
        alias.verify_cached_bytes(
            backend,
            username="owner",
            reference=OLD,
            candidate=candidate,
            check=MediaOwnershipCheck(lambda: None, deadline=time.monotonic() + 30),
            request=lambda: None,
        )
    opener.assert_not_called()


def prepare(snapshot_workflow_database, monkeypatch, *, acquire=True, historical_inventory_changed=False):
    store, tx = snapshot_workflow_database
    owned, ids, _, _ = fixtures.seed(snapshot_workflow_database, comments=0)
    handle = "alias" + uuid4().hex[:12]
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_posts set owner_username=%s,owner_profile_pic_url=%s,
            raw_data=jsonb_set(raw_data,'{owner}',%s::jsonb) where id=%s""",
            (handle, OLD, json.dumps({"username": handle, "profile_pic_url": OLD}), ids["post_id"]),
        )
        cur.execute(
            """update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{owner}',%s::jsonb)
            where post_id=%s""",
            (json.dumps({"username": handle, "profile_pic_url": OLD}), ids["post_id"]),
        )
    token = str(store.reserve(**owned)["owner_token"])
    owned = {**owned, "owner_token": token}
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_dispatches(owner_token,run_id,generation,stage,target_key)
            values(%s,%s,%s,'media',%s)""",
            (token, owned["run_id"], owned["generation"], owned["target_key"]),
        )
    if acquire:
        store.acquire(**owned)
    loader = MediaSnapshotLoader(tx)
    binding = {k: owned[k] for k in ("run_id", "account", "generation", "target_key")}
    initial = loader.load(**binding)
    assert initial.avatars[0].source_url == OLD and initial.avatars[0].username == handle
    sealed = Evidence.capture(
        source_identity="instagram:media:" + initial.shortcode,
        captured_at=initial.observed_at,
        parser_version=MEDIA_VERSION,
        payload={
            "shortcode": initial.shortcode,
            "post_id": initial.post_id,
            "canonical_post_id": initial.canonical_post_id,
            "inventory_fingerprint": initial.inventory_fingerprint,
            "ingestion_media": {"version": MEDIA_VERSION, "observed_at": utc_timestamp(initial.observed_at)},
            "media_urls": list(initial.media_urls),
            "expected_media_count": initial.expected_media_count,
            "comments_complete": True,
            "avatars_complete": True,
            "include_comment_avatars": False,
            "comments": [],
            "media": [],
            "avatars": [
                {
                    "username": handle,
                    "profile_pic_url": OLD,
                    "role": "owner",
                    "comment_id": None,
                    "observed_at": utc_timestamp(OBSERVED),
                    "hosted_url": None,
                    "content_hash": None,
                }
            ],
        },
    )
    # Immutable historical source fixture, not a production migration or write.
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_evidence(evidence_id,run_id,stage,target_key,
            source_identity,captured_at,expires_at,content_hash,parser_version,schema_version,payload)
            values(%s,%s,'media',%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                str(uuid4()),
                owned["run_id"],
                initial.shortcode,
                sealed.source_identity,
                sealed.captured_at,
                sealed.expires_at,
                sealed.content_hash,
                sealed.parser_version,
                sealed.schema_version,
                sealed.payload_json,
            ),
        )
    client = Storage()
    client.digest_object = Mock(return_value={"sha256": SHA, "bytes": len(DATA)})  # pyright: ignore[reportAttributeAccessIssue]
    client.close = Mock()  # pyright: ignore[reportAttributeAccessIssue]
    backend = storage(client=client)
    hosted_url = backend._url(alias.expected_key(handle, SHA, OLD))
    with tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at) values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (handle, REGIONAL, SHA, hosted_url, BYTE_CLOCK),
        )
        # A newer, changed native asset has no bearing on this historical one.
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at) values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (
                handle,
                REGIONAL.replace("717993431", "845832610"),
                "8" * 64,
                "https://media.example/new.jpg",
                OBSERVED + timedelta(days=1),
            ),
        )
    monkeypatch.setattr(alias, "safe_requests_get", Mock(return_value=Response(data=DATA)))
    if historical_inventory_changed:
        # A different non-slot branch changed after the retained media snapshot.
        # The owner slot continues to select the exact original 717 version.
        newer = json.dumps({"username": handle, "profile_pic_url": REGIONAL.replace("717993431", "845832610")})
        with tx() as conn, conn.cursor() as cur:
            cur.execute(
                "update social.instagram_posts set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) where id=%s",
                (newer, initial.post_id),
            )
            cur.execute(
                "update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) "
                "where post_id=%s",
                (newer, initial.post_id),
            )
        initial = loader.load(**binding)
        assert initial.inventory_fingerprint != sealed.read(pinned=True)["inventory_fingerprint"]
        assert initial.avatars[0].source_url == OLD
    check = MediaOwnershipCheck(lambda: store.check_owner(**owned), deadline=time.monotonic() + 60)
    callbacks = {"check_cancelled": check, "on_request": lambda: store.check_owner(**owned, record_request=True)}
    return SimpleNamespace(
        store=store,
        tx=tx,
        owned=owned,
        loader=loader,
        binding=binding,
        initial=initial,
        storage=backend,
        callbacks=callbacks,
        handle=handle,
    )


def bridge(f):
    return alias.bridge_cached_avatars(
        f.initial,
        loader=f.loader,
        store=f.store,
        owned=f.owned,
        storage_factory=lambda: f.storage,
        callbacks=f.callbacks,
    )


def proof_rows(f):
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        return alias.read_auxiliary(cur, f.owned["run_id"], f.initial.shortcode, [OLD])


def test_real_loader_bridge_proof_retry_and_independent_certifier(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    result = bridge(f)
    avatar = result.avatars[0]
    assert avatar.source_url == OLD and avatar.observed_at == BYTE_CLOCK and avatar.cached_sha256 == SHA
    assert avatar.download_url is None and avatar.download_provenance is None
    assert result.inventory_fingerprint == f.initial.inventory_fingerprint
    assert result.observed_at == OBSERVED and result.storage_keys() == f.initial.storage_keys()
    f.storage.client.close.assert_called_once()
    rows = proof_rows(f)
    assert len(rows) == 1 and rows[0]["owner_lineage"] is True
    proof = alias.validate_retained(rows[0], run_id=f.owned["run_id"], target_key=f.initial.shortcode)
    assert proof["source_clock"] == utc_timestamp(OBSERVED) and proof["byte_clock"] == utc_timestamp(BYTE_CLOCK)
    assert proof["verified_at"] > proof["source_clock"] and "native_id" not in json.dumps(proof)
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select * from social.instagram_ingestion_evidence where evidence_id=%s", (proof["source_evidence_id"],)
        )
        rows[0]["_source_evidence"] = cur.fetchone()
        cur.execute(
            "select * from social.instagram_ingestion_evidence where evidence_id=%s",
            (proof["current_source_evidence_id"],),
        )
        rows[0]["_current_source_evidence"] = cur.fetchone()
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    failures = []
    with common.bound(context):
        verified = verify_cached_aliases(
            {"target_key": f.initial.shortcode, "owner_generation": 1, "_cached_avatar_aliases": rows},
            result,
            registry,
            datetime.now(UTC),
            True,
            lambda good, code: failures.append(code) if not good else None,
        )
    assert not failures and len(verified) == 1
    f.initial = result
    f.storage.client.digest_object.reset_mock()
    assert bridge(f) == result  # durable proof is validated; no repeated byte IO
    f.storage.client.digest_object.assert_not_called()
    bad = deepcopy(rows)
    bad[0]["owner_lineage"] = False
    with common.bound(context):
        verify_cached_aliases(
            {"target_key": f.initial.shortcode, "owner_generation": 1, "_cached_avatar_aliases": bad},
            result,
            registry,
            datetime.now(UTC),
            True,
            lambda good, code: failures.append(code) if not good else None,
        )
    assert "cached_avatar_alias_auxiliary_proof_invalid" in failures


@pytest.mark.parametrize("drift", ["source", "registry", "owner", "proof"])
def test_drift_after_durable_proof_never_promotes(snapshot_workflow_database, monkeypatch, drift):
    f = prepare(snapshot_workflow_database, monkeypatch)
    retain = f.store.retain_evidence

    def changed(**kwargs):
        evidence_id = retain(**kwargs)
        if kwargs["evidence"].parser_version != alias.VERSION:
            return evidence_id
        with f.tx() as conn, conn.cursor() as cur:
            if drift == "source":
                cur.execute(
                    "update social.instagram_posts set owner_profile_pic_url=%s where id=%s",
                    (REGIONAL.replace("717993431", "845832610"), f.initial.post_id),
                )
            elif drift == "registry":
                cur.execute(
                    "update social.avatar_registry set content_hash=%s where account_handle=%s and source_url=%s",
                    ("f" * 64, f.handle, REGIONAL),
                )
            elif drift == "owner":
                cur.execute(
                    """update social.instagram_ingestion_targets
                    set owner_expires_at=clock_timestamp()-interval '1 second'
                    where run_id=%s and stage='media'""",
                    (f.owned["run_id"],),
                )
            else:
                cur.execute(
                    "update social.instagram_ingestion_evidence set payload=null where evidence_id=%s", (evidence_id,)
                )
        return evidence_id

    monkeypatch.setattr(f.store, "retain_evidence", changed)
    with pytest.raises((MediaIncomplete, LostOwnership)):
        bridge(f)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.avatar_registry where account_handle=%s and source_url=%s", (f.handle, OLD)
        )
        assert cur.fetchone()[0] == 0
    assert len(proof_rows(f)) == 1


def test_crash_before_promotion_retains_proof_and_retry_is_guarded(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    real = alias.write_avatar_version
    monkeypatch.setattr(alias, "write_avatar_version", Mock(side_effect=RuntimeError("fixture crash")))
    with pytest.raises(RuntimeError, match="fixture crash"):
        bridge(f)
    assert len(proof_rows(f)) == 1
    monkeypatch.setattr(alias, "write_avatar_version", real)
    result = bridge(f)
    assert result.avatars[0].cached_sha256 == SHA
    assert len(proof_rows(f)) == 2  # unused proof is preserved, never mistaken for promotion


def test_no_eligible_regional_slot_has_no_added_database_or_byte_work():
    source = SimpleNamespace(avatars=(AvatarSource("owner", "https://scontent.cdninstagram.com/old.jpg"),))
    store, factory = Mock(), Mock()
    assert (
        alias.bridge_cached_avatars(source, loader=Mock(), store=store, owned={}, storage_factory=factory, callbacks={})
        is source
    )
    store.transaction.assert_not_called()
    factory.assert_not_called()


def test_actual_activity_ordinary_snapshot_reload_canonical_receipt_and_replay(snapshot_workflow_database, monkeypatch):
    from scripts.ingestion.january_audit.media import load_cached_aliases, verify_avatars
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, _media

    f = prepare(snapshot_workflow_database, monkeypatch, acquire=False)

    def load(**callbacks):
        return alias.bridge_cached_avatars(
            f.loader.load(**f.binding, **callbacks),
            loader=f.loader,
            store=f.store,
            owned=f.owned,
            storage_factory=lambda: f.storage,
            callbacks=callbacks,
        )

    result = MediaActivity(f.store, storage=f.storage).execute(**f.owned, load=load)
    assert result["outcome"] == "committed"
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select e.* from social.instagram_ingestion_receipts r
            join social.instagram_ingestion_evidence e using(evidence_id)
            where r.run_id=%s and r.stage='media'""",
            (f.owned["run_id"],),
        )
        row = cur.fetchone()
        cur.execute("select * from social.instagram_posts where id=%s", (f.initial.post_id,))
        post = cur.fetchone()
        cur.execute("select * from social.instagram_post_payloads where post_id=%s", (f.initial.post_id,))
        sidecar = cur.fetchone()
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
    parsed = _media(
        alias.evidence_from_row(row),
        target_key=f.initial.shortcode,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        pinned=True,
    )
    assert parsed.result is not None
    assert parsed.result.snapshot.avatars[0].source_url == OLD
    assert parsed.result.avatars[0]["observed_at"] == utc_timestamp(BYTE_CLOCK)
    assert post["owner_profile_pic_url"] == OLD
    assert post["hosted_owner_profile_pic_url"] == f.storage._url(alias.expected_key(f.handle, SHA, OLD))
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    problems = []

    def check(good, code):
        if not good:
            problems.append(code)

    target = {
        "target_key": f.initial.shortcode,
        "target_outcome": "committed",
        "payload": row["payload"],
        "owner_generation": 1,
    }
    with common.bound(context), f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        load_cached_aliases(cur, [target])
        verified = verify_cached_aliases(target, parsed.result.snapshot, registry, datetime.now(UTC), True, check)
        identities = verify_avatars(
            post,
            {**post["raw_data"], **sidecar["raw_data"]},
            [],
            parsed.result.snapshot,
            parsed.result,
            registry,
            datetime.now(UTC),
            check,
        )
    assert not problems and len(verified) == len(identities) == 1
    assert len(context.query_material) == 3  # auxiliary, original seal, independent current seal
    assert all(q["row_count"] == 1 for q in context.query_material)
    assert all("rows_sha256" in q and "payload" not in q for q in context.query_material)


def test_promotion_exception_rolls_back_alias_and_preserves_durable_proof(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    real = alias.write_avatar_version

    def fail_after_write(*args, **kwargs):
        real(*args, **kwargs)
        raise RuntimeError("rollback fixture")

    monkeypatch.setattr(alias, "write_avatar_version", fail_after_write)
    with pytest.raises(RuntimeError, match="rollback fixture"):
        bridge(f)
    assert len(proof_rows(f)) == 1
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.avatar_registry where account_handle=%s and source_url=%s", (f.handle, OLD)
        )
        assert cur.fetchone()[0] == 0


def test_ambiguous_commit_retry_uses_durable_proof_without_new_byte_work(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    real = f.loader.load
    calls = 0

    def lose_readback(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("lost promotion readback")
        return real(**kwargs)

    monkeypatch.setattr(f.loader, "load", lose_readback)
    with pytest.raises(RuntimeError, match="lost promotion readback"):
        bridge(f)
    monkeypatch.setattr(f.loader, "load", real)
    f.initial = real(**f.binding)
    assert f.initial.avatars[0].cached_sha256 == SHA
    f.storage.client.digest_object.reset_mock()
    assert bridge(f) == f.initial
    f.storage.client.digest_object.assert_not_called()
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute("delete from social.instagram_ingestion_dispatches where owner_token=%s", (f.owned["owner_token"],))
    with pytest.raises(MediaIncomplete, match="proof_invalid"):
        bridge(f)


def test_closed_evidence_schema_and_sealed_slot_reject_forged_fields(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    bridge(f)
    row = proof_rows(f)[0]
    proof = alias.evidence_from_row(row).read(pinned=True)
    for changes in (
        {"native_id": "123"},
        {"headers": {"Cookie": "secret"}},
        {"slot": {**proof["slot"], "username": "other"}},
    ):
        bad = {**proof, **changes}
        with pytest.raises((MediaIncomplete, ValueError)):
            Evidence.capture(
                source_identity=row["source_identity"],
                captured_at=row["captured_at"],
                parser_version=alias.VERSION,
                payload=bad,
            )


def test_historical_unrelated_inventory_difference_keeps_slot_and_current_cas(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch, historical_inventory_changed=True)
    result = bridge(f)
    proof = alias.evidence_from_row(proof_rows(f)[0]).read(pinned=True)
    assert proof["sealed_source_inventory"] != proof["current_inventory"]
    assert proof["current_inventory"] == result.inventory_fingerprint
    assert result.avatars[0].source_url == OLD and result.avatars[0].observed_at == BYTE_CLOCK


@pytest.mark.parametrize("change", ["slot_clock", "source_reference", "username", "role", "hash"])
def test_sealed_slot_and_source_hash_must_match_exactly(snapshot_workflow_database, monkeypatch, change):
    f = prepare(snapshot_workflow_database, monkeypatch)
    bridge(f)
    proof = alias.evidence_from_row(proof_rows(f)[0]).read(pinned=True)
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select * from social.instagram_ingestion_evidence where evidence_id=%s", (proof["source_evidence_id"],)
        )
        source = cur.fetchone()
    bad = deepcopy(proof)
    if change == "slot_clock":
        bad["source_clock"] = utc_timestamp(OBSERVED + timedelta(seconds=1))
    elif change == "hash":
        bad["source_evidence_sha256"] = "f" * 64
    else:
        bad["slot"][change] = {"source_reference": REGIONAL, "username": "other", "role": "tagged"}[change]
    with pytest.raises(MediaIncomplete, match="proof_invalid"):
        alias.verify_sealed_source(bad, source)


def test_retry_with_null_auxiliary_payload_fails_closed(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    f.initial = bridge(f)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_ingestion_evidence set payload=null where run_id=%s and parser_version=%s",
            (f.owned["run_id"], alias.VERSION),
        )
    with pytest.raises(MediaIncomplete, match="proof_invalid"):
        bridge(f)


def test_byte_read_cancellation_closes_response_and_owner_temporary_client(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    closed = []

    class Cancelled(Response):
        def iter_content(self, **kwargs):
            assert kwargs["chunk_size"] == 64 * 1024
            yield DATA[:12]
            raise LostOwnership("cancel during full body")

        def __exit__(self, *_):
            closed.append(True)

    monkeypatch.setattr(alias, "safe_requests_get", Mock(return_value=Cancelled()))
    with pytest.raises(LostOwnership):
        bridge(f)
    assert closed == [True]
    f.storage.client.close.assert_called_once()
    assert not proof_rows(f)


def test_dedicated_projection_does_not_expand_ordinary_source_allowlist():
    from trr_backend.ingestion.evidence import CONTENT_KEYS, sanitize_payload

    private = {
        "native_id": "123",
        "owner_token": str(uuid4()),
        "sealed_source_inventory": {"sha256": SHA},
        "storage_binding_sha256": "a" * 64,
        "Cookie": "private",
    }
    assert not (private.keys() & CONTENT_KEYS)
    ordinary = {
        "shortcode": "Fixture",
        "owner": {"username": "owner", **private},
        "profile_pic_url": OLD + "?oh=delivery&access_token=must-not-be-retained",
        **private,
    }
    result = sanitize_payload(ordinary, parser_version="instagram-media-v1")
    assert result == {"shortcode": "Fixture", "owner": {"username": "owner"}, "profile_pic_url": OLD + "?oh=delivery"}


def test_bridge_reload_failure_cannot_fall_through_to_native_profile_requests(monkeypatch):
    from trr_backend.ingestion.composition import _load_media
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing

    load = Mock(return_value=SimpleNamespace(avatars=()))
    monkeypatch.setattr(alias, "bridge_cached_avatars", Mock(side_effect=AvatarSourcesMissing(["owner"])))
    owned = dict.fromkeys(("run_id", "account", "generation", "target_key"), "fixture")
    with pytest.raises(AvatarSourcesMissing):
        _load_media(SimpleNamespace(load=load), Mock(), {}, owned)


def test_unused_old_current_inventory_does_not_authorize_or_block_new_proof(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    real = alias.write_avatar_version
    monkeypatch.setattr(alias, "write_avatar_version", Mock(side_effect=RuntimeError("before promotion")))
    with pytest.raises(RuntimeError):
        bridge(f)
    monkeypatch.setattr(alias, "write_avatar_version", real)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) where id=%s",
            (
                json.dumps({"username": f.handle, "profile_pic_url": REGIONAL.replace("717993431", "845832610")}),
                f.initial.post_id,
            ),
        )
    f.initial = f.loader.load(**f.binding)
    result = bridge(f)
    f.initial = result
    assert bridge(f) == result
    rows = proof_rows(f)
    assert len(rows) == 2
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        for row in rows:
            proof = alias.evidence_from_row(row).read(pinned=True)
            cur.execute(
                "select * from social.instagram_ingestion_evidence where evidence_id=%s", (proof["source_evidence_id"],)
            )
            row["_source_evidence"] = cur.fetchone()
            cur.execute(
                "select * from social.instagram_ingestion_evidence where evidence_id=%s",
                (proof["current_source_evidence_id"],),
            )
            row["_current_source_evidence"] = cur.fetchone()
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    problems = []
    target = {"target_key": f.initial.shortcode, "owner_generation": 1, "_cached_avatar_aliases": rows}
    with common.bound(context):
        accepted = verify_cached_aliases(
            target,
            result,
            registry,
            datetime.now(UTC),
            True,
            lambda good, code: problems.append(code) if not good else None,
        )
        assert not problems and len(accepted) == 1
        changed = replace(result, inventory_fingerprint={"version": "instagram-media-inventory-v1", "sha256": "d" * 64})
        verify_cached_aliases(
            target,
            changed,
            registry,
            datetime.now(UTC),
            True,
            lambda good, code: problems.append(code) if not good else None,
        )
    assert "cached_avatar_alias_current_inventory_or_result_mismatch" in problems


def test_malformed_image_and_network_error_are_fixed_diagnostics(monkeypatch):
    backend, candidate, check, _ = byte_storage(monkeypatch)
    invalid = b"not an image"
    digest = hashlib.sha256(invalid).hexdigest()
    candidate = {**candidate, "sha256": digest, "hosted_url": backend._url(alias.expected_key("owner", digest, OLD))}
    backend.client.digest_object.return_value = {"sha256": digest, "bytes": len(invalid)}
    monkeypatch.setattr(alias, "safe_requests_get", Mock(return_value=Response(data=invalid)))
    with pytest.raises(MediaIncomplete, match="byte_verification_failed"):
        alias.verify_cached_bytes(
            backend, username="owner", reference=OLD, candidate=candidate, check=check, request=lambda: None
        )


def audit_current(f, snapshot):
    from scripts.ingestion.january_audit.media import load_cached_aliases
    from trr_backend.ingestion.media import snapshot_evidence

    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    problems = []
    with common.bound(context), f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select owner_generation from social.instagram_ingestion_targets where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        generation = cur.fetchone()["owner_generation"]
        target = {
            "target_key": snapshot.shortcode,
            "target_outcome": "committed",
            "payload": snapshot_evidence(snapshot).payload_json,
            "owner_generation": generation,
        }
        load_cached_aliases(cur, [target])
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        verified = verify_cached_aliases(
            target,
            snapshot,
            cur.fetchall(),
            datetime.now(UTC),
            True,
            lambda good, code: problems.append(code) if not good else None,
        )
    return problems, verified, target


def invalidate_and_reacquire(f):
    from trr_backend.ingestion.media import invalidate_media_inventory

    with f.tx() as conn, conn.cursor() as cur:
        invalidate_media_inventory(cur, f.owned["run_id"], f.initial.shortcode)
    binding = {k: v for k, v in f.owned.items() if k != "owner_token"}
    f.owned = {**binding, "owner_token": str(f.store.reserve(**binding)["owner_token"])}
    f.store.acquire(**f.owned)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.instagram_ingestion_dispatches(owner_token,run_id,generation,stage,target_key)
            values(%s,%s,%s,'media',%s)""",
            (f.owned["owner_token"], f.owned["run_id"], f.owned["generation"], f.initial.shortcode),
        )
    f.callbacks = {
        "check_cancelled": MediaOwnershipCheck(lambda: f.store.check_owner(**f.owned), deadline=time.monotonic() + 60),
        "on_request": lambda: f.store.check_owner(**f.owned, record_request=True),
    }
    f.initial = f.loader.load(**f.binding)


def change_unrelated_inventory(f):
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) where id=%s",
            (
                json.dumps({"username": f.handle, "profile_pic_url": REGIONAL.replace("717993431", "845832610")}),
                f.initial.post_id,
            ),
        )
    invalidate_and_reacquire(f)


def change_owner_role(f, *, roles=("tagged", "mention")):
    from tests.ingestion.test_media import SOURCE

    owner = {"username": "other_owner", "profile_pic_url": SOURCE}
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.instagram_posts set owner_username='other_owner',owner_profile_pic_url=%s,
            profile_tags=%s::jsonb,mentions=%s::jsonb,tagged_users_detail=%s::jsonb,
            raw_data=jsonb_set(jsonb_set(raw_data,'{owner}',%s::jsonb),'{owner_detail}',%s::jsonb) where id=%s""",
            (
                SOURCE,
                json.dumps([f.handle] if "tagged" in roles else []),
                json.dumps([f.handle] if "mention" in roles else []),
                json.dumps([{"username": f.handle, "profile_pic_url": OLD}]),
                json.dumps(owner),
                json.dumps(owner),
                f.initial.post_id,
            ),
        )
        cur.execute(
            """update social.instagram_post_payloads set raw_data=jsonb_set(
            jsonb_set(raw_data,'{owner}',%s::jsonb),'{owner_detail}',%s::jsonb) where post_id=%s""",
            (json.dumps(owner), json.dumps(owner), f.initial.post_id),
        )
    invalidate_and_reacquire(f)


def test_first_attempt_ordinary_seal_bridge_and_normal_receipt_with_no_prior_source(
    snapshot_workflow_database, monkeypatch
):
    from trr_backend.ingestion.media import snapshot_evidence

    f = prepare(snapshot_workflow_database, monkeypatch, acquire=False)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from social.instagram_ingestion_evidence where run_id=%s and stage='media'", (f.owned["run_id"],)
        )
    normal = snapshot_evidence(f.initial)
    before_bytes = alias.verify_cached_bytes

    def assert_sealed(*args, **kwargs):
        with f.tx() as conn, conn.cursor() as cur:
            cur.execute(
                "select content_hash,payload from social.instagram_ingestion_evidence "
                "where run_id=%s and parser_version=%s",
                (f.owned["run_id"], MEDIA_VERSION),
            )
            rows = cur.fetchall()
        assert rows == [(normal.content_hash, normal.payload_json)]
        return before_bytes(*args, **kwargs)

    monkeypatch.setattr(alias, "verify_cached_bytes", assert_sealed)

    def load(**callbacks):
        return alias.bridge_cached_avatars(
            f.loader.load(**f.binding, **callbacks),
            loader=f.loader,
            store=f.store,
            owned=f.owned,
            storage_factory=lambda: f.storage,
            callbacks=callbacks,
        )

    outcome = MediaActivity(f.store, storage=f.storage).execute(**f.owned, load=load)
    assert outcome["outcome"] == "committed"
    rows = proof_rows(f)
    proof = alias.evidence_from_row(rows[0]).read(pinned=True)
    assert proof["source_evidence_id"] == proof["current_source_evidence_id"]
    f.initial = f.loader.load(**f.binding)
    problems, verified, _ = audit_current(f, f.initial)
    assert not problems and len(verified) == 1


def test_crash_after_normal_sealing_leaves_replayable_source_and_retry_finishes(
    snapshot_workflow_database, monkeypatch
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from social.instagram_ingestion_evidence where run_id=%s and stage='media'", (f.owned["run_id"],)
        )
    retain = f.store.retain_evidence

    def crash(**kwargs):
        value = retain(**kwargs)
        if kwargs["evidence"].parser_version == MEDIA_VERSION:
            raise RuntimeError("crash after ordinary seal")
        return value

    monkeypatch.setattr(f.store, "retain_evidence", crash)
    with pytest.raises(RuntimeError, match="ordinary seal"):
        bridge(f)
    f.storage.client.digest_object.assert_not_called()
    assert not proof_rows(f)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.instagram_ingestion_evidence where run_id=%s and parser_version=%s",
            (f.owned["run_id"], MEDIA_VERSION),
        )
        assert cur.fetchone()[0] == 1
    monkeypatch.setattr(f.store, "retain_evidence", retain)
    result = bridge(f)
    assert result.avatars[0].cached_sha256 == SHA
    assert not audit_current(f, result)[0]


def test_postpromotion_inventory_invalidation_reauthorizes_without_registry_rewrite(
    snapshot_workflow_database, monkeypatch
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    f.initial = bridge(f)
    root_row = proof_rows(f)[0]
    root = alias.evidence_from_row(root_row).read(pinned=True)
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s and source_url=%s", (f.handle, OLD))
        before = cur.fetchone()
    change_unrelated_inventory(f)
    f.storage.client.digest_object.reset_mock()
    writer = Mock(side_effect=AssertionError("reauthorization must not rewrite registry"))
    monkeypatch.setattr(alias, "write_avatar_version", writer)
    result = bridge(f)
    writer.assert_not_called()
    f.storage.client.digest_object.assert_called_once()
    proofs = [alias.evidence_from_row(r).read(pinned=True) for r in proof_rows(f)]
    reauth = next(p for p in proofs if p["parent_evidence_id"] is not None)
    assert reauth["parent_evidence_id"] == str(root_row["evidence_id"])
    assert reauth["parent_evidence_sha256"] == root_row["content_hash"]
    assert reauth["current_inventory"] == result.inventory_fingerprint != root["current_inventory"]
    assert reauth["source_clock"] == root["source_clock"] and reauth["byte_clock"] == root["byte_clock"]
    assert reauth["source_evidence_id"] == root["source_evidence_id"]
    assert reauth["current_source_evidence_id"] != root["current_source_evidence_id"]
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s and source_url=%s", (f.handle, OLD))
        assert cur.fetchone() == before
    assert not audit_current(f, result)[0]


def test_changed_and_removed_roles_require_explicit_current_seals_and_partial_retry(
    snapshot_workflow_database, monkeypatch
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    f.initial = bridge(f)
    root_row = proof_rows(f)[0]
    change_owner_role(f)
    f.storage.client.digest_object.reset_mock()
    real_verify = alias.verify_cached_bytes
    completed = 0

    def crash_second_role(*args, **kwargs):
        nonlocal completed
        completed += 1
        if completed == 2:
            raise RuntimeError("second role interrupted")
        return real_verify(*args, **kwargs)

    monkeypatch.setattr(alias, "verify_cached_bytes", crash_second_role)
    with pytest.raises(RuntimeError, match="second role"):
        bridge(f)
    partial = proof_rows(f)
    assert len(partial) == 2  # root + first explicit current role
    problems, _, _ = audit_current(f, f.initial)
    assert "cached_avatar_alias_current_inventory_or_result_mismatch" in problems
    monkeypatch.setattr(alias, "verify_cached_bytes", real_verify)
    result = bridge(f)
    proofs = [alias.evidence_from_row(r).read(pinned=True) for r in proof_rows(f)]
    assert {p["current_slot"]["role"] for p in proofs if p["parent_evidence_id"]} == {"tagged", "mention"}
    assert all(p["slot"]["role"] == "owner" for p in proofs)
    assert all(p["parent_evidence_id"] in {None, str(root_row["evidence_id"])} for p in proofs)
    assert not audit_current(f, result)[0]
    f.initial = result
    change_owner_role(f, roles=("mention",))
    result = bridge(f)
    problems, verified, _ = audit_current(f, result)
    assert not problems and len(verified) == 1
    assert verified[0]["evidence_id"] not in {str(r["evidence_id"]) for r in partial}


def test_ordinary_snapshot_helper_preserves_unavailable_object(snapshot_workflow_database, monkeypatch):
    from trr_backend.ingestion.media import snapshot_evidence

    f = prepare(snapshot_workflow_database, monkeypatch)
    evidence = Evidence.capture(
        source_identity="instagram:media:" + f.initial.shortcode,
        captured_at=f.initial.observed_at,
        parser_version=MEDIA_VERSION,
        payload={"status": "deleted", "shortcode": f.initial.shortcode},
    )
    unavailable = replace(f.initial, unavailable=evidence)
    assert snapshot_evidence(unavailable) is evidence
    assert (
        alias.bridge_cached_avatars(
            unavailable, loader=Mock(), store=Mock(), owned={}, storage_factory=Mock(), callbacks={}
        )
        is unavailable
    )


@pytest.mark.parametrize("failure", ["bytes", "newer_unproved", "owner", "source", "current_seal", "parent"])
def test_reauthorization_guards_preserve_exact_cache_and_reject_unsafe_authorization(
    snapshot_workflow_database, monkeypatch, failure
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    f.initial = bridge(f)
    root_row = proof_rows(f)[0]
    change_unrelated_inventory(f)
    f.storage.client.digest_object.reset_mock()
    writer = Mock(side_effect=AssertionError("no registry rewrite"))
    monkeypatch.setattr(alias, "write_avatar_version", writer)
    if failure == "bytes":
        f.storage.client.digest_object.return_value = {"sha256": "f" * 64, "bytes": len(DATA)}
    elif failure == "newer_unproved":
        with f.tx() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into social.avatar_registry(platform,account_handle,source_url,status,last_checked_at)
                values('instagram',%s,%s,'failed',%s)""",
                (f.handle, REGIONAL.replace("iad6", "ams1"), datetime.now(UTC)),
            )
    else:
        retain = f.store.retain_evidence

        def change_after_authorization(**kwargs):
            evidence_id = retain(**kwargs)
            if kwargs["evidence"].parser_version != alias.VERSION:
                return evidence_id
            proof = kwargs["evidence"].read(pinned=True)
            if not proof["parent_evidence_id"]:
                return evidence_id
            with f.tx() as conn, conn.cursor() as cur:
                if failure == "owner":
                    cur.execute(
                        """update social.instagram_ingestion_targets
                        set owner_expires_at=clock_timestamp()-interval '1 second' where run_id=%s and stage='media'""",
                        (f.owned["run_id"],),
                    )
                elif failure == "source":
                    cur.execute(
                        "update social.instagram_posts set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) where id=%s",
                        (json.dumps({"username": "drift"}), f.initial.post_id),
                    )
                else:
                    selected = (
                        proof["current_source_evidence_id"]
                        if failure == "current_seal"
                        else str(root_row["evidence_id"])
                    )
                    cur.execute(
                        "update social.instagram_ingestion_evidence set payload=null where evidence_id=%s", (selected,)
                    )
            return evidence_id

        monkeypatch.setattr(f.store, "retain_evidence", change_after_authorization)
    with pytest.raises((MediaIncomplete, LostOwnership, ValueError, TypeError)):
        bridge(f)
    writer.assert_not_called()
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """select content_hash,hosted_url,last_checked_at from social.avatar_registry
            where account_handle=%s and source_url=%s""",
            (f.handle, OLD),
        )
        sha, url, observed = cur.fetchone()
        assert sha == SHA and observed == BYTE_CLOCK
        assert url == f.storage._url(alias.expected_key(f.handle, SHA, OLD))


def test_auditor_rejects_missing_or_altered_current_slot_and_root_lineage(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    f.initial = bridge(f)
    change_owner_role(f, roles=("mention",))
    result = bridge(f)
    problems, verified, target = audit_current(f, result)
    assert not problems and len(verified) == 1
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
    for damage in ("current_seal", "parent_missing", "parent_hash", "current_role"):
        bad = deepcopy(target)
        row = next(
            r
            for r in bad["_cached_avatar_aliases"]
            if alias.evidence_from_row(r).read(pinned=True)["parent_evidence_id"]
        )
        proof = alias.evidence_from_row(row).read(pinned=True)
        if damage == "current_seal":
            row["_current_source_evidence"]["content_hash"] = "f" * 64
        elif damage == "parent_missing":
            bad["_cached_avatar_aliases"] = [row]
        else:
            if damage == "parent_hash":
                proof["parent_evidence_sha256"] = "f" * 64
            else:
                proof["current_slot"]["role"] = "collaborator"
            sealed = Evidence.capture(
                source_identity=row["source_identity"],
                captured_at=row["captured_at"],
                parser_version=alias.VERSION,
                payload=proof,
            )
            row.update(payload=sealed.payload_json, content_hash=sealed.content_hash)
        failures = []
        with common.bound(context):
            verify_cached_aliases(
                bad,
                result,
                registry,
                datetime.now(UTC),
                True,
                lambda good, code, failures=failures: failures.append(code) if not good else None,
            )
        assert failures, damage


def grown_cohort(f):
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at) values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (
                f.handle,
                REGIONAL.replace("iad6", "ams1"),
                SHA,
                f.storage._url(alias.expected_key(f.handle, SHA, OLD)),
                BYTE_CLOCK,
            ),
        )


def prior_authorization(f):
    f.initial = bridge(f)
    change_unrelated_inventory(f)
    f.initial = bridge(f)
    problems, verified, _ = audit_current(f, f.initial)
    assert not problems and len(verified) == 1 and len(proof_rows(f)) == 2


def test_cohort_growth_preserves_old_child_history_and_accepts_only_fresh_authorization(
    snapshot_workflow_database, monkeypatch
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    prior_authorization(f)
    old = {str(r["evidence_id"]) for r in proof_rows(f)}
    grown_cohort(f)
    f.storage.client.digest_object.reset_mock()
    f.initial = bridge(f)
    f.storage.client.digest_object.assert_called_once()
    problems, verified, _ = audit_current(f, f.initial)
    assert not problems and len(verified) == 1
    assert len(proof_rows(f)) == 3
    assert verified[0]["evidence_id"] not in old


def test_cohort_growth_without_fresh_authorization_stays_blocked(snapshot_workflow_database, monkeypatch):
    f = prepare(snapshot_workflow_database, monkeypatch)
    prior_authorization(f)
    grown_cohort(f)
    problems, verified, _ = audit_current(f, f.initial)
    assert verified == []
    assert problems == ["cached_avatar_alias_current_inventory_or_result_mismatch"]
    assert len(proof_rows(f)) == 2


@pytest.mark.parametrize(
    "damage", ["schema", "hash", "original_source", "current_seal", "root", "account", "post", "owner", "time"]
)
def test_stale_cohort_history_is_validated_before_current_authorization_filter(
    snapshot_workflow_database, monkeypatch, damage
):
    f = prepare(snapshot_workflow_database, monkeypatch)
    prior_authorization(f)
    previous = {str(r["evidence_id"]) for r in proof_rows(f)}
    grown_cohort(f)
    f.initial = bridge(f)
    problems, verified, target = audit_current(f, f.initial)
    assert not problems and len(verified) == 1
    bad = deepcopy(target)
    old_child = next(
        r
        for r in bad["_cached_avatar_aliases"]
        if str(r["evidence_id"]) in previous
        and alias.evidence_from_row(r).read(pinned=True)["parent_evidence_id"] is not None
    )
    proof = alias.evidence_from_row(old_child).read(pinned=True)
    if damage == "schema":
        payload = json.loads(old_child["payload"])
        payload["unreviewed_flag"] = True
        old_child.update(
            payload=json.dumps(payload, sort_keys=True, separators=(",", ":")), content_hash=fingerprint(payload)
        )
    elif damage == "hash":
        old_child["content_hash"] = "f" * 64
    elif damage == "original_source":
        old_child["_source_evidence"]["content_hash"] = "f" * 64
    elif damage == "current_seal":
        old_child["_current_source_evidence"]["content_hash"] = "f" * 64
    elif damage == "owner":
        old_child["owner_lineage"] = False
    else:
        if damage == "root":
            proof["parent_evidence_sha256"] = "f" * 64
        elif damage == "account":
            proof["account"] = "wrong_account"
        elif damage == "post":
            proof["post_id"] = str(uuid4())
        else:
            proof["verified_at"] = utc_timestamp(datetime.now(UTC) + timedelta(days=1))
            old_child["captured_at"] = datetime.fromisoformat(proof["verified_at"])
        evidence = Evidence.capture(
            source_identity=old_child["source_identity"],
            captured_at=old_child["captured_at"],
            parser_version=alias.VERSION,
            payload=proof,
        )
        old_child.update(payload=evidence.payload_json, content_hash=evidence.content_hash)
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], OBSERVED, OBSERVED + timedelta(days=31)
    )
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
    failures = []
    with common.bound(context):
        verify_cached_aliases(
            bad,
            f.initial,
            registry,
            datetime.now(UTC),
            True,
            lambda good, code: failures.append(code) if not good else None,
        )
    assert failures, damage
    if damage == "time":
        assert "cached_avatar_alias_future_verification" in failures
    else:
        assert "cached_avatar_alias_auxiliary_proof_invalid" in failures
