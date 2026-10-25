"""Owner-scoped saved asset recovery: isolated PostgreSQL and mock object bytes."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock
from urllib.parse import urlsplit

import pytest
from psycopg2.extras import RealDictCursor

from tests.ingestion import test_cached_avatar_alias as fixtures
from tests.ingestion.test_media import MediaActivity, Response
from trr_backend.ingestion import cached_avatar_alias as cached
from trr_backend.ingestion import saved_owner_asset as saved
from trr_backend.ingestion.domain import LostOwnership
from trr_backend.ingestion.media import MediaIncomplete, mirror_snapshot

# Reuse guarded isolated PostgreSQL fixtures; no configured TRR services.
database = fixtures.database
media_database = fixtures.media_database
snapshot_database = fixtures.snapshot_database
snapshot_workflow_database = fixtures.snapshot_workflow_database
OLD, REGIONAL, DATA, SHA = fixtures.OLD, fixtures.REGIONAL, fixtures.DATA, fixtures.SHA


def setup(db, monkeypatch, *, acquire=True):
    f = fixtures.prepare(db, monkeypatch, acquire=acquire)
    historical = "old" + f.handle
    old_key = cached.expected_key(historical, SHA, OLD)
    old_url = f.storage._url(old_key)
    detail = {"username": historical, "user_id": "11425198", "profile_pic_url": OLD}
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """update social.avatar_registry set account_handle=%s,hosted_url=%s
            where account_handle=%s and source_url=%s""",
            (historical, old_url, f.handle, REGIONAL),
        )
        cur.execute("delete from social.avatar_registry where account_handle=%s", (f.handle,))
        cur.execute(
            """update social.instagram_posts set hosted_owner_profile_pic_url=%s,
            owner_user_id='17841400995940178',raw_data=jsonb_set(raw_data,'{owner_detail}',%s::jsonb) where id=%s""",
            (old_url, json.dumps(detail), f.initial.post_id),
        )
        cur.execute(
            "update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{owner_de"
            "tail}',%s::jsonb) where post_id=%s",
            (json.dumps(detail), f.initial.post_id),
        )
    client = f.storage.client
    client.objects[old_key] = DATA, {"sha256": SHA}

    def digest(**kwargs):
        body = client.objects[kwargs["Key"]][0]
        return {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}

    client.digest_object = Mock(side_effect=digest)

    def opener(_session, url, **kwargs):
        assert kwargs["allow_redirects"] is False and kwargs["stream"] is True
        assert kwargs["headers"] == {"Accept-Encoding": "identity"}
        key = urlsplit(url).path.lstrip("/")
        body = client.objects[key][0] if key in client.objects else b"missing"
        return Response(
            data=body, status=200 if key in client.objects else 404, headers={"Content-Length": str(len(body))}
        )

    monkeypatch.setattr(cached, "safe_requests_get", opener)
    monkeypatch.setattr(saved, "safe_requests_get", opener)
    f.initial = f.loader.load(**f.binding)
    f.historical, f.old_key, f.old_url, f.opener = historical, old_key, old_url, opener
    f.destination_key = cached.expected_key(f.handle, SHA, old_url)
    f.destination_url = f.storage._url(f.destination_key)
    return f


def recover(f):
    return saved.recover(
        f.initial,
        loader=f.loader,
        store=f.store,
        owned=f.owned,
        storage_factory=lambda: f.storage,
        callbacks=f.callbacks,
    )


def records(f):
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        return saved.read_records(cur, f.owned["run_id"], f.initial.shortcode, [OLD])


def loader(f):
    def load(**callbacks):
        return saved.recover(
            f.loader.load(**f.binding, **callbacks),
            loader=f.loader,
            store=f.store,
            owned=f.owned,
            storage_factory=lambda: f.storage,
            callbacks=callbacks,
        )

    return load


def test_initial_copy_is_exact_current_namespace_and_only_receipt_promotes(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    before = dict(f.storage.client.objects)
    result = recover(f)
    assert result.avatars[0].source_url == OLD and result.avatars[0].username == f.handle
    assert result.avatars[0].cached_hosted_url == f.destination_url and result.avatars[0].cached_sha256 == SHA
    assert result.avatars[0].observed_at == fixtures.BYTE_CLOCK
    assert f.storage.client.objects[f.old_key] == before[f.old_key]
    assert f.storage.client.objects[f.destination_key][0] == DATA
    assert result.inventory_fingerprint == f.initial.inventory_fingerprint
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.avatar_registry where account_handle=%s", (f.handle,))
        assert cur.fetchone()[0] == 0
        cur.execute("select hosted_owner_profile_pic_url from social.instagram_posts where id=%s", (f.initial.post_id,))
        assert cur.fetchone()[0] == f.old_url
    r = list(records(f).values())
    assert len(r) == 1
    assert r[0]["origin"]["slot"]["role"] == "owner"
    assert r[0]["origin"]["binding"]["branches"][0]["username"] == f.historical
    assert "native_id" not in json.dumps(r[0]["auth"])


def test_real_media_activity_atomic_registry_canonical_receipt(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch, acquire=False)
    # Real first attempt, with no prior ordinary source evidence.
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from social.instagram_ingestion_evidence where run_id=%s and stage='media'", (f.owned["run_id"],)
        )
    outcome = MediaActivity(f.store, storage=f.storage).execute(**f.owned, load=loader(f))
    assert outcome["outcome"] == "committed"
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        row = cur.fetchone()
        assert row["source_url"] == OLD and row["content_hash"] == SHA and row["hosted_url"] == f.destination_url
        assert row["last_checked_at"] == fixtures.BYTE_CLOCK
        cur.execute(
            "select hosted_owner_profile_pic_url,owner_user_id,owner_profile_pic_url,raw_data"
            " from social.instagram_posts where id=%s",
            (f.initial.post_id,),
        )
        post = cur.fetchone()
        assert post["hosted_owner_profile_pic_url"] == f.destination_url and post["owner_profile_pic_url"] == OLD
        assert post["owner_user_id"] == "17841400995940178"
        assert post["raw_data"]["owner_detail"]["user_id"] == "11425198"
        cur.execute(
            "select outcome from social.instagram_ingestion_receipts where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        assert cur.fetchone()["outcome"] == "committed"


@pytest.mark.parametrize("damage", ["hosted", "branch", "path", "hash", "unproved"])
def test_root_requires_explicit_saved_samepost_binding_and_unambiguous_cache(
    snapshot_workflow_database, monkeypatch, damage
):
    f = setup(snapshot_workflow_database, monkeypatch)
    with f.tx() as conn, conn.cursor() as cur:
        if damage == "hosted":
            cur.execute(
                "update social.instagram_posts set hosted_owner_profile_pic_url='https://foreign."
                "example/object.jpg' where id=%s",
                (f.initial.post_id,),
            )
        elif damage in {"branch", "path"}:
            detail = {
                "username": "different" if damage == "branch" else f.historical,
                "profile_pic_url": OLD if damage == "branch" else OLD.replace("717993431", "746862330"),
            }
            cur.execute(
                "update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{owner_de"
                "tail}',%s::jsonb) where post_id=%s",
                (json.dumps(detail), f.initial.post_id),
            )
        elif damage == "hash":
            cur.execute(
                """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
                hosted_url,status,last_checked_at)
                values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
                (f.historical, REGIONAL.replace("iad6", "ams1"), "f" * 64, f.old_url, fixtures.BYTE_CLOCK),
            )
        else:
            cur.execute(
                """insert into social.avatar_registry(platform,account_handle,source_url,status,last_checked_at)
                values('instagram',%s,%s,'failed',%s)""",
                (f.historical, REGIONAL.replace("iad6", "ams1"), datetime.now(UTC)),
            )
    f.initial = f.loader.load(**f.binding)
    with pytest.raises(MediaIncomplete):
        recover(f)
    assert f.destination_key not in f.storage.client.objects


def commit_prepared(f, prepared, *, fail=False):
    from trr_backend.ingestion.evidence import Evidence
    from trr_backend.ingestion.media import write_media
    from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, serialize_media_result

    result = mirror_snapshot(
        prepared, f.storage, check=f.callbacks["check_cancelled"], request=f.callbacks["on_request"]
    )
    evidence = Evidence.capture(
        source_identity="instagram:media:" + prepared.shortcode,
        captured_at=prepared.observed_at,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        payload=serialize_media_result(result),
    )
    evidence_id = f.store.retain_evidence(**f.owned, evidence=evidence)

    def write(conn):
        identity = write_media(
            conn,
            run_id=f.owned["run_id"],
            account=f.owned["account"],
            target_key=prepared.shortcode,
            observed_at=prepared.observed_at,
            result=result,
        )
        if fail:
            raise RuntimeError("rollback business receipt")
        return identity

    f.store.commit_target(**f.owned, observed_at=prepared.observed_at, evidence_id=evidence_id, write_canonical=write)
    return result


def audit(f, result):
    from scripts.ingestion.january_audit import common
    from scripts.ingestion.january_audit.media import load_saved_owner_assets, verify_saved_owner_assets
    from trr_backend.ingestion.replay import serialize_media_result

    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], fixtures.OBSERVED, fixtures.OBSERVED + timedelta(days=31)
    )
    target = {
        "target_key": result.snapshot.shortcode,
        "target_outcome": "committed",
        "payload": json.dumps(serialize_media_result(result)),
        "target_owner_token": f.owned["owner_token"],
    }
    failures = []
    with common.bound(context), f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select owner_generation from social.instagram_ingestion_targets where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        target["target_owner_generation"] = cur.fetchone()["owner_generation"]
        load_saved_owner_assets(cur, [target], datetime.now(UTC), True)
        cur.execute("select * from social.instagram_posts where id=%s", (f.initial.post_id,))
        post = cur.fetchone()
        cur.execute("select * from social.instagram_post_payloads where post_id=%s", (f.initial.post_id,))
        sidecar = cur.fetchone()
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
        identities = verify_saved_owner_assets(
            target,
            result.snapshot,
            result,
            post,
            sidecar,
            registry,
            lambda good, code: failures.append(code) if not good else None,
        )
    return failures, identities, context, target


def test_independent_record_audit_binds_original_and_destination_after_commit(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    result = commit_prepared(f, recover(f))
    failures, identities, before, _ = audit(f, result)
    assert not failures and len(identities) == 1
    identity = identities[0]
    assert identity["historical_handle"] == f.historical and identity["current_handle"] == f.handle
    assert identity["source_url"] == f.old_url and identity["source_key"] == f.old_key
    assert identity["source_clock"] == saved.stamp(fixtures.OBSERVED)
    assert identity["byte_clock"] == saved.stamp(fixtures.BYTE_CLOCK)
    assert before.query_material and all("rows_sha256" in q for q in before.query_material)
    failures, _, after, _ = audit(f, result)
    assert not failures and before.query_material == after.query_material


def test_copy_only_crash_retry_seals_uncached_input_and_does_not_rewrite_original(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    original = dict(f.storage.client.objects)
    verify = cached.verify_cached_bytes

    def fail_destination(storage, **kwargs):
        if kwargs["username"] == f.handle:
            raise RuntimeError("lost after immutable copy")
        return verify(storage, **kwargs)

    monkeypatch.setattr(cached, "verify_cached_bytes", fail_destination)
    with pytest.raises(RuntimeError, match="immutable copy"):
        recover(f)
    assert f.storage.client.objects[f.destination_key][0] == DATA
    assert not records(f)
    assert f.storage.client.objects[f.old_key] == original[f.old_key]
    monkeypatch.setattr(cached, "verify_cached_bytes", verify)
    prepared = recover(f)
    assert list(records(f).values())[0]["input"]["input_slot"]["hosted_url"] is None
    assert not audit(f, commit_prepared(f, prepared))[0]


def test_prewrite_rollback_and_retry_preserve_root_and_normal_receipt_atomicity(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    prepared = recover(f)
    root = next(iter(records(f)))
    with pytest.raises(RuntimeError, match="business receipt"):
        commit_prepared(f, prepared, fail=True)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.avatar_registry where account_handle=%s", (f.handle,))
        assert cur.fetchone()[0] == 0
        cur.execute(
            "select count(*) from social.instagram_ingestion_receipts where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        assert cur.fetchone()[0] == 0
        cur.execute("select hosted_owner_profile_pic_url from social.instagram_posts where id=%s", (f.initial.post_id,))
        assert cur.fetchone()[0] == f.old_url
    result = commit_prepared(f, recover(f))
    proofs = records(f)
    assert len(proofs) == 2
    child = next(p for p in proofs.values() if p["auth"]["parent_id"])
    assert child["auth"]["parent_id"] == root
    assert child["input"]["input_slot"]["hosted_url"] is None  # parent does not imply cached input
    assert not audit(f, result)[0]


def test_postcommit_invalidation_uses_cached_input_and_preserves_clocks(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    commit_prepared(f, recover(f))
    root = next(iter(records(f)))
    f.initial = f.loader.load(**f.binding)
    fixtures.change_unrelated_inventory(f)
    result = commit_prepared(f, recover(f))
    child = next(p for p in records(f).values() if p["auth"]["parent_id"])
    assert child["auth"]["parent_id"] == root
    assert child["input"]["input_slot"]["hosted_url"] == f.destination_url
    assert child["input"]["byte_clock"] == saved.stamp(fixtures.BYTE_CLOCK)
    assert child["origin"]["source_clock"] == saved.stamp(fixtures.OBSERVED)
    assert not audit(f, result)[0]


def test_changed_roles_get_fresh_explicit_authorizations_removed_roles_are_history(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    commit_prepared(f, recover(f))
    f.initial = f.loader.load(**f.binding)
    fixtures.change_owner_role(f)
    result = commit_prepared(f, recover(f))
    proofs = records(f)
    assert {p["input"]["slot"]["role"] for p in proofs.values() if p["auth"]["parent_id"]} == {"tagged", "mention"}
    assert all(p["origin"]["slot"]["role"] == "owner" for p in proofs.values())
    failures, identities, _, _ = audit(f, result)
    assert not failures and len(identities) == 2
    f.initial = f.loader.load(**f.binding)
    fixtures.change_owner_role(f, roles=("mention",))
    result = commit_prepared(f, recover(f))
    failures, identities, _, _ = audit(f, result)
    assert not failures and len(identities) == 1


@pytest.mark.parametrize("drift", ["hosted", "branch", "inventory", "cohort", "owner"])
def test_commit_guard_rechecks_every_prewrite_binding_before_any_promotion(
    snapshot_workflow_database, monkeypatch, drift
):
    f = setup(snapshot_workflow_database, monkeypatch)
    prepared = recover(f)
    with f.tx() as conn, conn.cursor() as cur:
        if drift == "hosted":
            cur.execute(
                "update social.instagram_posts set hosted_owner_profile_pic_url='https://media.ex"
                "ample/changed.jpg' where id=%s",
                (f.initial.post_id,),
            )
        elif drift == "branch":
            cur.execute(
                "update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{owner_de"
                "tail,user_id}','\"changed\"'::jsonb) where post_id=%s",
                (f.initial.post_id,),
            )
        elif drift == "inventory":
            cur.execute(
                "update social.instagram_posts set owner_profile_pic_url=%s where id=%s",
                (OLD.replace("717993431", "746862330"), f.initial.post_id),
            )
        elif drift == "cohort":
            cur.execute(
                """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
                hosted_url,status,last_checked_at)
                values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
                (f.historical, REGIONAL.replace("iad6", "ams1"), SHA, f.old_url, fixtures.BYTE_CLOCK),
            )
        else:
            cur.execute(
                "update social.instagram_ingestion_targets set owner_expires_at=clock_timestamp()"
                "-interval '1 second' where run_id=%s and stage='media'",
                (f.owned["run_id"],),
            )
    with pytest.raises((ValueError, LostOwnership)):
        commit_prepared(f, prepared)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from social.avatar_registry where account_handle=%s", (f.handle,))
        assert cur.fetchone()[0] == 0
        cur.execute(
            "select count(*) from social.instagram_ingestion_receipts where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        assert cur.fetchone()[0] == 0


def byte_report(f, result):
    from scripts.ingestion.january_audit import common
    from scripts.ingestion.january_audit import media as media_audit
    from scripts.ingestion.january_audit.posts import digest

    failures, identities, _, target = audit(f, result)
    assert not failures
    with f.tx() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """select e.* from social.instagram_ingestion_receipts r
            join social.instagram_ingestion_evidence e using(evidence_id)
            where r.run_id=%s and r.stage='media' and r.owner_token=%s""",
            (f.owned["run_id"], f.owned["owner_token"]),
        )
        retained = cur.fetchone()
        cur.execute("select * from social.instagram_posts where id=%s", (f.initial.post_id,))
        post = cur.fetchone()
        cur.execute("select * from social.instagram_post_payloads where post_id=%s", (f.initial.post_id,))
        sidecar = cur.fetchone()
        cur.execute("select * from social.avatar_registry where account_handle=%s", (f.handle,))
        registry = cur.fetchall()
        cur.execute("select id from social.social_post_media_assets where post_id=%s", (f.initial.canonical_post_id,))
        asset_id = str(cur.fetchone()["id"])
    checked = []

    def check(good, code):
        if not good:
            checked.append(code)

    slots = media_audit.verify_avatars(
        post,
        {**post["raw_data"], **sidecar["raw_data"]},
        [],
        result.snapshot,
        result,
        registry,
        datetime.now(UTC),
        check,
    )
    assert not checked
    row = {**retained, **target}
    plan = media_audit.object_plan(row, result.snapshot, result, {}, slots, check)
    plan.extend(media_audit.saved_owner_byte_plans(plan, identities))
    assert not checked
    for item in plan:
        if not item["role"].startswith(("avatar:", "saved_owner_asset_source:", "comment:")):
            item["canonical_asset_id"] = asset_id
    reservation = {"owner_token": f.owned["owner_token"], "owner_generation": target["target_owner_generation"]}
    record = {
        "shortcode": f.initial.shortcode,
        "source_evidence_id": str(retained["evidence_id"]),
        "source_hash": retained["content_hash"],
        "source_evidence_parser": retained["parser_version"],
        "record_identity": {
            "legacy_post_id": f.initial.post_id,
            "canonical_post_id": f.initial.canonical_post_id,
            "media_reservation": reservation,
        },
        "original_observed_at": result.snapshot.observed_at.isoformat(),
        "comment_binding": {"post_id": f.initial.post_id, "media_reservation": reservation, "observed_comment_ids": []},
        "object_roles": len(plan),
        "object_role_sha256": {p["role"]: digest(p) for p in plan},
        "avatar_slots": slots,
        "saved_owner_asset_sources": identities,
    }
    report = {
        "schema_version": 2,
        "operator_scope": "media_records_and_byte_plan_not_final_certifier",
        "final_acceptance": False,
        "run_id": f.owned["run_id"],
        "intent_hash": "a" * 64,
        "full_history_gate_closed": True,
        "status": "passed",
        "verified_targets": 1,
        "audited_january_count": 1,
        "failures": [],
        "pending": [],
        "verified": [record],
        "byte_read_plan": {
            "status": "ready_for_verified_subset",
            "objects": plan,
            "physical_urls": len({p["url"] for p in plan}),
        },
    }
    context = common.AuditContext(
        f.owned["run_id"], "a" * 64, f.owned["account"], fixtures.OBSERVED, fixtures.OBSERVED + timedelta(days=31)
    )
    return report, context


def test_final_physical_plan_checks_old_and_current_objects_as_distinct_roles(snapshot_workflow_database, monkeypatch):
    import io
    from types import SimpleNamespace

    import requests

    from scripts.ingestion.january_audit import common, hosted

    f = setup(snapshot_workflow_database, monkeypatch)
    result = commit_prepared(f, recover(f))
    report, context = byte_report(f, result)
    with common.bound(context):
        plan = hosted.physical_plan(report, "https://media.example")
    selected = [p for p in plan if p["storage_key"] in {f.old_key, f.destination_key}]
    assert len(selected) == 2 and {p["storage_key"] for p in selected} == {f.old_key, f.destination_key}
    assert {r["role"].split(":")[0] for p in selected for r in p["roles"]} == {"avatar", "saved_owner_asset_source"}

    class ReadOnlyClient:
        def get_object(self, **kwargs):
            body = f.storage.client.objects[kwargs["Key"]][0]
            return {
                "Body": io.BytesIO(body),
                "ContentLength": len(body),
                "ContentType": "image/png",
                "ResponseMetadata": {"HTTPStatusCode": 200},
            }

    class PublicResponse(Response):
        def __init__(self, body):
            super().__init__(data=body, headers={"Content-Length": str(len(body))})
            self.raw = io.BytesIO(body)

        def __exit__(self, *_):
            self.raw.close()

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def get(self, url, **kwargs):
            assert kwargs["allow_redirects"] is False and kwargs["headers"] == {"Accept-Encoding": "identity"}
            return PublicResponse(f.storage.client.objects[urlsplit(url).path.lstrip("/")][0])

    monkeypatch.setattr(requests, "Session", Session)
    results = [hosted.inspect_object(p, ReadOnlyClient(), SimpleNamespace(bucket=f.storage.bucket)) for p in selected]
    assert all(r["status"] == "verified" for r in results)
    assert {r["storage_key"] for r in results} == {f.old_key, f.destination_key}
    # Independent final verification cannot accept destination success alone.
    f.storage.client.objects[f.old_key] = b"corrupt original", {"sha256": SHA}
    old = next(p for p in selected if p["storage_key"] == f.old_key)
    assert (
        hosted.inspect_object(old, ReadOnlyClient(), SimpleNamespace(bucket=f.storage.bucket))["status"] != "verified"
    )


def test_historical_byte_role_cannot_be_relabelled_or_omit_provenance(snapshot_workflow_database, monkeypatch):
    from copy import deepcopy

    from scripts.ingestion.january_audit import common, hosted
    from scripts.ingestion.january_audit.posts import digest

    f = setup(snapshot_workflow_database, monkeypatch)
    report, context = byte_report(f, commit_prepared(f, recover(f)))
    old = next(p for p in report["byte_read_plan"]["objects"] if p["role"].startswith("saved_owner_asset_source:"))
    for changed in ({"source_key": f.destination_key}, {"historical_handle": f.handle}, {"sha256": "f" * 64}):
        bad = deepcopy(report)
        item = next(p for p in bad["byte_read_plan"]["objects"] if p["role"] == old["role"])
        item["saved_owner_asset_source"].update(changed)
        bad["verified"][0]["object_role_sha256"][item["role"]] = digest(item)
        with (
            common.bound(context),
            pytest.raises(common.GuardError, match="saved_owner_asset_source_provenance_mismatch"),
        ):
            hosted.physical_plan(bad, "https://media.example")


@pytest.mark.parametrize("namespace", ["historical", "current"])
def test_legal_cohort_growth_needs_new_authorization_and_preserves_history(
    snapshot_workflow_database, monkeypatch, namespace
):
    f = setup(snapshot_workflow_database, monkeypatch)
    result = commit_prepared(f, recover(f))
    f.initial = f.loader.load(**f.binding)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
                hosted_url,status,last_checked_at)
            values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (
                f.historical if namespace == "historical" else f.handle,
                REGIONAL.replace("iad6", "ams1"),
                SHA,
                f.old_url if namespace == "historical" else f.destination_url,
                fixtures.BYTE_CLOCK,
            ),
        )
    failures, identities, _, _ = audit(f, result)
    assert "saved_owner_asset_current_authorization_missing" in failures and not identities
    fixtures.invalidate_and_reacquire(f)
    result = commit_prepared(f, recover(f))
    failures, identities, _, _ = audit(f, result)
    assert not failures and len(identities) == 1 and len(records(f)) == 2


@pytest.mark.parametrize("damage", ["hash", "current_seal", "input_schema", "owner", "future"])
def test_invalid_old_authorization_never_disappears_behind_fresh_proof(snapshot_workflow_database, monkeypatch, damage):
    f = setup(snapshot_workflow_database, monkeypatch)
    commit_prepared(f, recover(f))
    f.initial = f.loader.load(**f.binding)
    fixtures.change_unrelated_inventory(f)
    commit_prepared(f, recover(f))
    older = next(p for p in records(f).values() if p["auth"]["parent_id"])
    f.initial = f.loader.load(**f.binding)
    fixtures.change_unrelated_inventory(f)
    # Change a different native field so a new owner/input is required even if
    # the prior helper uses identical source content on this second invalidation.
    result = commit_prepared(f, recover(f))
    assert not audit(f, result)[0]
    with f.tx() as conn, conn.cursor() as cur:
        if damage == "hash":
            cur.execute(
                "update social.instagram_ingestion_evidence set content_hash=%s where evidence_id=%s",
                ("f" * 64, older["row"]["evidence_id"]),
            )
        elif damage == "current_seal":
            cur.execute(
                "update social.instagram_ingestion_evidence set payload=null where evidence_id=%s",
                (older["input"]["source_evidence_id"],),
            )
        elif damage == "input_schema":
            cur.execute(
                "update social.instagram_ingestion_evidence set payload='{}' where evidence_id=%s",
                (older["auth"]["input_id"],),
            )
        elif damage == "owner":
            cur.execute(
                "delete from social.instagram_ingestion_dispatches where owner_token=%s",
                (older["auth"]["owner_token"],),
            )
        else:
            from trr_backend.ingestion.evidence import Evidence

            p = dict(older["auth"])
            p["verified_at"] = saved.stamp(datetime.now(UTC) + timedelta(days=1))
            e = Evidence.capture(
                source_identity=older["row"]["source_identity"],
                captured_at=datetime.fromisoformat(p["verified_at"]),
                parser_version=saved.AUTH_VERSION,
                payload=p,
            )
            cur.execute(
                "update social.instagram_ingestion_evidence set captured_at=%s,expires_at=%s,content_hash=%s,pa"
                "yload=%s where evidence_id=%s",
                (e.captured_at, e.expires_at, e.content_hash, e.payload_json, older["row"]["evidence_id"]),
            )
    with pytest.raises((ValueError, TypeError, KeyError)):
        audit(f, result)


def test_current_image_and_native_id_namespaces_are_not_used_for_saved_asset_claim(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "update social.instagram_posts set raw_data=jsonb_set(raw_data,'{user}',%s::jsonb) where id=%s",
            (
                json.dumps(
                    {
                        "id": "11425198",
                        "pk": "11425198",
                        "username": f.handle,
                        "profile_pic_url": OLD.replace("717993431", "746862330"),
                    }
                ),
                f.initial.post_id,
            ),
        )
    f.initial = f.loader.load(**f.binding)
    calls = []

    def opener(session, url, **kwargs):
        calls.append(url)
        return f.opener(session, url, **kwargs)

    monkeypatch.setattr(saved, "safe_requests_get", opener)
    monkeypatch.setattr(cached, "safe_requests_get", opener)
    prepared = recover(f)
    assert all(url.startswith("https://media.example/") for url in calls)
    assert prepared.avatars[0].source_url == OLD and prepared.avatars[0].cached_sha256 == SHA
    p = next(iter(records(f).values()))
    assert "native_id" not in json.dumps(p["input"]) and "user_id" not in json.dumps(p["input"])


def test_corrupt_destination_is_not_overwritten_even_when_metadata_claims_expected_hash(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    f.storage.client.objects[f.destination_key] = b"wrong bytes", {"sha256": SHA}
    with pytest.raises(MediaIncomplete):
        recover(f)
    assert f.storage.client.objects[f.destination_key][0] == b"wrong bytes"
    assert f.storage.client.objects[f.old_key][0] == DATA
    assert not records(f)


@pytest.mark.parametrize("bad", ["redirect", "range", "encoding", "length", "truncation"])
def test_copy_body_is_complete_and_not_redirected_transformed_or_truncated(
    snapshot_workflow_database, monkeypatch, bad
):
    f = setup(snapshot_workflow_database, monkeypatch)
    seen = 0

    def opener(session, url, **kwargs):
        nonlocal seen
        if url == f.old_url:
            seen += 1
            if seen == 2:
                if bad == "redirect":
                    return Response(status=302)
                if bad == "range":
                    return Response(headers={"Content-Range": "bytes 0-1/9"})
                if bad == "encoding":
                    return Response(headers={"Content-Encoding": "gzip"})
                if bad == "length":
                    return Response(headers={"Content-Length": str(len(DATA) + 1)})
                return Response(data=DATA[:-1])
        return f.opener(session, url, **kwargs)

    monkeypatch.setattr(saved, "safe_requests_get", opener)
    monkeypatch.setattr(cached, "safe_requests_get", opener)
    with pytest.raises(MediaIncomplete):
        recover(f)
    assert f.destination_key not in f.storage.client.objects and not records(f)


def test_ordinary_cached_no_aux_keeps_large_cohort_fastpath_and_does_not_recover(
    snapshot_workflow_database, monkeypatch
):
    from psycopg2.extras import execute_values

    from trr_backend.ingestion.composition import _load_media

    f = setup(snapshot_workflow_database, monkeypatch, acquire=False)
    f.storage.client.objects[f.destination_key] = DATA, {"sha256": SHA}
    with f.tx() as conn, conn.cursor() as cur:
        rows = [("instagram", f.handle, OLD, SHA, f.destination_url, "mirrored", fixtures.BYTE_CLOCK)]
        rows += [
            (
                "instagram",
                f.handle,
                OLD.replace("lax3", "lax" + str(i + 10)),
                SHA,
                f.destination_url,
                "mirrored",
                fixtures.BYTE_CLOCK,
            )
            for i in range(cached.MAX_ROWS + 1)
        ]
        execute_values(
            cur,
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at) values %s""",
            rows,
        )
        cur.execute(
            """update social.instagram_posts set owner_profile_pic_url_hd=%s,
            raw_data=jsonb_set(raw_data,'{owner_detail,profile_pic_url_hd}',%s::jsonb) where id=%s""",
            (OLD, json.dumps(OLD.replace("717993431", "746862330")), f.initial.post_id),
        )
    f.storage.client.digest_object.reset_mock()
    snapshot_load = Mock(wraps=f.loader.load)
    f.loader.load = snapshot_load

    def load(**callbacks):
        return _load_media(f.loader, f.store, {}, f.owned, **callbacks)

    outcome = MediaActivity(f.store, storage=f.storage).execute(**f.owned, load=load)
    assert outcome["outcome"] == "committed"
    assert snapshot_load.call_count == 1
    f.storage.client.digest_object.assert_not_called()
    assert not records(f)


def test_crash_after_dedicated_input_seal_has_no_copy_or_promotion(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    retain = f.store.retain_evidence

    def stop(**kwargs):
        value = retain(**kwargs)
        if kwargs["evidence"].parser_version == saved.INPUT_VERSION:
            raise RuntimeError("after prewrite input seal")
        return value

    monkeypatch.setattr(f.store, "retain_evidence", stop)
    with pytest.raises(RuntimeError, match="input seal"):
        recover(f)
    assert not records(f) and f.destination_key not in f.storage.client.objects
    f.storage.client.digest_object.assert_not_called()
    monkeypatch.setattr(f.store, "retain_evidence", retain)
    assert not audit(f, commit_prepared(f, recover(f)))[0]


def test_body_cancellation_closes_response_and_owned_client_without_advancing(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    closed = []

    class Cancelled(Response):
        def iter_content(self, **kwargs):
            assert kwargs["chunk_size"] == 64 * 1024
            yield DATA[:3]
            raise LostOwnership("cancelled copy")

        def __exit__(self, *_):
            closed.append(True)

    monkeypatch.setattr(saved, "safe_requests_get", lambda *args, **kwargs: Cancelled())
    with pytest.raises(LostOwnership):
        recover(f)
    assert closed == [True]
    f.storage.client.close.assert_called_once()
    assert f.destination_key not in f.storage.client.objects and not records(f)


def test_deadline_before_bytes_does_not_copy_or_promote(snapshot_workflow_database, monkeypatch):
    import time

    from trr_backend.ingestion.media import MediaOwnershipCheck

    f = setup(snapshot_workflow_database, monkeypatch)
    f.callbacks["check_cancelled"] = MediaOwnershipCheck(lambda: None, deadline=time.monotonic() - 1)
    with pytest.raises(MediaIncomplete, match="target_deadline"):
        recover(f)
    assert f.destination_key not in f.storage.client.objects and not records(f)


def test_ambiguous_commit_reconciles_receipt_instead_of_reclaiming_completed_target(
    snapshot_workflow_database, monkeypatch
):
    f = setup(snapshot_workflow_database, monkeypatch)
    result = commit_prepared(f, recover(f))
    reservation_args = {k: v for k, v in f.owned.items() if k != "owner_token"}
    assert f.store.reserve(**reservation_args) is None
    assert not audit(f, result)[0]
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from social.instagram_ingestion_receipts where run_id=%s and stage='media'",
            (f.owned["run_id"],),
        )
        assert cur.fetchone()[0] == 1


def test_two_current_roles_partial_copy_retry_never_inherits_authorization(snapshot_workflow_database, monkeypatch):
    f = setup(snapshot_workflow_database, monkeypatch)
    commit_prepared(f, recover(f))
    f.initial = f.loader.load(**f.binding)
    fixtures.change_owner_role(f)
    copy = saved._copy
    seen = 0

    def second_fails(*args, **kwargs):
        nonlocal seen
        seen += 1
        if seen == 2:
            raise RuntimeError("second current role")
        return copy(*args, **kwargs)

    monkeypatch.setattr(saved, "_copy", second_fails)
    with pytest.raises(RuntimeError, match="second current role"):
        recover(f)
    assert len(records(f)) == 2
    monkeypatch.setattr(saved, "_copy", copy)
    result = commit_prepared(f, recover(f))
    assert not audit(f, result)[0]
    assert {p["input"]["slot"]["role"] for p in records(f).values() if p["auth"]["parent_id"]} == {"tagged", "mention"}


def test_ordinary_evidence_projection_does_not_accept_saved_asset_private_fields():
    from trr_backend.ingestion.evidence import sanitize_payload

    payload = {
        "username": "owner",
        "historical_handle": "old",
        "input_slot": {},
        "owner_generation": 8,
        "binding": {"hosted_owner_url": "https://media.example/private.jpg"},
    }
    assert sanitize_payload(payload, parser_version="instagram-media-v1") == {"username": "owner"}


@pytest.mark.parametrize("namespace", ["historical", "current"])
def test_complete_matching_cohort_does_not_silently_drop_unsupported_host_rows(
    snapshot_workflow_database, monkeypatch, namespace
):
    f = setup(snapshot_workflow_database, monkeypatch)
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at) values('instagram',%s,%s,%s,%s,'mirrored',%s)""",
            (
                f.historical if namespace == "historical" else f.handle,
                "https://foreign.example" + urlsplit(OLD).path,
                SHA,
                f.old_url if namespace == "historical" else f.destination_url,
                fixtures.BYTE_CLOCK,
            ),
        )
    with pytest.raises(MediaIncomplete, match="saved_owner_asset_cohort_reference_unsupported"):
        recover(f)
    assert f.destination_key not in f.storage.client.objects and not records(f)


@pytest.mark.parametrize("value", [11425198, True, None])
def test_raw_native_id_or_boolean_is_never_coerced_into_historical_handle(
    snapshot_workflow_database, monkeypatch, value
):
    f = setup(snapshot_workflow_database, monkeypatch)
    detail = {"username": value, "user_id": "11425198", "profile_pic_url": OLD}
    with f.tx() as conn, conn.cursor() as cur:
        for table, key in [("instagram_posts", "id"), ("instagram_post_payloads", "post_id")]:
            from psycopg2 import sql

            cur.execute(
                sql.SQL(
                    "update social.{} set raw_data=jsonb_set(raw_data,'{{owner_detail}}',%s::jsonb) where {}=%s"
                ).format(sql.Identifier(table), sql.Identifier(key)),
                (json.dumps(detail), f.initial.post_id),
            )
    f.initial = f.loader.load(**f.binding)
    returned = recover(f)
    assert returned.avatars[0].cached_hosted_url is None
    assert f.destination_key not in f.storage.client.objects and not records(f)
    f.storage.client.digest_object.assert_not_called()
