"""Ordinary cached roles cannot spend derived-alias inventory or byte budgets."""

import json
from unittest.mock import Mock

import pytest

from tests.ingestion import test_cached_avatar_alias as fixtures
from trr_backend.ingestion import cached_avatar_alias as alias
from trr_backend.ingestion.media import MediaActivity, MediaIncomplete
from trr_backend.ingestion.replay import MEDIA_RESULT_REPLAY_VERSION, _media

database = fixtures.database
media_database = fixtures.media_database
snapshot_database = fixtures.snapshot_database
snapshot_workflow_database = fixtures.snapshot_workflow_database
BRAVO = "https://scontent-mia5-2.cdninstagram.com/v/t51.82787-19/587397904_18544015456058415_848275806229035744_n.jpg"


def large_cohort(f, handle, reference):
    hosted = f.storage._url(alias.expected_key(handle, fixtures.SHA, reference))
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into social.avatar_registry(platform,account_handle,source_url,content_hash,
            hosted_url,status,last_checked_at)
            select 'instagram',%s,%s||case when i=0 then '' else '?copy='||i::text end,%s,%s,'mirrored',%s
            from generate_series(0,2048) i on conflict(platform,account_handle,source_url) do nothing""",
            (handle, reference, fixtures.SHA, hosted, fixtures.BYTE_CLOCK),
        )
    return hosted


def add_bravo(f, *, owner=False):
    large_cohort(f, "bravotv", BRAVO)
    person = {"username": "bravotv", "profile_pic_url": BRAVO}
    with f.tx() as conn, conn.cursor() as cur:
        if owner:
            cur.execute(
                "update social.instagram_posts set owner_username='bravotv',owner_profile_pic_url=%s,"
                "raw_data=jsonb_set(raw_data,'{owner}',%s::jsonb) where id=%s",
                (BRAVO, json.dumps(person), f.initial.post_id),
            )
            cur.execute(
                "update social.instagram_post_payloads set raw_data=jsonb_set(raw_data,'{owner}',%s::jsonb) "
                "where post_id=%s",
                (json.dumps(person), f.initial.post_id),
            )
        else:
            for table, key in [("instagram_posts", "id"), ("instagram_post_payloads", "post_id")]:
                cur.execute(
                    f"update social.{table} set raw_data=jsonb_set(raw_data,'{{tagged_users_detail}}',%s::jsonb) "
                    f"where {key}=%s",
                    (json.dumps([person]), f.initial.post_id),
                )
    f.initial = f.loader.load(**f.binding)
    bravo = next(a for a in f.initial.avatars if a.username == "bravotv")
    assert bravo.cached_hosted_url and bravo.cached_sha256 == fixtures.SHA


def test_ordinary_cached_bravo_2049_rows_has_no_cohort_or_byte_work(snapshot_workflow_database, monkeypatch):
    f = fixtures.prepare(snapshot_workflow_database, monkeypatch)
    add_bravo(f, owner=True)
    cohort = Mock(side_effect=AssertionError("ordinary cohort query forbidden"))
    bytes_ = Mock(side_effect=AssertionError("ordinary byte work forbidden"))
    factory = Mock(side_effect=AssertionError("ordinary storage construction forbidden"))
    monkeypatch.setattr(alias, "read_cohort", cohort)
    monkeypatch.setattr(alias, "verify_cached_bytes", bytes_)
    result = alias.bridge_cached_avatars(
        f.initial, loader=f.loader, store=f.store, owned=f.owned, storage_factory=factory, callbacks=f.callbacks
    )
    assert result == f.initial
    cohort.assert_not_called()
    bytes_.assert_not_called()
    factory.assert_not_called()


@pytest.mark.parametrize("derived", [False, True])
def test_uncached_and_auxiliary_derived_large_cohorts_still_fail_closed(
    snapshot_workflow_database, monkeypatch, derived
):
    f = fixtures.prepare(snapshot_workflow_database, monkeypatch)
    if derived:
        fixtures.bridge(f)
        f.initial = f.loader.load(**f.binding)
        assert f.initial.avatars[0].cached_hosted_url
        assert fixtures.proof_rows(f)
    else:
        assert not f.initial.avatars[0].cached_hosted_url
    large_cohort(f, f.handle, fixtures.REGIONAL)
    byte_work = Mock(side_effect=AssertionError("cap failure must precede bytes"))
    monkeypatch.setattr(alias, "verify_cached_bytes", byte_work)
    with pytest.raises(MediaIncomplete, match="cached_avatar_alias_inventory_limit"):
        fixtures.bridge(f)
    byte_work.assert_not_called()


def test_first_owner_promotion_then_large_ordinary_cache_commits_normal_receipt(
    snapshot_workflow_database, monkeypatch
):
    f = fixtures.prepare(snapshot_workflow_database, monkeypatch, acquire=False)
    add_bravo(f)
    original = alias.read_cohort
    queried = []

    def cohort(cur, username, reference, **kwargs):
        queried.append(username)
        assert username != "bravotv"
        return original(cur, username, reference, **kwargs)

    monkeypatch.setattr(alias, "read_cohort", cohort)

    def load(**callbacks):
        return alias.bridge_cached_avatars(
            f.loader.load(**f.binding, **callbacks),
            loader=f.loader,
            store=f.store,
            owned=f.owned,
            storage_factory=lambda: f.storage,
            callbacks=callbacks,
        )

    receipt = MediaActivity(f.store, storage=f.storage).execute(**f.owned, load=load)
    assert receipt["outcome"] == "committed" and f.handle in queried and "bravotv" not in queried
    with f.tx() as conn, conn.cursor() as cur:
        cur.execute("select * from social.instagram_ingestion_evidence where evidence_id=%s", (receipt["evidence_id"],))
        row = dict(zip([c.name for c in cur.description], cur.fetchone(), strict=True))
    result = _media(
        alias.evidence_from_row(row),
        target_key=f.initial.shortcode,
        parser_version=MEDIA_RESULT_REPLAY_VERSION,
        pinned=True,
    ).result
    assert result is not None
    assert {a["username"] for a in result.avatars} == {f.handle, "bravotv"}
    assert len(fixtures.proof_rows(f)) == 1
