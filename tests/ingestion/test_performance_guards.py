"""Short lossless snapshot, resume and pacing checks; no live services."""

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import psycopg2
import pytest

from tests.ingestion import test_comments as comments_fixture
from tests.ingestion import test_media as media_fixture
from trr_backend.ingestion.comment_transport import PublicRelayResume
from trr_backend.ingestion.comments import CommentActivity, CommentSourceError, _advance, _checkpoint_delta, _PageMap
from trr_backend.ingestion.media import CommentMedia, legacy_comment_source_key, mirror_snapshot
from trr_backend.socials.instagram.comments_scrapling import fetcher

database = comments_fixture.database


@pytest.mark.parametrize("failure", ["parser", "writer"])
def test_one_page_transaction_keeps_evidence_when_page_rolls_back(database, failure):
    store, transaction, args = comments_fixture.setup(database)
    if failure == "writer":
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("""create function social.fail_performance_page() returns trigger language plpgsql as
                $$begin raise exception 'injected canonical failure'; end$$;
                create trigger fail_performance_page before insert on social.instagram_comments
                for each row execute function social.fail_performance_page()""")
        payload = comments_fixture.NATIVE["rest"]
    else:
        payload = {"comments": [{"text": "missing native ID"}], "has_more_comments": False}
    try:
        with pytest.raises((CommentSourceError, psycopg2.Error)):
            CommentActivity(store, comments_fixture.source([payload]), clock=lambda: comments_fixture.OBSERVED).execute(
                **args
            )
        assert not comments_fixture.query(
            transaction, "select * from social.instagram_comments where post_id=%s", (args["post_id"],)
        )
        assert not comments_fixture.query(
            transaction, "select * from social.instagram_ingestion_comment_pages where run_id=%s", (args["run_id"],)
        )
        saved = comments_fixture.query(
            transaction, "select checkpoint from social.instagram_ingestion_targets where run_id=%s", (args["run_id"],)
        )
        assert saved[0]["checkpoint"] == {}
        evidence = comments_fixture.query(
            transaction,
            "select payload,content_hash from social.instagram_ingestion_evidence where run_id=%s",
            (args["run_id"],),
        )
        assert len(evidence) == 1 and evidence[0]["content_hash"]
    finally:
        if failure == "writer":
            with transaction() as conn, conn.cursor() as cur:
                cur.execute(
                    "drop trigger fail_performance_page on social.instagram_comments; "
                    "drop function social.fail_performance_page()"
                )


def test_incremental_checkpoint_delta_matches_full_snapshot_with_deletions():
    previous = {
        "comment_pages": {
            "observed": {"a": {"parent": None}, "b": {"parent": "a"}},
            "lanes": {"a": {"exhausted": False}},
            "version": 1,
        }
    }
    current = deepcopy(previous)
    for key in ("observed", "lanes"):
        current["comment_pages"][key] = _PageMap(previous["comment_pages"][key])
    current["comment_pages"]["observed"].pop("b")
    current["comment_pages"]["observed"]["c"] = {"parent": "a"}
    current["comment_pages"]["lanes"]["a"] = {"exhausted": True}
    assert _checkpoint_delta(previous, current) == _checkpoint_delta(
        previous,
        deepcopy(
            dict(
                current,
                comment_pages={
                    key: dict(value) if isinstance(value, _PageMap) else value
                    for key, value in current["comment_pages"].items()
                },
            )
        ),
    )
    assert "b" in previous["comment_pages"]["observed"]
    assert previous["comment_pages"]["lanes"]["a"]["exhausted"] is False


@pytest.mark.parametrize("complete", [False, True])
def test_embedded_connection_never_replaces_saved_child_query(complete):
    thread = {"count": 0 if complete else None, "edges": [], "page_info": {"has_next_page": False, "end_cursor": None}}
    page = comments_fixture.native_count_page({}, thread=thread)
    provenance = {**page.cursor_provenance, "request_cursor": "top-tail"}  # pyright: ignore[reportGeneralTypeIssues]
    binding = {key: provenance[key] for key in ("relay_doc_id", "relay_friendly_name", "sort_order", "page_size")}
    child = {
        **binding,
        "relay_doc_id": "child-doc",
        "started_from_root": True,
        "exhausted": False,
        "cursors": ["saved-child-tail"],
        "cursor_param": "after",
    }
    state = {
        "version": 1,
        "adapter_family": "instagram_public_relay",
        "adapter_version": 1,
        "observed": {"p": {"parent": None, "expected_replies": None}},
        "lanes": {"__top__": {**binding, "exhausted": False, "cursors": ["top-tail"]}, "p": child},
    }
    _advance(
        state,
        replace(page, started_from_root=False, cursor_provenance=provenance),
        scan_started_at=comments_fixture.OBSERVED,
    )
    assert state["lanes"]["p"] == child
    resume = PublicRelayResume.from_checkpoint({"comment_pages": state}, [SimpleNamespace(comment_id="p", replies=[])])
    assert resume.replies["p"]["pending"] is True
    assert resume.replies["p"]["after"] == "saved-child-tail"


def test_unknown_embedded_pagination_cannot_prove_exhaustion():
    page = comments_fixture.native_count_page(
        {}, thread={"count": None, "edges": [], "page_info": {"has_next_page": False}}
    )
    state = {"version": 1, "observed": {}, "lanes": {}, "expected": None}
    _advance(state, page, scan_started_at=comments_fixture.OBSERVED)
    assert state["lanes"]["p"]["exhausted"] is False
    resume = PublicRelayResume.from_checkpoint({"comment_pages": state}, [SimpleNamespace(comment_id="p", replies=[])])
    assert resume.replies["p"]["pending"] is True


@pytest.mark.parametrize("signed_variant", [False, True])
def test_attachment_reuse_preserves_all_slots_keys_and_clocks(signed_variant):
    url = "https://scontent.cdninstagram.com/attachment.png?signature=exact"
    other = url + "2" if signed_variant else url
    rows = [str(uuid4()), str(uuid4())]
    source = media_fixture.snapshot(
        comments=tuple(
            CommentMedia(str(index), row, (asset,), legacy_comment_source_key(row, (asset,)), media_fixture.OBSERVED)
            for index, (row, asset) in enumerate(zip(rows, (url, other), strict=True))
        )
    )
    downloads = []

    def opener(_client, asset, **_kwargs):
        downloads.append(asset)
        return media_fixture.Response()

    client = media_fixture.Storage()
    result = mirror_snapshot(
        source, media_fixture.storage(client, opener=opener), check=media_fixture.noop, request=media_fixture.noop
    )
    assert downloads.count(url) == 1
    assert len(downloads) == (3 if signed_variant else 2)  # one post source plus attachments
    outputs = [result.comment_fields[str(index)]["hosted_media_urls"][0] for index in range(2)]
    assert outputs[0] != outputs[1]
    assert all(any(output.endswith(key) for key in client.objects) for output in outputs)
    assert result.snapshot.comments == source.comments


def test_matched_attachment_download_reuse_keeps_exact_output(monkeypatch):
    import hashlib
    import json
    import subprocess
    import sys
    import types

    from trr_backend.ingestion import media

    baseline = types.ModuleType("trr_backend.ingestion._performance_media_baseline")
    baseline.__package__ = "trr_backend.ingestion"
    baseline.__file__ = media.__file__
    monkeypatch.setitem(sys.modules, baseline.__name__, baseline)
    code = subprocess.check_output(
        ["git", "show", "16b35037:trr_backend/ingestion/media.py"], cwd=comments_fixture.ROOT
    )
    exec(compile(code, baseline.__file__, "exec"), baseline.__dict__)
    url = "https://scontent.cdninstagram.com/duplicate.png?exact-signature"
    rows = [str(uuid4()), str(uuid4())]
    source = media_fixture.snapshot(
        comments=tuple(
            CommentMedia(str(index), row, (url,), legacy_comment_source_key(row, (url,)), media_fixture.OBSERVED)
            for index, row in enumerate(rows)
        )
    )
    proof, counts = None, {}
    for name, mirror in (("before", baseline.mirror_snapshot), ("after", mirror_snapshot)):
        downloads = []

        def opener(_client, asset, downloads=downloads, **_kwargs):
            downloads.append(asset)
            return media_fixture.Response()

        client = media_fixture.Storage()
        result = mirror(
            source, media_fixture.storage(client, opener=opener), check=media_fixture.noop, request=media_fixture.noop
        )
        inventory = (
            result.post_fields,
            result.comment_fields,
            {key: hashlib.sha256(value[0]).hexdigest() for key, value in client.objects.items()},
        )
        if proof is None:
            proof = inventory
        assert inventory == proof
        counts[name] = {
            "attachment_downloads": downloads.count(url),
            "total_downloads": len(downloads),
            "output_objects": len(client.objects),
            "comment_slots": len(result.comment_fields),
        }
    assert counts["before"]["attachment_downloads"] == 2
    assert counts["after"]["attachment_downloads"] == 1
    print(json.dumps({"matched_media_io": counts, "exact_output": True}))


@pytest.mark.parametrize("commit_seconds,cooldown", [(0.1, 0.0), (0.3, 0.4)])
def test_pacing_commit_consumes_reserved_wait_and_rechecks_cooldown(monkeypatch, commit_seconds, cooldown):
    from trr_backend.db import pg

    now, sleeps, active = [10.0], [], [False]

    class Cursor:
        def execute(self, *_args):
            pass

        def fetchone(self):
            return {"wait_seconds": 0.25}

    @contextmanager
    def connection(**_kwargs):
        active[0] = True
        yield object()
        now[0] += commit_seconds
        active[0] = False

    @contextmanager
    def cursor(**_kwargs):
        yield Cursor()

    def sleep(seconds):
        assert not active[0]
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(pg, "db_connection", connection)
    monkeypatch.setattr(pg, "db_cursor", cursor)
    monkeypatch.setattr(fetcher.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(fetcher.time, "sleep", sleep)
    monkeypatch.setattr(fetcher, "_wait_for_global_api_cooldown", lambda **_kwargs: True)
    monkeypatch.setattr(fetcher, "_persisted_global_api_cooldown_remaining", lambda _key: cooldown)
    result = fetcher._try_advisory_lock_pace(key="fixture", delay_seconds=0.25, deadline=20.0)
    assert result["paced"] is True
    expected = max(0.25, commit_seconds) + cooldown
    assert now[0] - 10 == pytest.approx(expected)
    assert sum(sleeps) == pytest.approx(max(0, 0.25 - commit_seconds) + cooldown)
