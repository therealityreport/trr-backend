"""One inherited absolute budget includes process startup and resume work."""

import json
import os
import subprocess
import sys
import time

import pytest

from tests.ingestion.test_comments import database as database_fixture
from trr_backend.ingestion import runtime

database = database_fixture

RESERVATION = {
    "run_id": "synthetic",
    "account": "bravotv",
    "generation": 1,
    "stage": "comments",
    "target_key": "fixture",
    "owner_token": "synthetic-owner",
}


def test_runtime_worker_entrypoint_counts_startup_resume_and_emits_receipt_before_hard_cap(monkeypatch, tmp_path):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CONCURRENCY", "2")
    monkeypatch.setenv("TRR_TEST_DEADLINE_PROOF", str(tmp_path / "proof.json"))
    monkeypatch.setenv("TRR_INSTAGRAM_REPLY_TELEMETRY_DIR", str(tmp_path / "timings"))
    for key in tuple(os.environ):
        if key.startswith("SOCIAL_INSTAGRAM_PUBLIC_REPLY_CANARY_"):
            monkeypatch.delenv(key)
    actual_popen = subprocess.Popen
    children = []
    script = r"""
import asyncio,json,os,time
from datetime import UTC,datetime,timedelta
from pathlib import Path
import httpx
from tests.ingestion import test_public_comment_transport as public
from tests.ingestion.test_parallel_reply_context import Gate
from trr_backend.db import pg
from trr_backend.ingestion import worker
from trr_backend.ingestion.comment_transport import PublicRelayCommentSource
from trr_backend.ingestion.composition import _GatedSource
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.post_transport import SourceFailure
from trr_backend.ingestion.reply_context import execution_deadline,source_deadline
from trr_backend.ingestion.runtime import ACTIVITY_DEADLINE_ENV
from trr_backend.socials.instagram.comments_scrapling import reply_http
hard=float(os.environ[ACTIVITY_DEADLINE_ENV])
proof={'hard':hard,'entry_before_startup':time.monotonic()}
real_client=httpx.AsyncClient
spools=[]
original_spool=reply_http.DiskResponseSpool
original_run=asyncio.run
started=[]
cancelled=[]
observed=[]
closed=[]
def spool():
    value=original_spool();spools.append(value);return value
reply_http.DiskResponseSpool=spool
def run(coro):
    async def checked():
        try:return await coro
        finally:
            assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()]
    return original_run(checked())
asyncio.run=run
async def handler(request):
    if request.method=='GET':return public.bootstrap()
    _,variables=public.body(request)
    parent=variables.get('parent_comment_id')
    if parent is None:
        return httpx.Response(200,json=public.page([public.actual_parent_shape(p) for p in ('p1','p2')]))
    started.append(parent)
    if parent=='p2':return httpx.Response(200,json=public.page([public.node('received-peer')],child=True))
    try:await asyncio.sleep(60)
    except asyncio.CancelledError:cancelled.append(parent);raise
    raise AssertionError('request survived owner budget')
def client(*args,**kwargs):
    kwargs['transport']=httpx.MockTransport(handler)
    return real_client(*args,**kwargs)
public.maintained.httpx.AsyncClient=client
def factory(**kwargs):
    value=public.maintained.InstagramCommentsScraplingFetcher(**kwargs)
    value._api_delay_seconds=value._global_api_delay_seconds=0
    return value
def execute(reservation,*,deadline_callback,**kwargs):
    # Context loading starts after process/import startup, but must not reset it.
    proof['before_owner_callback']=time.monotonic()
    deadline_callback(datetime.now(UTC)+timedelta(seconds=600))
    assert execution_deadline.get()==hard
    time.sleep(.25) # retained-inventory resume work
    proof['after_resume']=time.monotonic()
    soft=source_deadline(time.monotonic()+590)
    proof['soft']=soft
    assert soft==hard-20 and soft>time.monotonic()
    gate=Gate();gate.deadline=soft
    def finish(code=None,**kw):
        gate.failures.append((code,kw));return Failure(code) if code else None
    gate.finish_failure=finish
    fetch=PublicRelayCommentSource(fetcher_factory=factory)
    try:
        _GatedSource(fetch,gate,failure_code='comment_fetch_failed')(
            shortcode='fixture',checkpoint={},pre_source_request=lambda:None,
            source_observer=lambda kind,payload:observed.append(kind))
    except SourceFailure as error:
        assert error.failure.code=='target_deadline'
    else:raise AssertionError('expected source time cap')
    proof.update(finished=time.monotonic(),started=started,cancelled=cancelled,observed=observed,
                 spools_closed=all(value.closed for value in spools),health_codes=[code for code,_ in gate.failures])
    assert proof['spools_closed'] and set(started)=={'p1','p2'} and cancelled==['p1']
    assert 'graphql_replies' in observed and not any(proof['health_codes'])
    assert list(Path(os.environ['TRR_INSTAGRAM_REPLY_TELEMETRY_DIR']).glob('*.json'))
    return {'outcome':'unresolved','stage':'comments','code':'target_deadline'}
worker.execute_reservation=execute
pg.close_pool=lambda:closed.append(True)
time.sleep(.2) # startup delay before worker.main reads its inherited deadline
status=worker.main(execution_placement='local',expected_parent_pid=os.getppid())
assert execution_deadline.get() is None and closed==[True]
proof['worker_finished']=time.monotonic()
proof['pool_closed']=True
Path(os.environ['TRR_TEST_DEADLINE_PROOF']).write_text(json.dumps(proof))
raise SystemExit(status)
"""

    def spawn(argv, **kwargs):
        assert argv[:3] == [sys.executable, "-m", "trr_backend.ingestion.worker"]
        inherited = float(kwargs["env"][runtime.ACTIVITY_DEADLINE_ENV])
        assert 22 < inherited - time.monotonic() <= 23
        child = actual_popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    started = time.monotonic()
    assert runtime.run_ingestion_activity(RESERVATION, execution_placement="local", timeout_seconds=23) == {
        "outcome": "unresolved",
        "stage": "comments",
        "code": "target_deadline",
    }
    elapsed = time.monotonic() - started
    proof = json.loads((tmp_path / "proof.json").read_text())
    assert 2.5 < elapsed < 8
    assert proof["after_resume"] > proof["before_owner_callback"] + 0.2
    assert proof["soft"] == proof["hard"] - 20
    assert proof["soft"] <= proof["finished"] < proof["worker_finished"] < proof["hard"] - 18
    assert proof["pool_closed"] is proof["spools_closed"] is True
    assert children[0].poll() == 0
    receipts = [json.loads(path.read_text()) for path in (tmp_path / "timings").glob("*.json")]
    assert len(receipts) == 1 and receipts[0]["metrics"]["committed_pages"] == 1


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1", "invalid", "future"])
def test_worker_rejects_invalid_inherited_absolute_deadline(monkeypatch, capsys, value):
    from trr_backend.db import pg
    from trr_backend.ingestion import worker
    from trr_backend.ingestion.reply_context import execution_deadline

    monkeypatch.setenv(runtime.ACTIVITY_DEADLINE_ENV, str(time.monotonic() + 591) if value == "future" else value)
    monkeypatch.setattr(worker, "execute_reservation", lambda *args, **kwargs: pytest.fail("invalid deadline executed"))
    monkeypatch.setattr(pg, "close_pool", lambda: None)
    assert worker.main() == 1
    assert execution_deadline.get() is None
    assert json.loads(capsys.readouterr().out)["code"] == "worker_failed"


def test_source_deadline_keeps_direct_call_behavior_and_never_extends_inherited_budget():
    from trr_backend.ingestion.reply_context import execution_deadline, source_deadline

    assert source_deadline(1000) == 1000 and source_deadline(None) is None
    token = execution_deadline.set(590)
    try:
        assert source_deadline(1000) == 570
        assert source_deadline(400) == 400
        assert source_deadline(None) == 570
    finally:
        execution_deadline.reset(token)


@pytest.mark.parametrize("resume_seconds,expected_source", [(5, True), (12, False)])
def test_comment_activity_resume_consumes_existing_absolute_soft_budget(
    database, monkeypatch, resume_seconds, expected_source
):
    from tests.ingestion import test_comments as existing
    from trr_backend.ingestion.comments import CommentActivity, CommentFetchSummary
    from trr_backend.ingestion.post_transport import SourceFailure
    from trr_backend.ingestion.reply_context import execution_deadline

    store, _, args = existing.setup(database)
    clock = [100.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    activity = CommentActivity(store, clock=lambda: existing.OBSERVED)
    resume = activity._resume_inventory
    called = []

    def slow_resume(*values):
        result = resume(*values)
        clock[0] += resume_seconds
        return result

    def source(**callbacks):
        called.append(True)
        assert callbacks["pre_source_request"].deadline == 110
        assert callbacks["pre_source_request"].deadline - time.monotonic() == 5
        return CommentFetchSummary(fetch_failed=True)

    activity._resume_inventory = slow_resume
    token = execution_deadline.set(130.0)
    try:
        if expected_source:
            assert activity.execute(**args, fetch=source)["outcome"] == "unresolved"
        else:
            with pytest.raises(SourceFailure) as caught:
                activity.execute(**args, fetch=source)
            assert caught.value.failure.code == "target_deadline"
    finally:
        execution_deadline.reset(token)
    assert bool(called) is expected_source


def test_parent_hard_cap_includes_popen_delay_and_reaps_child(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    actual_popen = subprocess.Popen
    children = []

    def delayed_spawn(argv, **kwargs):
        inherited = float(kwargs["env"][runtime.ACTIVITY_DEADLINE_ENV])
        time.sleep(0.15)
        assert inherited - time.monotonic() < 0.11
        child = actual_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", delayed_spawn)
    started = time.monotonic()
    assert runtime.run_ingestion_activity(RESERVATION, execution_placement="local", timeout_seconds=0.25) == {
        "outcome": "unresolved",
        "stage": "comments",
        "code": "activity_timeout",
    }
    assert 0.23 < time.monotonic() - started < 0.36
    assert children[0].poll() is not None


@pytest.mark.parametrize("builder", ["public_parts", "comments_gate", "profile", "media_avatar"])
def test_public_constructor_deadlines_share_worker_soft_cap(monkeypatch, builder):
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from trr_backend.ingestion import avatar_resolution, avatar_transport, composition, identity_policy
    from trr_backend.ingestion.media_snapshot import AvatarSourcesMissing
    from trr_backend.ingestion.reply_context import execution_deadline

    gates, sources = [], []
    store = SimpleNamespace(transaction=lambda: None, check_owner=lambda **kwargs: None)
    monkeypatch.setattr(composition, "_worker_store", lambda context: store)
    monkeypatch.setattr(composition, "_source_profile", lambda context: SimpleNamespace(access_mode="public"))

    def gate(**kwargs):
        gates.append(kwargs)
        return SimpleNamespace(**kwargs)

    def avatar(**kwargs):
        sources.append(kwargs)
        return SimpleNamespace(**kwargs, close=lambda: None)

    monkeypatch.setattr(identity_policy, "SourceRequestGate", gate)
    monkeypatch.setattr(avatar_transport, "AvatarProfileSource", avatar)
    monkeypatch.setattr(
        avatar_resolution,
        "AvatarResolutionActivity",
        lambda *args, **kwargs: SimpleNamespace(execute=lambda **values: None),
    )
    context = {"target": {"owner_expires_at": datetime.now(UTC) + timedelta(seconds=600), "checkpoint": {}}}
    owned = dict(RESERVATION)
    hard = time.monotonic() + 100
    token = execution_deadline.set(hard)
    try:
        if builder == "public_parts":
            assert composition._public_source_parts(context, owned)["deadline"] == hard - 20
        elif builder == "comments_gate":
            result = composition._gate_source(
                None, context, owned, {"raw_cookies": {}}, failure_code="comment_fetch_failed", public_only=True
            )
            assert result.gate.deadline == hard - 20
        elif builder == "profile":
            source, _ = composition._profile_provider(context, owned)
            assert source.deadline == hard - 20
        else:
            calls = []

            def load(**kwargs):
                calls.append(True)
                if len(calls) == 1:
                    raise AvatarSourcesMissing(["fixture"])
                return "ready"

            assert composition._load_media(SimpleNamespace(load=load), store, context, owned) == "ready"
    finally:
        execution_deadline.reset(token)
    assert len(gates) == 1 and gates[0]["deadline"] == hard - 20
    assert all(source["deadline"] == hard - 20 for source in sources)
