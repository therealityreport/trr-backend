"""Finite local subprocess and Modal routing checks; no provider/DB connections."""

import builtins
import importlib
import json
import os
import signal
import subprocess
import sys
from types import ModuleType, SimpleNamespace

import pytest

from trr_backend.ingestion import runtime

PUBLIC_DIAGNOSTIC = {
    "event": "instagram_public_request_failed",
    "error_code": "forbidden",
    "status": 403,
    "method": "GET",
    "endpoint_category": "profile_bootstrap",
}


def test_disabled_runtime_imports_no_engine_composition_or_connection(monkeypatch):
    monkeypatch.delenv(runtime.COORDINATOR_ENABLED, raising=False)
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name in {
            "dbos",
            "psycopg",
            "psycopg2",
            "trr_backend.ingestion.composition",
            "trr_backend.ingestion.workflows",
        }:
            pytest.fail(f"disabled runtime imported {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    assert runtime.run_ingestion_coordinator() == {"status": "disabled"}
    assert runtime.main() == 0


def test_enabled_unconfigured_returns_no_secret_and_does_not_spawn(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")

    def missing():
        raise ValueError("postgresql://private:credential@private-db")

    monkeypatch.setattr(runtime, "_configured_runtime", missing)
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *_a, **_k: pytest.fail("unconfigured spawn"))
    assert runtime.run_ingestion_coordinator() == {"status": "unconfigured", "failure_class": "ValueError"}


@pytest.mark.parametrize(
    "code,status", [(0, "completed"), (124, "bounded_timeout"), (125, "lease_lost"), (1, "failed")]
)
def test_child_exit_and_private_session_reaped(monkeypatch, code, status):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    monkeypatch.setattr(runtime, "_configured_runtime", lambda: object())
    actual_popen = subprocess.Popen
    children = []

    def spawn(argv, **kwargs):
        assert argv == [sys.executable, "-m", "trr_backend.ingestion.runtime"]
        assert kwargs["start_new_session"] is True
        assert kwargs["stdout"] == subprocess.DEVNULL and kwargs["stderr"] == subprocess.DEVNULL
        child = actual_popen([sys.executable, "-c", f"raise SystemExit({code})"], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    assert runtime.run_ingestion_coordinator() == {"status": status, "exit_code": code}
    assert children[0].poll() == code


def test_timeout_kills_and_reaps_private_coordinator(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "1")
    monkeypatch.setattr(runtime, "_configured_runtime", lambda: object())
    monkeypatch.setattr(runtime, "COORDINATOR_TIMEOUT_SECONDS", 0.1)
    actual_popen = subprocess.Popen
    children = []

    def spawn(_argv, **kwargs):
        child = actual_popen([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        assert os.getpgid(child.pid) == child.pid
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    assert runtime.run_ingestion_coordinator() == {"status": "timed_out"}
    assert children[0].poll() == -signal.SIGKILL
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


def test_interrupted_maintenance_reaps_child_before_propagating(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    monkeypatch.setattr(runtime, "_configured_runtime", lambda: object())

    class InterruptedChild:
        pid = 123456789
        waited = 0

        def wait(self, timeout):
            self.waited += 1
            if self.waited == 1:
                raise KeyboardInterrupt
            return -9

    child = InterruptedChild()
    killed = []
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *_a, **_k: child)
    monkeypatch.setattr(runtime.os, "killpg", lambda *args: killed.append(args))
    with pytest.raises(KeyboardInterrupt):
        runtime.run_ingestion_coordinator()
    assert killed == [(child.pid, signal.SIGKILL)] and child.waited == 2


def test_main_builds_existing_adapter_and_session_preserving_lease(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    config = SimpleNamespace(
        system_database_url="postgresql://synthetic-local", workflow_version="v1", runtime_role="runtime"
    )
    scheduler = object()
    composition = ModuleType("trr_backend.ingestion.composition")
    composition.runtime_config = lambda: config
    composition.build_scheduler = lambda: scheduler
    psycopg = ModuleType("psycopg")
    calls = []
    lease = object()
    psycopg.connect = lambda *args, **kwargs: calls.append((args, kwargs)) or lease
    workflows = ModuleType("trr_backend.ingestion.workflows")

    class Adapter:
        def __init__(self, actual_scheduler, actual_config):
            assert actual_scheduler is scheduler and actual_config is config

        def run_bounded(self, connect):
            assert connect() is lease

    workflows.DBOSAdapter = Adapter
    monkeypatch.setitem(sys.modules, composition.__name__, composition)
    monkeypatch.setitem(sys.modules, "psycopg", psycopg)
    monkeypatch.setitem(sys.modules, workflows.__name__, workflows)
    assert runtime.main() == 0
    assert calls == [(("postgresql://synthetic-local",), {"autocommit": True, "connect_timeout": 5})]


@pytest.fixture
def modal_jobs_stub(monkeypatch):
    from trr_backend import modal_jobs

    previous = sys.modules.get("modal")
    monkeypatch.setitem(sys.modules, "modal", modal_jobs._build_modal_stub_module())
    module = importlib.reload(modal_jobs)
    try:
        yield module
    finally:
        if previous is None:
            sys.modules.pop("modal", None)
        else:
            sys.modules["modal"] = previous
        importlib.reload(modal_jobs)


def test_existing_modal_lanes_route_ingestion_before_legacy_and_preserve_caps(monkeypatch, modal_jobs_stub):
    module = modal_jobs_stub
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    received = []
    monkeypatch.setattr(
        runtime, "run_ingestion_activity", lambda value: received.append(value) or {"outcome": "committed"}
    )
    monkeypatch.setattr(module, "_execute_social_job", lambda *_a, **_k: pytest.fail("new path imported legacy"))
    closed = []
    monkeypatch.setattr(module, "_close_db_pools_after_worker", lambda family: closed.append(family))
    for name, stage, cap in [
        ("run_social_posts_job", "listing", module._SOCIAL_CONCURRENCY_LIMIT),
        ("run_social_posts_job", "post_details", module._SOCIAL_CONCURRENCY_LIMIT),
        ("run_social_comments_job", "comments", module._SOCIAL_COMMENTS_CONCURRENCY_LIMIT),
        ("run_social_media_job", "media", module._SOCIAL_MEDIA_CONCURRENCY_LIMIT),
    ]:
        function = getattr(module, name)
        options = function._modal_function_options
        assert options["image"] is module._browser_image
        assert options["secrets"] == module._secrets
        assert options["retries"] == 0 and options["timeout"] == 7200 and options["max_containers"] == cap
        reservation = {
            "run_id": "run",
            "account": "account",
            "generation": 1,
            "stage": stage,
            "target_key": "post",
            "owner_token": "private-owner",
            "cookies": "must-not-forward",
        }
        assert function.get_raw_f()(ingestion_reservation=reservation) == {"outcome": "committed"}
        assert received[-1] == {k: v for k, v in reservation.items() if k != "cookies"}
    assert closed == ["instagram_ingestion"] * 4
    assert not hasattr(module, "instagram_ingestion_activity")
    with pytest.raises(ValueError, match="lane"):
        module.run_social_media_job.get_raw_f()(ingestion_reservation=dict(received[0], stage="comments"))
    with pytest.raises(ValueError, match="mutually exclusive"):
        module.run_social_posts_job.get_raw_f()("legacy", ingestion_reservation=received[0])
    legacy = []
    monkeypatch.setattr(
        module, "_execute_social_job", lambda *args, **kwargs: legacy.append((args, kwargs)) or {"status": "legacy"}
    )
    assert module.run_social_posts_job.get_raw_f()("old-post", "old-dispatch") == {"status": "legacy"}
    assert module.run_social_comments_job.get_raw_f()("old-comments") == {"status": "legacy"}
    assert module.run_social_media_job.get_raw_f()("old-media") == {"status": "legacy"}
    assert legacy == [
        (("old-post",), {"worker_prefix": "modal:social-posts", "dispatch_token": "old-dispatch"}),
        (("old-comments",), {"worker_prefix": "modal:social-comments"}),
        (("old-media",), {"worker_prefix": "modal:social-media"}),
    ]


def test_activity_subprocess_receives_stdin_only_and_projects_safe_summary(monkeypatch):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    actual_popen = subprocess.Popen
    children = []

    def spawn(argv, **kwargs):
        assert argv == [sys.executable, "-m", "trr_backend.ingestion.worker"]
        assert kwargs["stdin"] == subprocess.PIPE and kwargs["stderr"] == subprocess.DEVNULL
        script = """import json,sys
reservation=json.load(sys.stdin)
assert reservation['owner_token']=='private-owner'
print(json.dumps({'outcome':'committed','stage':'comments','owner_token':'must-not-return','payload':{'secret':'drop'}}))
"""
        child = actual_popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    reservation = {
        "run_id": "run",
        "account": "account",
        "generation": 1,
        "stage": "comments",
        "target_key": "post",
        "owner_token": "private-owner",
    }
    assert runtime.run_ingestion_activity(reservation) == {"outcome": "committed", "stage": "comments"}
    assert children[0].poll() == 0


@pytest.mark.parametrize("with_diagnostic", [True, False])
def test_public_diagnostic_crosses_real_transport_worker_main_and_runtime(monkeypatch, caplog, with_diagnostic):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    actual_popen = subprocess.Popen
    script = """
import sys,time,requests
from types import SimpleNamespace
from trr_backend.db import pg
from trr_backend.ingestion import worker
from trr_backend.ingestion.domain import Failure
from trr_backend.ingestion.public_diagnostics import record_public_diagnostic,current_public_diagnostic
from trr_backend.ingestion.public_transport import ObservedPublicSession,_CallbackAbort
pg.close_pool=lambda:None
record_public_diagnostic(DIAGNOSTIC)
def execute(reservation,**kwargs):
    sys.stderr.write("private-stderr-sentinel")
    if not ENABLED:
        return {"outcome":"committed","stage":reservation["stage"],"payload":"private-output-sentinel"}
    sent=[]
    class Gate:
        def before_request(self):return 1
        def record_failure(self,*args,**kwargs):return Failure("public_access_backoff")
    class Session:
        def __init__(self):
            self.cookies=requests.cookies.RequestsCookieJar()
            self.headers,self.adapters,self.proxies={},{},{}
        def mount(self,key,value):self.adapters[key]=value
        def request(self,*args,**kwargs):
            sent.append(1)
            response=requests.Response()
            response.status_code=403
            response._content=b"private-body-sentinel"
            response.headers.update({"content-type":"text/html","set-cookie":"private-cookie-sentinel"})
            return response
        def close(self):pass
    counts=[]
    session=ObservedPublicSession(session=Session(),gate=Gate(),deadline=time.monotonic()+30,
        check_cancelled=lambda:None,on_request=lambda:counts.append(1),
        on_payload=lambda _:None,on_failure=lambda _:None)
    try:
        session.get("https://www.instagram.com/private-account-sentinel/?token=private-query-sentinel")
    except _CallbackAbort as error:
        assert len(sent)==len(counts)==1
        return {"outcome":"unresolved","stage":reservation["stage"],"code":error.error.failure.code}
    finally:session.close()
    raise AssertionError("synthetic403 did not fail")
worker.execute_reservation=execute
code=worker.main()
assert current_public_diagnostic() is None
raise SystemExit(code)
""".replace("DIAGNOSTIC", repr(dict(PUBLIC_DIAGNOSTIC, error_code="rate_limited"))).replace(
        "ENABLED", repr(with_diagnostic)
    )

    def spawn(argv, **kwargs):
        assert argv == [sys.executable, "-m", "trr_backend.ingestion.worker"]
        assert kwargs["stderr"] == subprocess.DEVNULL and kwargs["stdout"] == subprocess.PIPE
        return actual_popen([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    reservation = {
        "run_id": "run",
        "account": "account",
        "generation": 1,
        "stage": "listing",
        "target_key": "page:root",
        "owner_token": "private-owner-sentinel",
    }
    expected = (
        {
            "outcome": "unresolved",
            "stage": "listing",
            "code": "public_access_backoff",
            "public_diagnostic": PUBLIC_DIAGNOSTIC,
        }
        if with_diagnostic
        else {"outcome": "committed", "stage": "listing"}
    )
    assert runtime.run_ingestion_activity(reservation) == expected
    records = [record for record in caplog.records if record.name == "trr_backend.ingestion.runtime"]
    assert [json.loads(record.getMessage()) for record in records] == ([PUBLIC_DIAGNOSTIC] if with_diagnostic else [])
    assert "sentinel" not in caplog.text


@pytest.mark.parametrize(
    "bad",
    [
        dict(PUBLIC_DIAGNOSTIC, url="https://private-url-sentinel"),
        dict(PUBLIC_DIAGNOSTIC, status=True),
        dict(PUBLIC_DIAGNOSTIC, error_code="private-error-sentinel"),
    ],
)
def test_runtime_revalidates_and_discards_malicious_child_diagnostic(monkeypatch, caplog, bad):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    actual_popen = subprocess.Popen
    output = {"outcome": "committed", "stage": "listing", "public_diagnostic": bad}
    script = "import json,sys;json.load(sys.stdin);print(json.dumps(" + repr(output) + "))"
    monkeypatch.setattr(
        runtime.subprocess, "Popen", lambda _argv, **kw: actual_popen([sys.executable, "-c", script], **kw)
    )
    reservation = {
        "run_id": "run",
        "account": "account",
        "generation": 1,
        "stage": "listing",
        "target_key": "page:root",
        "owner_token": "private-owner-sentinel",
    }
    assert runtime.run_ingestion_activity(reservation) == {"outcome": "committed", "stage": "listing"}
    assert "sentinel" not in caplog.text
    assert not [record for record in caplog.records if record.name == "trr_backend.ingestion.runtime"]


@pytest.mark.parametrize(
    "script,error_code",
    [
        ("import time; time.sleep(30)", "activity_timeout"),
        ("import sys; sys.stdout.write('x'*70000); sys.stdout.flush()", "activity_output_limit"),
    ],
)
def test_activity_timeout_or_output_overflow_kills_and_reaps(monkeypatch, script, error_code):
    monkeypatch.setenv(runtime.COORDINATOR_ENABLED, "true")
    monkeypatch.setattr(runtime, "ACTIVITY_TIMEOUT_SECONDS", 0.2)
    actual_popen = subprocess.Popen
    children = []

    def spawn(_argv, **kwargs):
        child = actual_popen([sys.executable, "-c", script], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    reservation = {
        "run_id": "run",
        "account": "account",
        "generation": 1,
        "stage": "comments",
        "target_key": "post",
        "owner_token": "private-owner",
    }
    assert runtime.run_ingestion_activity(reservation) == {
        "outcome": "unresolved",
        "stage": "comments",
        "code": error_code,
    }
    assert children[0].poll() is not None
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


@pytest.mark.parametrize(
    "failure_point,coordinator_status",
    [
        (None, "completed"),
        (None, "disabled"),
        (None, "error"),
        ("import", "error"),
        ("bootstrap", "error"),
        ("recovery", "error"),
    ],
)
def test_existing_sweep_recovers_before_coordinator_and_preserves_legacy_outcome(
    monkeypatch, modal_jobs_stub, failure_point, coordinator_status
):
    module = modal_jobs_stub
    events = []
    monkeypatch.setattr(module, "_validate_modal_maintenance_owner_config", lambda: None)
    monkeypatch.setattr(module, "_worker_started", lambda *a, **kw: events.append("started") or 0)
    monkeypatch.setattr(module, "_worker_finished", lambda *a, **kw: events.append("finished"))
    monkeypatch.setattr(module, "_worker_failed", lambda *a, **kw: events.append("failed"))
    monkeypatch.setattr(module, "_close_db_pools_after_worker", lambda family: events.append("cleanup"))
    monkeypatch.setattr(
        module, "_recover_stale_pending_social_catalog_launches", lambda: events.append("pending") or {"recovered": 0}
    )

    def wake():
        events.append("coordinator")
        if coordinator_status == "error":
            raise ValueError("coordinator unavailable")
        return {"status": coordinator_status}

    monkeypatch.setattr(runtime, "run_ingestion_coordinator", wake)

    def bootstrap(provider):
        events.append("bootstrap")
        if failure_point == "bootstrap":
            raise RuntimeError("legacy bootstrap unavailable")

    def recover():
        events.append("recovery")
        if failure_point == "recovery":
            raise RuntimeError("legacy recovery unavailable")
        return {"status": "completed", "dispatched": 1}

    real_import = builtins.__import__

    def legacy_import(name, *args, **kwargs):
        if name == "trr_backend.socials":
            events.append("import")
            if failure_point == "import":
                raise RuntimeError("legacy import unavailable")
            return SimpleNamespace(social_season_analytics_impl=ModuleType("synthetic_social_provider"))
        if name == "trr_backend.socials.control_plane_bootstrap":
            return SimpleNamespace(register_social_control_plane_providers=bootstrap)
        if name == "trr_backend.socials.control_plane":
            return SimpleNamespace(recover_and_dispatch_due_social_jobs=recover)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", legacy_import)
    if failure_point:
        with pytest.raises(RuntimeError, match=f"legacy {failure_point} unavailable"):
            module.sweep_social_dispatch_queue.get_raw_f()()
    else:
        result = module.sweep_social_dispatch_queue.get_raw_f()()
        assert result["status"] == "completed" and result["dispatched"] == 1
        if coordinator_status == "disabled":
            assert "instagram_ingestion_coordinator" not in result
        else:
            assert result["instagram_ingestion_coordinator"] == (
                {"status": "error", "failure_class": "ValueError"}
                if coordinator_status == "error"
                else {"status": "completed"}
            )
        assert events.index("recovery") < events.index("pending") < events.index("finished") < events.index("cleanup")
    assert events[-2:] == ["cleanup", "coordinator"]
