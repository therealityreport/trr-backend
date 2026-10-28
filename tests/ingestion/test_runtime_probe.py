"""Fail-closed local fixtures only; never resolves real deployment credentials."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ingestion import probe_deployed_runtime as probe


@pytest.fixture
def metadata(tmp_path):
    hashes = {}
    for relative in probe.REQUIRED_HASHES:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic approved source\n")
        hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    expected = {
        "project_ref": probe.PROJECT_REF,
        "target": probe.TARGET.copy(),
        "runtime_role": "trr_ingestion_runtime",
        "workflow_version": "instagram-ingestion-v1",
        "executor_id": "instagram-coordinator-slot-0",
        "lock_namespace": 48112,
        "lock_key": 20261016,
        "lifetime_seconds": 90,
        "connection_limit": 4,
        "private_ssl_policy": probe.PRIVATE_SSL_POLICY,
        "source_hashes": hashes,
    }
    env = {**probe.TARGET, **dict.fromkeys(probe.OFF_FLAGS, "0"), "TRR_INSTAGRAM_INGESTION_SCHEMA_READY": "1"}
    return expected, env, tmp_path


@pytest.mark.parametrize(
    "mutation,code",
    [
        ("target", "TARGET_MISMATCH"),
        ("flag", "PUBLISHED_GATE_NOT_OFF"),
        ("missing_flag", "PUBLISHED_GATE_NOT_OFF"),
        ("schema", "SCHEMA_GATE_NOT_READY"),
        ("hash", "SOURCE_HASH_MISMATCH"),
        ("missing_hash", "SOURCE_HASHES_MISSING"),
        ("role", "RUNTIME_ROLE_MISMATCH"),
        ("budget", "BUDGET_MISMATCH"),
        ("version", "VERSION_MISMATCH"),
    ],
)
def test_preconditions_fail_before_any_runtime(metadata, mutation, code):
    expected, env, root = metadata
    if mutation == "target":
        env.pop("MODAL_ENVIRONMENT")
    elif mutation == "flag":
        env[probe.OFF_FLAGS[0]] = "1"
    elif mutation == "missing_flag":
        env.pop(probe.OFF_FLAGS[1])
    elif mutation == "schema":
        env["TRR_INSTAGRAM_INGESTION_SCHEMA_READY"] = "true"
    elif mutation == "hash":
        (root / next(iter(probe.REQUIRED_HASHES))).write_text("changed")
    elif mutation == "missing_hash":
        expected["source_hashes"].pop(next(iter(probe.REQUIRED_HASHES)))
    elif mutation == "role":
        expected["runtime_role"] = "postgres"
    elif mutation == "budget":
        expected["connection_limit"] = 5
    else:
        expected["workflow_version"] = "new-version"
    with pytest.raises(probe.ProbeError, match=code):
        probe.validate_expected(expected, env, root)


def test_exact_reviewed_metadata_is_accepted(metadata):
    expected, env, root = metadata
    assert probe.validate_expected(expected, env, root) is expected


@pytest.mark.parametrize(
    "dsn",
    [
        f"postgresql://role:private@db.{probe.PROJECT_REF}.supabase.co:6543/postgres?sslmode=require",
        "postgresql://role:private@db.other.supabase.co:5432/postgres?sslmode=require",
        f"postgresql://role:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres",
        "postgresql://role.other:private@aws-0-east.pooler.supabase.com:5432/postgres?sslmode=require",
        f"postgresql://role:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres?sslmode=require&options=unsafe",
    ],
)
def test_session_endpoint_ssl_project_guards(dsn):
    with pytest.raises(probe.ProbeError, match="SESSION_DSN_MISMATCH"):
        probe.validate_dsn(dsn)


def test_exact_direct_or_session_pooler_binding():
    for host, user in [
        (f"db.{probe.PROJECT_REF}.supabase.co", "trr_ingestion_runtime"),
        ("aws-0-east.pooler.supabase.com", f"trr_ingestion_runtime.{probe.PROJECT_REF}"),
    ]:
        dsn = f"postgresql://{user}:private@{host}:5432/postgres?sslmode=require"
        assert probe.validate_dsn(dsn, role="trr_ingestion_runtime") == dsn


@pytest.mark.parametrize(
    "sample",
    [
        probe.Sample((), (), business_rows=1),
        probe.Sample((), (), workflows=1),
        probe.Sample((), (), role_limit=5),
        probe.Sample((1, 2, 3, 4, 5), ()),
    ],
)
def test_existing_work_or_connection_budget_fails_closed(sample):
    with pytest.raises(probe.ProbeError):
        sample.safe()


class Clock:
    value = 0

    def now(self):
        return self.value

    def sleep(self, duration):
        self.value += duration


class Child:
    def __init__(self, clock, result, exit_at):
        self.clock, self.exit_at = clock, exit_at
        read_fd, write_fd = os.pipe()
        os.write(write_fd, json.dumps(result).encode())
        os.close(write_fd)
        self.stdout = os.fdopen(read_fd, "rb")

    def poll(self):
        return 0 if self.clock.value >= self.exit_at else None


def lifecycle(
    *, baseline=probe.Sample((), ()), contender="busy", holder_dies_at_busy=False, holder_swap=False, helper_result=None
):
    clock, events, children = Clock(), [], {}

    class Observer:
        def sample(self):
            events.append("sample")
            if "helper" not in children or clock.value >= 85:
                return baseline
            return probe.Sample((101, 102, 103), (102,) if holder_swap and "contender" in children else (101,))

        def fresh_lease(self):
            events.append("fresh")

    def spawn(role):
        events.append(role)
        if role == "helper":
            child = Child(
                clock, helper_result if helper_result is not None else {"status": "completed", "exit_code": 0}, 85
            )
        else:
            assert "sample" in events and "helper" in children
            if holder_dies_at_busy:
                children["helper"].exit_at = 0
            child = Child(clock, {"status": contender}, 0)
        children[role] = child
        return child

    return clock, events, Observer(), spawn


def test_actual_helper_and_contender_procedure_order_with_observation_fixture(metadata):
    clock, events, observer, spawn = lifecycle()
    result = probe.supervise(metadata[0], observer, spawn, clock=clock.now, sleep=clock.sleep)
    assert result["status"] == "passed" and result["peak_runtime_connections"] == 3
    assert events.index("helper") < events.index("contender") < events.index("fresh")
    assert result["holder_alive_at_busy"] and result["database_cleanup"]
    assert result["holder_backend_pid"] == 101 and result["holder_stable_through_busy"]
    assert result["private_ssl_policy"] == probe.PRIVATE_SSL_POLICY and not result["serving_ssl_defaults_proven"]


@pytest.mark.parametrize(
    "options,code",
    [
        ({"baseline": probe.Sample((1,), (1,))}, "RUNTIME_BASELINE_NOT_EMPTY"),
        ({"contender": "bounded"}, "CONTENDER_NOT_BUSY"),
        ({"holder_dies_at_busy": True}, "HOLDER_NOT_ALIVE_AT_BUSY"),
        ({"holder_swap": True}, "HOLDER_NOT_ALIVE_AT_BUSY"),
    ],
)
def test_lifecycle_does_not_accept_missing_singleton_evidence(metadata, options, code):
    clock, _, observer, spawn = lifecycle(**options)
    with pytest.raises(probe.ProbeError, match=code):
        probe.supervise(metadata[0], observer, spawn, clock=clock.now, sleep=clock.sleep)


def test_cleanup_checks_pid_start_identity_before_signalling():
    old = probe.ProcessIdentity(900001, 10, 900001, 1)
    replacement = probe.ProcessIdentity(900001, 10, 900001, 2)
    clock, signals = Clock(), []
    assert not probe.cleanup_owned(
        10,
        clock=clock.now,
        sleep=clock.sleep,
        identities=lambda parent: {old.pid: old},
        current=lambda pid: replacement,
        kill=lambda *args: signals.append(args),
        killpg=lambda *args: signals.append(args),
    )
    assert signals == []


def test_owned_tree_excludes_shared_processes(tmp_path):
    def stat(pid, parent, group, start):
        folder = tmp_path / str(pid)
        folder.mkdir()
        folder.joinpath("stat").write_text(f"{pid} (process name) S {parent} {group} " + "0 " * 16 + str(start))

    stat(10, 1, 10, 100)
    stat(11, 10, 11, 110)
    stat(12, 11, 12, 120)
    stat(20, 1, 20, 200)
    owned = probe.owned_descendants(10, tmp_path)
    assert set(owned) == {11, 12} and owned[12].started == 120


@pytest.mark.parametrize("disconnect", [False, True])
def test_independent_watchdog_deadline_or_eof_cleans_child_group(monkeypatch, metadata, disconnect):
    actual = subprocess.Popen
    children, cleanup = [], []

    def spawn(argv, **kwargs):
        child = actual([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(probe.sys, "platform", "linux")
    monkeypatch.setattr(probe.ctypes, "CDLL", lambda *a, **k: SimpleNamespace(prctl=lambda *args: 0))
    monkeypatch.setattr(probe.subprocess, "Popen", spawn)

    def clean(parent):
        cleanup.append(parent)
        os.killpg(children[0].pid, signal.SIGKILL)
        children[0].wait(timeout=2)
        return True

    monkeypatch.setattr(probe, "cleanup_owned", clean)
    read_fd, write_fd = os.pipe()
    try:
        if disconnect:
            os.close(write_fd)
        result = probe.watchdog(metadata[0], read_fd, timeout=0.15)  # pyright: ignore[reportArgumentType]
    finally:
        os.close(read_fd)
        if not disconnect:
            os.close(write_fd)
    assert result == {
        "status": "not_passed",
        "code": "CALLER_DISCONNECTED" if disconnect else "SUPERVISOR_DEADLINE",
        "process_cleanup": True,
    }
    assert cleanup == [os.getpid()] and children[0].poll() is not None
    with pytest.raises(ProcessLookupError):
        os.kill(children[0].pid, 0)


def test_cli_errors_never_emit_argument_or_credential_text(capsys):
    assert probe.main(["--unexpected", "postgresql://private:secret@db"]) == 1
    output = capsys.readouterr()
    assert not output.err and "private" not in output.out and "secret" not in output.out
    assert json.loads(output.out) == {"status": "not_passed", "code": "ARGUMENTS_INVALID"}


def test_observer_requires_ssl_in_connection_without_rewriting_published_dsn():
    dsn = f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres"
    assert probe.validate_dsn(dsn, observer=True) == dsn
    with pytest.raises(probe.ProbeError):
        probe.validate_dsn(dsn)
    with pytest.raises(probe.ProbeError):
        probe.validate_dsn(dsn + "?sslmode=disable", observer=True)


@pytest.mark.parametrize("ignored_guard", [None, "readonly", "timeout", "row_security", "client_tls"])
def test_observer_pooler_ignores_startup_options_but_session_guards_are_verified(monkeypatch, ignored_guard):
    import psycopg2

    observed, closed, calls = [], [], []
    # Model the live pooler: startup options have no effect, and server-side
    # pg_stat_ssl may be false despite client TLS. Only SET changes this state.
    state = {"readonly": "off", "row_security": "on", "timeout": False, "server_ssl": False}

    class Cursor:
        last = ""

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, statement, params=None):
            self.last = statement
            observed.append(statement)
            if statement == "set session default_transaction_read_only=on" and ignored_guard != "readonly":
                state["readonly"] = "on"
            elif statement == "set session statement_timeout='5000ms'" and ignored_guard != "timeout":
                state["timeout"] = True
            elif statement == "set session row_security=off" and ignored_guard != "row_security":
                state["row_security"] = "off"

        def fetchone(self):
            if "current_database()" in self.last:
                return ("postgres", "postgres", state["readonly"], state["row_security"], state["timeout"])
            return (True, False, False, False, False, False, 4)

        def fetchall(self):
            return [(name,) for name in probe.TABLES]

    conn = SimpleNamespace(
        cursor=Cursor, close=lambda: closed.append(True), info=SimpleNamespace(ssl_in_use=ignored_guard != "client_tls")
    )
    monkeypatch.setattr(psycopg2, "connect", lambda dsn, **kwargs: calls.append(kwargs) or conn)

    def make():
        return probe.Observer(
            SimpleNamespace(runtime_role="trr_ingestion_runtime"),
            f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres",
        )

    if ignored_guard:
        with pytest.raises(probe.ProbeError, match="OBSERVER_SESSION_UNSAFE"):
            make()
        assert closed == [True]
        assert not any("select relname" in statement or "from pg_roles" in statement for statement in observed)
    else:
        observer = make()
        observer.close()
        assert closed == [True] and state["server_ssl"] is False
    assert calls[0]["sslmode"] == "require" and calls[0]["connect_timeout"] == 5
    assert "default_transaction_read_only=on" in calls[0]["options"]
    assert "statement_timeout=5000" in calls[0]["options"] and "row_security=off" in calls[0]["options"]
    if ignored_guard != "client_tls":
        assert observed[:3] == [
            "set session default_transaction_read_only=on",
            "set session statement_timeout='5000ms'",
            "set session row_security=off",
        ]
        assert observed[3].startswith("select current_database()")
    else:
        assert observed == []
    assert not any(
        "pg_stat_ssl" in statement or "query " in statement or "query," in statement for statement in observed
    )


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://postgres:private@db.foreign.supabase.co:5432/postgres?sslmode=require",
        f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:6543/postgres?sslmode=require",
        f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres?sslmode=disable",
    ],
)
def test_valid_first_candidate_never_hides_unsafe_application_fallback(dsn):
    good = f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres?sslmode=require"
    with pytest.raises(probe.ProbeError, match="SESSION_DSN_MISMATCH"):
        probe.validate_candidates(({"url": good}, {"url": dsn}), "trr_ingestion_runtime")


@pytest.mark.parametrize("existing", [None, "require", "verify-ca", "verify-full", "disable", "prefer", ""])
def test_declared_private_ssl_policy_preserves_verification_and_rejects_insecure(existing):
    env = {} if existing is None else {"PGSSLMODE": existing}
    if existing in ("disable", "prefer", ""):
        with pytest.raises(probe.ProbeError, match="PRIVATE_SSL_POLICY_UNSAFE"):
            probe.enforce_private_ssl(env)
    else:
        assert probe.enforce_private_ssl(env) == (existing or "require")


@pytest.mark.parametrize("sample", [probe.Sample((1, 2), (1, 2)), probe.Sample((1,), (2,))])
def test_lock_evidence_requires_single_runtime_holder(sample):
    with pytest.raises(probe.ProbeError, match="LOCK_OWNERSHIP_INVALID"):
        sample.safe()


def test_final_output_read_is_bounded_when_real_descendant_retains_pipe(tmp_path):
    child_pid = tmp_path / "descendant.pid"
    script = (
        "import subprocess,sys,json; from pathlib import Path; "
        "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
        f"Path({str(child_pid)!r}).write_text(str(child.pid)); print(json.dumps(dict(status='busy')),flush=True)"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script], start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    try:
        child.wait(timeout=3)
        started = time.monotonic()
        with pytest.raises(probe.ProbeError, match="CHILD_OUTPUT_TIMEOUT"):
            probe.child_result(child, timeout=0.15)
        assert time.monotonic() - started < 1
    finally:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait(timeout=2)


def test_temporary_script_binds_reviewed_root_before_shadow_pythonpath(tmp_path):
    root, shadow = tmp_path / "reviewed", tmp_path / "shadow"
    for directory in (root, shadow):
        directory.joinpath("trr_backend").mkdir(parents=True)
        directory.joinpath("trr_backend/__init__.py").write_text("")
    root.joinpath("trr_backend/marker.py").write_text("VALUE='reviewed'\n")
    shadow.joinpath("trr_backend/marker.py").write_text("VALUE='shadow'\n")
    runner = tmp_path / "temporary_runner.py"
    runner.write_text(
        "import importlib.util,sys,os,json\n"
        f"spec=importlib.util.spec_from_file_location('proof',{str(Path(probe.__file__).resolve())!r})\n"
        "proof=importlib.util.module_from_spec(spec);sys.modules['proof']=proof;spec.loader.exec_module(proof)\n"
        f"os.chdir({str(root)!r});proof.bind_source_root()\n"
        "from trr_backend import marker\n"
        "proof.verify_module_origins(proof.Path.cwd());print(json.dumps(dict(value=marker.VALUE)))\n"
    )
    env = dict(os.environ, PYTHONPATH=str(shadow))
    result = subprocess.run([sys.executable, str(runner)], env=env, capture_output=True, timeout=3, check=True)
    assert json.loads(result.stdout) == {"value": "reviewed"}


def test_preloaded_foreign_app_module_is_rejected_instead_of_discarded(monkeypatch, tmp_path):
    foreign = SimpleNamespace(__file__=str(tmp_path / "shadow/trr_backend.py"))
    monkeypatch.setitem(sys.modules, "trr_backend", foreign)
    with pytest.raises(probe.ProbeError, match="MODULE_ORIGIN_MISMATCH"):
        probe.bind_source_root(tmp_path / "reviewed")
    assert sys.modules["trr_backend"] is foreign


@pytest.mark.parametrize("family", ["application", "session_control"])
def test_configuration_checks_both_complete_candidate_families_before_observer(monkeypatch, metadata, family):
    from trr_backend.db import connection
    from trr_backend.ingestion import composition
    from trr_backend.ingestion.workflows import RuntimeConfig

    expected = metadata[0]
    good = f"postgresql://postgres:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres"
    foreign = "postgresql://postgres:private@db.other.supabase.co:5432/postgres?sslmode=require"
    config = RuntimeConfig(
        system_database_url=(
            f"postgresql://trr_ingestion_runtime:private@db.{probe.PROJECT_REF}.supabase.co:5432/postgres?sslmode=require"
        ),
        workflow_version=expected["workflow_version"],
    )
    monkeypatch.setattr(composition, "runtime_config", lambda: config)
    monkeypatch.setattr(probe, "version", lambda name: "3.3.0")
    monkeypatch.setenv("PGSSLMODE", "require")
    calls = []

    def candidates(name):
        calls.append(name)
        return ({"url": good}, {"url": foreign}) if family == name else ({"url": good},)

    monkeypatch.setattr(connection, "resolve_database_url_candidate_details", lambda: candidates("application"))
    monkeypatch.setattr(
        connection, "resolve_session_database_url_candidate_details", lambda: candidates("session_control")
    )
    with pytest.raises(probe.ProbeError, match="SESSION_DSN_MISMATCH"):
        probe.configuration(expected)
    assert "application" in calls and (family != "session_control" or "session_control" in calls)


def test_helper_rejection_carries_bounded_lifecycle_observations_only(metadata):
    helper = {
        "status": "bounded_timeout",
        "exit_code": 124,
        "failure_class": "postgresql://private:secret@db",
        "env": {"token": "private-token"},
    }
    clock, _, observer, spawn = lifecycle(helper_result=helper)
    with pytest.raises(probe.ProbeError, match="HELPER_NOT_COMPLETED") as caught:
        probe.supervise(metadata[0], observer, spawn, clock=clock.now, sleep=clock.sleep)
    summary = caught.value.diagnostics
    assert summary["helper_result"] == {"status": "bounded_timeout", "exit_code": 124}
    assert 85 <= summary["elapsed_seconds"] < 86
    assert summary["peak_runtime_connections"] == 3 and summary["samples"] > 0
    assert summary["holder_backend_pid"] == 101 and summary["holder_stable_through_busy"]
    assert summary["contender_busy"] and summary["holder_alive_at_busy"]
    assert "private" not in json.dumps(summary) and "secret" not in json.dumps(summary)


def test_diagnostics_projection_rejects_unapproved_strings_fields_and_values():
    error = probe.ProbeError(
        "HELPER_NOT_COMPLETED",
        diagnostics={
            "helper_result": {"status": "private-secret", "exit_code": "postgresql://private", "token": "private"},
            "elapsed_seconds": float("inf"),
            "samples": -1,
            "peak_runtime_connections": True,
            "holder_backend_pid": "private",
            "contender_busy": True,
            "exception": "private",
            "env": {"token": "private"},
        },
    )
    assert error.diagnostics == {"helper_result": {"status": "invalid_result"}, "contender_busy": True}


def test_cli_retains_safe_probe_error_metadata_without_exception_repr(monkeypatch, capsys):
    monkeypatch.setattr(probe, "validate_expected", lambda *args: None)

    def execute(*args):
        raise probe.ProbeError(
            "HELPER_NOT_COMPLETED",
            diagnostics={
                "helper_result": {"status": "failed", "exit_code": 1, "error": "private-secret"},
                "elapsed_seconds": 90.12345,
                "samples": 5,
                "contender_busy": True,
            },
        )

    monkeypatch.setattr(probe, "execute", execute)
    assert probe.main(["--stage", "supervisor", "--expected-json", "{}"]) == 1
    captured = capsys.readouterr()
    assert not captured.err and "private" not in captured.out
    result = json.loads(captured.out)
    assert result["code"] == "HELPER_NOT_COMPLETED" and result["status"] == "not_passed"
    assert result["diagnostics"] == {
        "helper_result": {"status": "failed", "exit_code": 1},
        "elapsed_seconds": 90.123,
        "samples": 5,
        "contender_busy": True,
    }
