"""Temporary in-container lifecycle proof; never provisions or calls Modal.

Parent must verify the deployed container/target before exec. Pass nonsecret JSON
via --expected-json, including reviewed source hashes. Published gates stay off;
only A's private environment enables the unchanged coordinator helper. Successful
JSON returned after OS and database cleanup is the acceptance evidence. Missing
output/container disappearance is not success. Linux /proc and subreaping are
required. No DSN, exception text, SQL text, or credential is emitted.

Modal 1.5.5 invocation must separate remote argv from CLI options:
    <verified-modal-cli> container exec --profile admin-56995 --no-pty \
        <verified-container-id> -- python -c <wrapper>
Without the `--` separator, `-c` is parsed as a Modal option and no remote
execution occurs. The parent verifies that exact container belongs to main /
trr-backend-jobs before invocation; the script still checks published metadata.

Supported container-exec wrapper shape (source_b64, expected_json and source_root
are nonsecret reviewed inputs; source_root contains trr_backend/). Expected JSON
must declare private_ssl_policy="require_when_absent_preserve_verified". The
probe sets PGSSLMODE=require only when absent, preserves require/verify-ca/full,
and rejects insecure explicit modes in every app/control candidate. This proves
lifecycle under that declared private SSL policy, not serving SSL defaults:


    directory = tempfile.mkdtemp(prefix="trr-ingestion-runtime-proof-")
    os.chmod(directory, 0o700)
    path = pathlib.Path(directory) / "probe.py"
    path.write_bytes(base64.b64decode(source_b64, validate=True))
    os.chmod(path, 0o600)
    child = None
    try:
        child = subprocess.Popen(
            [sys.executable, str(path), "--expected-json", expected_json],
            cwd=source_root, start_new_session=True)
        child.wait(timeout=170)
    finally:
        if child is not None and child.poll() is None:
            child.kill()  # closes its liveness pipe; detached watchdog cleans
            child.wait(timeout=5)
        shutil.rmtree(directory)  # this newly created dedicated directory only

The wrapper must report its own exception/timeout using a fixed not_passed JSON
code, never a traceback. Its imports are Python stdlib only. It must pass through
the probe's final safe JSON, and require status=passed and process_cleanup=true.
The watchdog survives wrapper/exec disconnect, observes liveness-pipe EOF and
cleans owned descendants. No returned final evidence means not passed. Temporary
source removal after a disconnect does not cancel that already-running watchdog.

"""

from __future__ import annotations

import argparse
import contextlib
import ctypes
import hashlib
import importlib
import json
import logging
import os
import re
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

PROJECT_REF = "vwxfvzutyufrkhfgoeaa"
DEADLINE_SECONDS = 150
CLEANUP_SECONDS = 5
TARGET = {
    "MODAL_PROFILE": "admin-56995",
    "MODAL_WORKSPACE": "admin-56995",
    "MODAL_ENVIRONMENT": "main",
    "TRR_MODAL_APP_NAME": "trr-backend-jobs",
}
OFF_FLAGS = (
    "TRR_INSTAGRAM_INGESTION_ENABLED",
    "TRR_INSTAGRAM_INGESTION_REPLAY_APPLY_ENABLED",
    "TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED",
)
REQUIRED_HASHES = frozenset(
    f"trr_backend/ingestion/{name}.py"
    for name in ("runtime", "workflows", "workflow_store", "composition", "persistence", "admission")
)
REQUIRED_HASHES |= {"trr_backend/db/connection.py", "trr_backend/db/pg.py"}
PRIVATE_SSL_POLICY = "require_when_absent_preserve_verified"
TABLES = tuple(
    "instagram_ingestion_" + name
    for name in (
        "runs",
        "commands",
        "outbox",
        "manifests",
        "targets",
        "discovery_pages",
        "evidence",
        "receipts",
        "partitions",
        "decisions",
        "dispatches",
        "comment_pages",
        "replay_receipts",
    )
)


class ProbeError(Exception):
    """Fixed safe code and explicitly projected lifecycle observations only."""

    def __init__(self, code, *, diagnostics=None):
        super().__init__(code)
        self.diagnostics = safe_probe_diagnostics(diagnostics or {})


def safe_helper_result(value):
    statuses = {"disabled", "unconfigured", "timed_out", "completed", "bounded_timeout", "lease_lost", "failed"}
    if value is None:
        return {"status": "not_observed"}
    if not isinstance(value, dict):
        return {"status": "invalid_result"}
    status = value.get("status")
    result = {"status": status if isinstance(status, str) and status in statuses else "invalid_result"}
    code = value.get("exit_code")
    if type(code) is int and -255 <= code <= 255:
        result["exit_code"] = code
    return result


def safe_probe_diagnostics(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    if "helper_result" in value:
        result["helper_result"] = safe_helper_result(value["helper_result"])
    elapsed = value.get("elapsed_seconds")
    if type(elapsed) in (float, int) and 0 <= elapsed <= DEADLINE_SECONDS + CLEANUP_SECONDS + 5:
        result["elapsed_seconds"] = round(elapsed, 3)
    for key in ("peak_runtime_connections", "samples", "holder_backend_pid"):
        number = value.get(key)
        if type(number) is int and 0 <= number <= 2**31 - 1:
            result[key] = number
    for key in ("contender_busy", "holder_alive_at_busy", "holder_stable_through_busy"):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    return result


def require(condition, code):
    if not condition:
        raise ProbeError(code)


def validate_expected(expected, env, root=Path(".")):
    require(isinstance(expected, dict), "EXPECTED_METADATA_INVALID")
    require(expected.get("project_ref") == PROJECT_REF, "PROJECT_MISMATCH")
    require(expected.get("target") == TARGET and all(env.get(k) == v for k, v in TARGET.items()), "TARGET_MISMATCH")
    require(all(env.get(k, "").lower() in {"0", "false", "off", "no"} for k in OFF_FLAGS), "PUBLISHED_GATE_NOT_OFF")
    require(env.get("TRR_INSTAGRAM_INGESTION_SCHEMA_READY") == "1", "SCHEMA_GATE_NOT_READY")
    require(expected.get("runtime_role") == "trr_ingestion_runtime", "RUNTIME_ROLE_MISMATCH")
    require(expected.get("workflow_version") == "instagram-ingestion-v1", "VERSION_MISMATCH")
    require(expected.get("executor_id") == "instagram-coordinator-slot-0", "EXECUTOR_MISMATCH")
    require(expected.get("lock_namespace") == 48112 and expected.get("lock_key") == 20261016, "LOCK_MISMATCH")
    require(expected.get("lifetime_seconds") == 90 and expected.get("connection_limit") == 4, "BUDGET_MISMATCH")
    require(expected.get("private_ssl_policy") == PRIVATE_SSL_POLICY, "PRIVATE_SSL_POLICY_MISSING")
    hashes = expected.get("source_hashes")
    require(isinstance(hashes, dict) and REQUIRED_HASHES <= hashes.keys(), "SOURCE_HASHES_MISSING")
    for relative, digest in hashes.items():
        require(
            isinstance(relative, str) and re.fullmatch(r"(?:trr_backend|scripts)/[a-zA-Z0-9_/]+\.py", relative),
            "SOURCE_PATH_INVALID",
        )
        require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), "SOURCE_HASH_INVALID")
        path = root / relative
        require(path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == digest, "SOURCE_HASH_MISMATCH")
    return expected


def validate_dsn(value, *, role=None, observer=False):
    try:
        parsed = urlsplit(value)
        username = unquote(parsed.username or "")
        host = parsed.hostname or ""
        direct = host == f"db.{PROJECT_REF}.supabase.co"
        pooler = host.endswith(".pooler.supabase.com") and username.endswith("." + PROJECT_REF)
        actual_role = username if direct else username.removesuffix("." + PROJECT_REF)
        query = parse_qs(parsed.query, strict_parsing=True)
        valid = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.port == 5432
            and parsed.path == "/postgres"
            and not parsed.fragment
            and (direct or pooler)
            and bool(parsed.password)
            and bool(re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", actual_role))
            and (
                query.get("sslmode") in (["require"], ["verify-ca"], ["verify-full"])
                or (observer and "sslmode" not in query)
            )
            and set(query) <= {"sslmode", "sslrootcert", "connect_timeout", "application_name"}
            and all(len(v) == 1 for v in query.values())
            and (role is None or role == actual_role)
        )
        require(valid, "SESSION_DSN_MISMATCH")
    except Exception:
        raise ProbeError("SESSION_DSN_MISMATCH") from None
    return value


def verify_module_origins(root):
    root = root.resolve()
    for name, module in tuple(sys.modules.items()):
        if name.split(".")[0] not in {"trr_backend", "api"} or module is None:
            continue
        relative = Path(*name.split("."))
        origin = getattr(module, "__file__", None)
        if origin is not None:
            resolved = Path(origin).resolve()
            require(
                resolved in {(root / relative).with_suffix(".py"), root / relative / "__init__.py"},
                "MODULE_ORIGIN_MISMATCH",
            )
        else:
            paths = tuple(getattr(module, "__path__", ()))
            require(
                bool(paths) and all(Path(value).resolve() == root / relative for value in paths),
                "MODULE_ORIGIN_MISMATCH",
            )


def bind_source_root(root=None):
    root = (root or Path.cwd()).resolve()
    verify_module_origins(root)  # Never discard an already-imported shadow.
    require((root / "trr_backend/__init__.py").is_file(), "SOURCE_PACKAGE_MISSING")
    sys.path.insert(0, str(root))
    importlib.invalidate_caches()
    importlib.import_module("trr_backend")
    verify_module_origins(root)
    return root


def validate_candidates(candidates, runtime_role):
    require(bool(candidates), "APPLICATION_CREDENTIAL_MISSING")
    for candidate in candidates:
        # Both resolver families are reachable by actual application/control pools.
        dsn = validate_dsn(candidate.get("url"), observer=True)
        require(unquote(urlsplit(dsn).username or "").split(".")[0] != runtime_role, "APPLICATION_ROLE_INVALID")
    return candidates[0]["url"]


def enforce_private_ssl(env=None):
    env = os.environ if env is None else env
    require(env.get("PGSSLMODE", "require") in {"require", "verify-ca", "verify-full"}, "PRIVATE_SSL_POLICY_UNSAFE")
    env.setdefault("PGSSLMODE", "require")
    return env["PGSSLMODE"]


def configuration(expected):
    root = bind_source_root()
    from trr_backend.db.connection import (
        resolve_database_url_candidate_details,
        resolve_session_database_url_candidate_details,
    )
    from trr_backend.ingestion.composition import runtime_config

    # Explicit private libpq policy is inherited by unchanged A/helper/runtime/B.
    # It does not establish the serving process's original SSL defaults.
    enforce_private_ssl()
    config = runtime_config()
    require(
        all(
            getattr(config, key) == expected[key]
            for key in (
                "runtime_role",
                "workflow_version",
                "executor_id",
                "lock_namespace",
                "lock_key",
                "lifetime_seconds",
            )
        ),
        "RUNTIME_CONFIG_MISMATCH",
    )
    require(version("dbos") == "3.3.0", "SDK_VERSION_MISMATCH")
    require(
        config.dbos_config()["sys_db_pool_size"] == 1
        and config.dbos_config()["db_engine_kwargs"]["max_overflow"] == 0
        and config.dbos_config()["run_migrations"] is False,
        "POOL_CONFIG_MISMATCH",
    )
    validate_dsn(config.system_database_url, role=expected["runtime_role"])
    candidates = resolve_database_url_candidate_details()
    observer = validate_candidates(candidates, expected["runtime_role"])
    validate_candidates(resolve_session_database_url_candidate_details(), expected["runtime_role"])
    verify_module_origins(root)
    return config, observer


@dataclass(frozen=True)
class Sample:
    pids: tuple[int, ...]
    lock_pids: tuple[int, ...]
    business_rows: int = 0
    workflows: int = 0
    role_limit: int = 4

    def safe(self):
        require(len(self.lock_pids) <= 1 and set(self.lock_pids) <= set(self.pids), "LOCK_OWNERSHIP_INVALID")
        require(self.business_rows == 0 and self.workflows == 0, "EXISTING_WORK_DETECTED")
        require(self.role_limit == 4 and len(self.pids) <= 4, "ROLE_CONNECTION_BUDGET_FAILED")


class Observer:
    def __init__(self, config, observer_dsn):
        import psycopg2

        self.config = config
        self.conn = psycopg2.connect(
            observer_dsn,
            connect_timeout=5,
            application_name="trr-ingestion-runtime-proof-observer",
            sslmode=parse_qs(urlsplit(observer_dsn).query).get("sslmode", [enforce_private_ssl()])[0],
            options="-c default_transaction_read_only=on -c statement_timeout=5000 -c row_security=off",
        )
        self.conn.autocommit = True
        try:
            # pg_stat_ssl describes the pooler-to-server leg. This is the
            # actual client-to-pooler TLS state for our observer connection.
            require(bool(self.conn.info.ssl_in_use), "OBSERVER_SESSION_UNSAFE")
            with self.conn.cursor() as cur:
                # Session poolers may ignore libpq startup options. Establish
                # every observer guard explicitly before the first read.
                cur.execute("set session default_transaction_read_only=on")
                cur.execute("set session statement_timeout='5000ms'")
                cur.execute("set session row_security=off")
                cur.execute(
                    "select current_database(),current_user,current_setting('default_transaction_read_only'),"
                    "current_setting('row_security'),(current_setting('statement_timeout')::interval "
                    "> interval '0 seconds' "
                    "and current_setting('statement_timeout')::interval <= interval '5 seconds')"
                )
                database, role, readonly, row_security, bounded_timeout = cur.fetchone()
                require(
                    database == "postgres"
                    and role != config.runtime_role
                    and readonly == "on"
                    and row_security == "off"
                    and bounded_timeout,
                    "OBSERVER_SESSION_UNSAFE",
                )
                cur.execute(
                    "select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace "
                    "where n.nspname='social' and c.relkind in ('r','p') and relname like 'instagram_ingestion_%'"
                )
                require({row[0] for row in cur.fetchall()} == set(TABLES), "BUSINESS_SCHEMA_MISMATCH")
                cur.execute(
                    "select rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolconnlimit "
                    "from pg_roles where rolname=%s",
                    (config.runtime_role,),
                )
                row = cur.fetchone()
                require(row and row[0] and not any(row[1:6]) and row[6] == 4, "RUNTIME_ROLE_UNSAFE")
        except BaseException:
            with contextlib.suppress(Exception):
                self.conn.close()
            raise

    def sample(self):
        with self.conn.cursor() as cur:
            cur.execute("select pid from pg_stat_activity where usename=%s order by pid", (self.config.runtime_role,))
            pids = tuple(row[0] for row in cur.fetchall())
            cur.execute(
                "select l.pid from pg_locks l join pg_stat_activity a on a.pid=l.pid "
                "where l.locktype='advisory' and l.granted and l.classid=%s and l.objid=%s "
                "and l.objsubid=2 and a.usename=%s order by l.pid",
                (self.config.lock_namespace, self.config.lock_key, self.config.runtime_role),
            )
            locks = tuple(row[0] for row in cur.fetchall())
            cur.execute(
                "select sum(n) from ("
                + " union all ".join(f"select count(*) n from social.{table}" for table in TABLES)
                + ") counts"
            )
            business = int(cur.fetchone()[0])
            cur.execute("select count(*) from dbos.workflow_status")
            workflows = int(cur.fetchone()[0])
            cur.execute("select rolconnlimit from pg_roles where rolname=%s", (self.config.runtime_role,))
            limit = cur.fetchone()[0]
        result = Sample(pids, locks, business, workflows, limit)
        result.safe()
        return result

    def fresh_lease(self):
        import psycopg

        with psycopg.connect(self.config.system_database_url, autocommit=True, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select pg_try_advisory_lock(%s,%s)", (self.config.lock_namespace, self.config.lock_key))
                require(cur.fetchone()[0], "FRESH_LEASE_FAILED")
                cur.execute("select pg_advisory_unlock(%s,%s)", (self.config.lock_namespace, self.config.lock_key))
                require(cur.fetchone()[0], "FRESH_UNLOCK_FAILED")

    def close(self):
        self.conn.close()


def child_command(stage, expected):
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "--stage",
        stage,
        "--expected-json",
        json.dumps(expected, separators=(",", ":")),
    ]


def child_result(child, *, timeout=1.0):
    require(child.poll() in (0, 1), "CHILD_FAILED")
    output = bytearray()
    end = time.monotonic() + timeout
    try:
        while time.monotonic() < end:
            readable, _, _ = select.select([child.stdout], [], [], min(0.1, max(0, end - time.monotonic())))
            if not readable:
                continue
            block = os.read(child.stdout.fileno(), 8193 - len(output))
            if not block:
                result = json.loads(output)
                require(isinstance(result, dict), "CHILD_OUTPUT_INVALID")
                return result
            output.extend(block)
            require(len(output) <= 8192, "CHILD_OUTPUT_INVALID")
        raise ProbeError("CHILD_OUTPUT_TIMEOUT")
    finally:
        child.stdout.close()


def supervise(expected, observer, spawn, *, clock=time.monotonic, sleep=time.sleep):
    baseline = observer.sample()
    require(not baseline.pids and not baseline.lock_pids, "RUNTIME_BASELINE_NOT_EMPTY")
    started = clock()
    peak, samples, b, busy_proven, holder_pid = 0, 0, None, False, None
    actual_helper_result, contender_busy = None, False
    a = spawn("helper")
    try:
        while a.poll() is None:
            sample = observer.sample()
            samples += 1
            peak = max(peak, len(sample.pids))
            if holder_pid is not None:
                require(sample.lock_pids == (holder_pid,), "HOLDER_CHANGED_DURING_CONTENDER")
            if sample.lock_pids and b is None:
                holder_pid = sample.lock_pids[0]
                b = spawn("contender")
            if b is not None and b.poll() is not None:
                contender_busy = child_result(b) == {"status": "busy"}
                require(contender_busy, "CONTENDER_NOT_BUSY")
                require(a.poll() is None and observer.sample().lock_pids == (holder_pid,), "HOLDER_NOT_ALIVE_AT_BUSY")
                busy_proven = True
                break
            require(clock() - started < 30, "SINGLETON_PROOF_TIMEOUT")
            sleep(0.2)
        require(busy_proven, "CONTENDER_BUSY_NOT_PROVEN")
        while a.poll() is None:
            sample = observer.sample()
            samples += 1
            peak = max(peak, len(sample.pids))
            require(clock() - started < 120, "HELPER_TIMEOUT")
            sleep(0.2)
        actual_helper_result = child_result(a)
        require(actual_helper_result == {"status": "completed", "exit_code": 0}, "HELPER_NOT_COMPLETED")
        require(80 <= clock() - started <= 120, "HELPER_LIFETIME_MISMATCH")
        require(3 <= peak <= 4, "CONNECTION_DEMAND_NOT_PROVEN")
        for _ in range(25):
            final = observer.sample()
            if not final.pids and not final.lock_pids:
                break
            sleep(0.2)
        require(not final.pids and not final.lock_pids, "DATABASE_CLEANUP_UNCONFIRMED")
        observer.fresh_lease()
        require(not observer.sample().pids and not observer.sample().lock_pids, "FRESH_SESSION_CLEANUP_UNCONFIRMED")
        return {
            "status": "passed",
            "code": "BOUNDED_LIFECYCLE_PROVEN",
            "peak_runtime_connections": peak,
            "samples": samples,
            "contender_busy": True,
            "holder_alive_at_busy": True,
            "holder_backend_pid": holder_pid,
            "holder_stable_through_busy": True,
            "private_ssl_policy": expected["private_ssl_policy"],
            "serving_ssl_defaults_proven": False,
            "database_cleanup": True,
            "fresh_lease": True,
            "business_rows": 0,
            "workflows": 0,
            "source_hashes": expected["source_hashes"],
        }
    except ProbeError as error:
        raise ProbeError(
            str(error),
            diagnostics={
                "helper_result": actual_helper_result,
                "elapsed_seconds": clock() - started,
                "peak_runtime_connections": peak,
                "samples": samples,
                "holder_backend_pid": holder_pid,
                "contender_busy": contender_busy,
                "holder_alive_at_busy": busy_proven,
                "holder_stable_through_busy": busy_proven,
            },
        ) from None


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    parent: int
    group: int
    started: int


def process_identity(pid, proc_root=Path("/proc")):
    try:
        fields = (proc_root / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
        return ProcessIdentity(pid, int(fields[1]), int(fields[2]), int(fields[19]))
    except (OSError, ValueError, IndexError):
        return None


def owned_descendants(parent, proc_root=Path("/proc")):
    processes = [process_identity(int(path.name), proc_root) for path in proc_root.iterdir() if path.name.isdigit()]
    owned = {parent}
    result = {}
    while True:
        found = {p.pid: p for p in processes if p and p.parent in owned and p.pid not in owned}
        if not found:
            return result
        result.update(found)
        owned.update(found)


def cleanup_owned(
    parent,
    *,
    clock=time.monotonic,
    sleep=time.sleep,
    identities=owned_descendants,
    current=process_identity,
    kill=os.kill,
    killpg=os.killpg,
):
    end = clock() + CLEANUP_SECONDS
    while clock() < end:
        owned = identities(parent)
        if not owned:
            return True
        for item in reversed(list(owned.values())):
            if current(item.pid) != item:
                continue  # PID reuse is never authority to signal a replacement.
            try:
                if item.group == item.pid:
                    killpg(item.group, signal.SIGKILL)
                else:
                    kill(item.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        while True:
            try:
                if os.waitpid(-1, os.WNOHANG)[0] == 0:
                    break
            except ChildProcessError:
                break
        sleep(0.05)
    return not identities(parent)


def watchdog(expected, parent_pipe, *, timeout=DEADLINE_SECONDS):
    require(sys.platform == "linux", "LINUX_WATCHDOG_REQUIRED")
    require(ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) == 0, "SUBREAPER_REQUIRED")
    child = subprocess.Popen(
        child_command("supervisor", expected),
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + timeout
    code = None
    try:
        while child.poll() is None:
            readable, _, _ = select.select([parent_pipe], [], [], 0.1)
            if readable and os.read(parent_pipe, 1) == b"":
                code = "CALLER_DISCONNECTED"
                break
            if time.monotonic() >= deadline:
                code = "SUPERVISOR_DEADLINE"
                break
    finally:
        # Orphans can retain stdout after the supervisor exits. Close their
        # descriptors by owned cleanup before the bounded final output read.
        cleaned = cleanup_owned(os.getpid())
        if child.poll() is None:
            child.wait(timeout=1)
    require(cleaned, "PROCESS_CLEANUP_UNCONFIRMED")
    result = child_result(child) if code is None else {"status": "not_passed", "code": code}
    result["process_cleanup"] = True
    return result


@contextlib.contextmanager
def quiet():
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            yield
    finally:
        logging.disable(previous)


def execute(stage, expected):
    validate_expected(expected, os.environ)
    config, observer_dsn = configuration(expected)
    if stage in {"helper", "contender"}:
        # Recheck immediately in each private process before any runtime call.
        guard = Observer(config, observer_dsn)
        try:
            sample = guard.sample()
            if stage == "helper":
                require(not sample.pids and not sample.lock_pids, "RUNTIME_BASELINE_NOT_EMPTY")
            else:
                require(bool(sample.lock_pids), "HOLDER_LOCK_NOT_OBSERVED")
        finally:
            guard.close()
    if stage == "helper":
        from trr_backend.ingestion.runtime import run_ingestion_coordinator

        verify_module_origins(Path.cwd())
        os.environ["TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED"] = "1"
        return run_ingestion_coordinator()
    if stage == "contender":
        import psycopg

        from trr_backend.ingestion.composition import build_scheduler
        from trr_backend.ingestion.workflows import DBOSAdapter

        adapter = DBOSAdapter(build_scheduler(), config)
        verify_module_origins(Path.cwd())
        result = adapter.run_bounded(
            lambda: psycopg.connect(config.system_database_url, autocommit=True, connect_timeout=5)
        )
        return {"status": result}
    observer = Observer(config, observer_dsn)
    try:

        def spawn(role):
            return subprocess.Popen(
                child_command(role, expected),
                start_new_session=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )

        return supervise(expected, observer, spawn)
    finally:
        observer.close()


def main(argv=None):
    class SafeParser(argparse.ArgumentParser):
        def error(self, message):
            raise ProbeError("ARGUMENTS_INVALID")

    try:
        parser = SafeParser(add_help=False)
        parser.add_argument("--expected-json", required=True)
        parser.add_argument(
            "--stage", choices=("wrapper", "watchdog", "supervisor", "helper", "contender"), default="wrapper"
        )
        parser.add_argument("--parent-pipe", type=int)
        args = parser.parse_args(argv)
        expected = json.loads(args.expected_json)
        with quiet():
            validate_expected(expected, os.environ)
            if args.stage == "wrapper":
                read_fd, write_fd = os.pipe()
                try:
                    child = subprocess.Popen(
                        child_command("watchdog", expected) + ["--parent-pipe", str(read_fd)],
                        start_new_session=True,
                        pass_fds=(read_fd,),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL,
                    )
                    os.close(read_fd)
                    child.wait(timeout=DEADLINE_SECONDS + CLEANUP_SECONDS + 5)
                    result = child_result(child)
                finally:
                    os.close(write_fd)
            elif args.stage == "watchdog":
                require(args.parent_pipe is not None, "PARENT_PIPE_REQUIRED")
                result = watchdog(expected, args.parent_pipe)
            else:
                result = execute(args.stage, expected)
    except ProbeError as error:
        result = {"status": "not_passed", "code": str(error)}
        if error.diagnostics:
            result["diagnostics"] = error.diagnostics
    except Exception:
        result = {"status": "not_passed", "code": "PROBE_FAILED"}
    try:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    except BrokenPipeError:
        return 1
    return 0 if result.get("status") in {"passed", "completed", "busy"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
