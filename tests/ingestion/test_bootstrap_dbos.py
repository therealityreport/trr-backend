"""Bootstrap safety checks and real DBOS migrations in disposable PostgreSQL."""

import json
import logging
from urllib.parse import quote, unquote, urlsplit

import psycopg2
import pytest

from scripts.ingestion.bootstrap_dbos import PROJECT_REF, load_target, main, parser
from tests.ingestion import test_persistence

base_database = test_persistence.database
PASSWORD = "test-runtime-password-not-for-output-123456"
ADMIN_PASSWORD = "test-admin-password-not-for-output"
ROLE = "pipeline_dbos_runtime"


def arguments(*, apply=False):
    args = [
        "--project-ref",
        PROJECT_REF,
        "--environment",
        "main",
        "--admin-dsn-env",
        "TEST_DBOS_ADMIN_DSN",
        "--runtime-dsn-env",
        "TEST_DBOS_RUNTIME_DSN",
        "--runtime-role",
        ROLE,
        "--workflow-version",
        "instagram-ingestion-v1",
    ]
    if apply:
        args += ["--apply", "--password-env", "TEST_DBOS_PASSWORD", "--connection-limit", "4"]
    return args


def environment():
    return {
        "TEST_DBOS_ADMIN_DSN": f"postgresql://postgres:{quote(ADMIN_PASSWORD)}@db.{PROJECT_REF}.supabase.co:5432/postgres?sslmode=require",
        "TEST_DBOS_RUNTIME_DSN": f"postgresql://{ROLE}:{quote(PASSWORD)}@db.{PROJECT_REF}.supabase.co:5432/postgres?sslmode=require",
        "TEST_DBOS_PASSWORD": PASSWORD,
    }


def safe_output(capsys):
    captured = capsys.readouterr()
    for secret in (PASSWORD, ADMIN_PASSWORD, "postgresql://", "SCRAM-SHA-256$"):
        assert secret not in captured.out + captured.err
    assert not captured.err
    return json.loads(captured.out)


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("project", "aywqykmrlgzgdhaysajr", "PROJECT_BINDING_MISMATCH"),
        (
            "runtime",
            f"postgresql://{ROLE}:{PASSWORD}@db.{PROJECT_REF}.supabase.co:6543/postgres?sslmode=require",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        (
            "runtime",
            f"postgresql://{ROLE}:{PASSWORD}@db.otherref.supabase.co:5432/postgres?sslmode=require",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        (
            "runtime",
            f"postgresql://{ROLE}:{PASSWORD}@db.{PROJECT_REF}.supabase.co:5432/new_database?sslmode=require",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        (
            "runtime",
            f"postgresql://{ROLE}:{PASSWORD}@db.{PROJECT_REF}.supabase.co:5432/postgres?sslmode=require&options=unsafe",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        (
            "runtime",
            f"postgresql://postgres:{PASSWORD}@db.{PROJECT_REF}.supabase.co:5432/postgres?sslmode=require",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        (
            "runtime",
            f"postgresql://{ROLE}:{PASSWORD}@db.{PROJECT_REF}.supabase.co:5432/postgres",
            "INVALID_OR_MISMATCHED_SESSION_DSN",
        ),
        ("role", "service_role", "INVALID_RUNTIME_ROLE"),
        ("password", "another-valid-but-mismatching-password", "RUNTIME_PASSWORD_ENV_MISMATCH_OR_TOO_SHORT"),
    ],
)
def test_target_and_credential_guards_precede_any_connection(capsys, field, value, error):
    args, env = arguments(apply=True), environment()
    if field == "project":
        args[args.index("--project-ref") + 1] = value
    elif field == "role":
        args[args.index("--runtime-role") + 1] = value
    elif field == "password":
        env["TEST_DBOS_PASSWORD"] = value
    else:
        env["TEST_DBOS_RUNTIME_DSN"] = value
    assert main(args, environ=env, connect=lambda *_: pytest.fail("Guard reached a database")) == 1
    assert safe_output(capsys)["error"] == error


def test_apply_requires_password_and_bounded_connections_without_echoing_bad_arguments(capsys):
    args = arguments() + ["--apply"]
    assert main(args, environ=environment(), connect=lambda *_: pytest.fail("Missing apply inputs reached DB")) == 1
    assert safe_output(capsys)["error"] == "APPLY_REQUIRES_PASSWORD_ENV_AND_CONNECTION_LIMIT"
    assert main(arguments() + ["--unexpected", PASSWORD], environ=environment()) == 1
    assert safe_output(capsys)["error"] == "INVALID_ARGUMENTS"


@pytest.fixture(scope="module")
def local_database(base_database, register_isolated_postgres):
    _, transaction = base_database
    with transaction() as conn:
        params = conn.get_dsn_parameters()
    admin_url = f"postgresql://{params['user']}@{params['host']}:{params['port']}/postgres"
    calls = []

    def connect(supplied):
        # All external-looking strings are validated but redirected to this
        # fixture's loopback server. No production endpoint is contacted.
        parsed = urlsplit(supplied)
        login = unquote(parsed.username).removesuffix("." + PROJECT_REF)
        username = params["user"] if login == "postgres" else login
        calls.append(username)
        assert unquote(parsed.password)  # the supplied DSN still carries a credential
        # The fixture cluster uses trust auth, and the non-live guard admits only
        # host/port/dbname/user, so the password is validated but not forwarded.
        return psycopg2.connect(host=params["host"], port=params["port"], dbname="postgres", user=username)

    def migrate(supplied, role):
        from dbos import DBOS

        assert supplied == environment()["TEST_DBOS_ADMIN_DSN"]
        DBOS.migrate(admin_url, application_role=role)

    # The bootstrap also logs in as the restricted runtime role it creates.
    with register_isolated_postgres(host=params["host"], port=int(params["port"]), dbname="postgres", user=ROLE):
        yield transaction, connect, migrate, calls


def test_inspect_is_read_only_then_apply_proves_restricted_runtime_and_repeat_inspect(local_database, capsys):
    transaction, connect, migrate, calls = local_database
    migrated = []

    def tracked_migrate(*args):
        migrated.append(True)
        return migrate(*args)

    assert main(arguments(), environ=environment(), connect=connect, migrate=tracked_migrate) == 2
    inspection = safe_output(capsys)
    assert not inspection["ready"] and not inspection["schema_exists"] and not inspection["role_exists"]
    assert not migrated and calls == ["ingestion_test"]
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from pg_roles where rolname=%s", (ROLE,))
        assert cur.fetchone() == (0,)
    assert main(arguments(apply=True), environ=environment(), connect=connect, migrate=tracked_migrate) == 0
    applied = safe_output(capsys)
    assert applied["ready"] and not applied["credentials_exported"]
    assert applied["migration_version"] == applied["required_migration_version"]
    checks = applied["runtime_checks"]
    assert checks["usage"] and checks["table_access"] and checks["sequence_access"] and checks["function_access"]
    assert checks["run_migrations"] is False and not checks["create"] and not checks["application_access"]
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """select rolpassword,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,rolinherit,
            rolconnlimit from pg_authid where rolname=%s""",
            (ROLE,),
        )
        verifier, *flags, limit = cur.fetchone()
        assert verifier.startswith("SCRAM-SHA-256$") and PASSWORD not in verifier
        assert not any(flags) and limit == 4
    assert (
        main(
            arguments(),
            environ=environment(),
            connect=connect,
            migrate=lambda *_: pytest.fail("Inspect invoked migration"),
        )
        == 0
    )
    assert safe_output(capsys)["ready"]
    assert len(migrated) == 1


def test_existing_privileged_role_is_not_silently_altered(local_database, capsys):
    transaction, connect, _, _ = local_database
    unsafe_role = "unsafe_dbos_runtime"
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("create role unsafe_dbos_runtime login superuser connection limit 4")
    args, env = arguments(apply=True), environment()
    args[args.index("--runtime-role") + 1] = unsafe_role
    env["TEST_DBOS_RUNTIME_DSN"] = env["TEST_DBOS_RUNTIME_DSN"].replace(ROLE, unsafe_role)
    assert (
        main(args, environ=env, connect=connect, migrate=lambda *_: pytest.fail("Unsafe role reached migration")) == 1
    )
    assert safe_output(capsys)["error"] == "EXISTING_ROLE_IS_NOT_RESTRICTED"
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("select rolsuper from pg_roles where rolname=%s", (unsafe_role,))
        assert cur.fetchone() == (True,)


def test_migration_failure_suppresses_sdk_logs_and_reports_reconciliation(local_database, capsys):
    _, connect, _, _ = local_database

    def noisy_failure(*args):
        print(PASSWORD)
        logging.getLogger("dbos").error(ADMIN_PASSWORD)
        raise RuntimeError("postgresql://secret-in-provider-exception")

    assert main(arguments(apply=True), environ=environment(), connect=connect, migrate=noisy_failure) == 1
    assert safe_output(capsys)["error"] == "MIGRATION_FAILED_RECONCILE_BEFORE_RETRY"


def test_explicit_supabase_session_pooler_binding_and_secret_repr():
    env = environment()
    host = "aws-0-us-east-1.pooler.supabase.com"
    env["TEST_DBOS_ADMIN_DSN"] = (
        f"postgresql://postgres.{PROJECT_REF}:{quote(ADMIN_PASSWORD)}@{host}:5432/postgres?sslmode=require"
    )
    env["TEST_DBOS_RUNTIME_DSN"] = (
        f"postgresql://{ROLE}.{PROJECT_REF}:{quote(PASSWORD)}@{host}:5432/postgres?sslmode=require"
    )
    target = load_target(parser().parse_args(arguments(apply=True)), env)
    assert target.apply and target.role == ROLE
    assert PASSWORD not in repr(target) and ADMIN_PASSWORD not in repr(target) and "postgresql://" not in repr(target)


def test_interrupted_inspection_has_no_traceback_or_credential_output(capsys):
    def interrupted(dsn):
        raise KeyboardInterrupt()

    assert main(arguments(), environ=environment(), connect=interrupted) == 130
    assert safe_output(capsys)["error"] == "INTERRUPTED_RECONCILE_STATE"
