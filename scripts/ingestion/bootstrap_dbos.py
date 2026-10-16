"""Inspect/bootstrap DBOS 3.3 in an explicitly bound, existing TRR database.

Run as a module from TRR-Backend. All credentials are read by explicit env name;
none are printed, exported, accepted as argument values or written to disk.
Default inspect is read-only. --apply creates an absent role and runs DBOS's
supported migration/grant API. Existing roles are never password-rotated.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from importlib.metadata import version
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg2
from psycopg2 import extensions, sql
from psycopg2.extras import RealDictCursor

PROJECT_REF = "vwxfvzutyufrkhfgoeaa"
_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,127}")
_ROLE_NAME = re.compile(r"[a-z_][a-z0-9_]{0,62}")


class BootstrapError(Exception):
    """Safe code only; never include provider exceptions or credential values."""


class SafeParser(argparse.ArgumentParser):
    def error(self, message):
        raise BootstrapError("INVALID_ARGUMENTS")


@dataclass(frozen=True)
class Target:
    project_ref: str
    environment: str
    role: str
    workflow_version: str
    admin_dsn: str = field(repr=False)
    runtime_dsn: str = field(repr=False)
    password: str | None = field(default=None, repr=False)
    connection_limit: int | None = None
    apply: bool = False


def _env(env, name):
    if not _ENV_NAME.fullmatch(name) or not env.get(name):
        raise BootstrapError("REQUIRED_CREDENTIAL_ENV_MISSING")
    return env[name]


def _dsn(value, project_ref, *, runtime_role=None):
    try:
        parsed = urlsplit(value)
        username = unquote(parsed.username or "")
        host = parsed.hostname or ""
        query = parse_qs(parsed.query, strict_parsing=True)
        direct = host == f"db.{project_ref}.supabase.co"
        pooler = host.endswith(".pooler.supabase.com") and username.endswith("." + project_ref)
        role = username if direct else username.removesuffix("." + project_ref)
        valid = (
            parsed.scheme in {"postgres", "postgresql"}
            and parsed.port == 5432
            and parsed.path == "/postgres"
            and not parsed.fragment
            and (direct or pooler)
            and bool(parsed.password)
            and _ROLE_NAME.fullmatch(role)
            and query.get("sslmode") in (["require"], ["verify-ca"], ["verify-full"])
            and set(query) <= {"sslmode", "sslrootcert", "connect_timeout", "application_name"}
            and all(len(values) == 1 for values in query.values())
            and (runtime_role is None or role == runtime_role)
        )
        if not valid:
            raise ValueError
        return parsed
    except Exception:
        raise BootstrapError("INVALID_OR_MISMATCHED_SESSION_DSN") from None


def load_target(args, env) -> Target:
    if args.project_ref != PROJECT_REF:
        raise BootstrapError("PROJECT_BINDING_MISMATCH")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", args.environment):
        raise BootstrapError("INVALID_ENVIRONMENT")
    if (
        not _ROLE_NAME.fullmatch(args.runtime_role)
        or args.runtime_role.startswith("pg_")
        or args.runtime_role in {"postgres", "anon", "authenticated", "service_role", "supabase_admin"}
    ):
        raise BootstrapError("INVALID_RUNTIME_ROLE")
    admin, runtime = _env(env, args.admin_dsn_env), _env(env, args.runtime_dsn_env)
    admin_url = _dsn(admin, args.project_ref)
    runtime_url = _dsn(runtime, args.project_ref, runtime_role=args.runtime_role)
    if unquote(admin_url.username or "").split(".")[0] == args.runtime_role:
        raise BootstrapError("ADMIN_AND_RUNTIME_ROLE_MUST_DIFFER")
    password = _env(env, args.password_env) if args.password_env else None
    if args.apply and (password is None or args.connection_limit is None):
        raise BootstrapError("APPLY_REQUIRES_PASSWORD_ENV_AND_CONNECTION_LIMIT")
    if password is not None and (len(password) < 24 or password != unquote(runtime_url.password or "")):
        raise BootstrapError("RUNTIME_PASSWORD_ENV_MISMATCH_OR_TOO_SHORT")
    if args.connection_limit is not None and not 1 <= args.connection_limit <= 32:
        raise BootstrapError("INVALID_CONNECTION_LIMIT")
    if not args.workflow_version.strip():
        raise BootstrapError("WORKFLOW_VERSION_REQUIRED")
    return Target(
        args.project_ref,
        args.environment,
        args.runtime_role,
        args.workflow_version,
        admin,
        runtime,
        password,
        args.connection_limit,
        args.apply,
    )


def _connect(dsn):
    return psycopg2.connect(dsn, connect_timeout=10, application_name="trr-dbos-bootstrap")


def _inspect_admin(conn, role):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""select current_database() as database,
            exists(select 1 from pg_database where datname=current_database() and datallowconn) as existing,
            to_regnamespace('dbos') is not null as schema_exists,
            to_regclass('dbos.dbos_migrations') is not null as migrations_exist""")
        result = dict(cur.fetchone())
        if result["database"] != "postgres" or not result["existing"]:
            raise BootstrapError("EXISTING_POSTGRES_DATABASE_REQUIRED")
        cur.execute(
            """select rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls,
            rolinherit,rolconnlimit,
            exists(select 1 from pg_auth_members m where m.member=r.oid) as member,
            exists(select 1 from pg_namespace n where n.nspowner=r.oid) as schema_owner,
            exists(select 1 from pg_database d where d.datdba=r.oid) as database_owner,
            exists(select 1 from pg_class c where c.relowner=r.oid) as object_owner,
            exists(select 1 from pg_namespace n where n.nspname !~ '^pg_'
              and has_schema_privilege(r.oid,n.oid,'CREATE')) as schema_create
            from pg_roles r where rolname=%s""",
            (role,),
        )
        row = cur.fetchone()
        result["role"] = dict(row) if row else None
        result["migration_version"] = 0
        if result["migrations_exist"]:
            cur.execute("select coalesce(max(version),0) from dbos.dbos_migrations")
            result["migration_version"] = cur.fetchone()["coalesce"]
        return result


def _restricted(role, connection_limit=None):
    return bool(
        role
        and role["rolcanlogin"]
        and role["rolconnlimit"] > 0
        and (connection_limit is None or role["rolconnlimit"] <= connection_limit)
        and not any(
            role[name]
            for name in (
                "rolsuper",
                "rolcreatedb",
                "rolcreaterole",
                "rolreplication",
                "rolbypassrls",
                "rolinherit",
                "member",
                "schema_owner",
                "database_owner",
                "object_owner",
                "schema_create",
            )
        )
    )


def _latest_migration():
    if version("dbos") != "3.3.0":
        raise BootstrapError("DBOS_330_REQUIRED")
    from dbos._migration import get_dbos_migrations

    return len(get_dbos_migrations("dbos", True, False))


def _migrate(dsn, role):
    from dbos import DBOS

    DBOS.migrate(dsn, application_role=role)


@contextlib.contextmanager
def _quiet_sdk():
    # This is a short-lived CLI, not a shared API process. SDK/SQLAlchemy error
    # logs may include URLs or SQL. Suppress them, including import-time output.
    previous = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            yield
    finally:
        logging.disable(previous)


def _runtime_checks(conn, target):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""select current_user as role,
          coalesce(has_schema_privilege(current_user,to_regnamespace('dbos'),'USAGE'),false) as usage,
          coalesce(has_schema_privilege(current_user,to_regnamespace('dbos'),'CREATE'),false) as create,
          exists(select 1 from pg_database where datname=current_database()
            and has_database_privilege(current_user,oid,'CREATE')) as database_create,
          (select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname='dbos' and c.relkind in ('r','p','v','m','f')) as tables,
          not exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname='dbos' and c.relkind in ('r','p','v','m','f') and not (
              has_table_privilege(current_user,c.oid,'SELECT') and has_table_privilege(current_user,c.oid,'INSERT')
              and has_table_privilege(current_user,c.oid,'UPDATE') and has_table_privilege(current_user,c.oid,'DELETE')
            )) as table_access,
          not exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname='dbos' and c.relkind='S' and not (
              has_sequence_privilege(current_user,c.oid,'USAGE') and has_sequence_privilege(current_user,c.oid,'SELECT')
              and has_sequence_privilege(current_user,c.oid,'UPDATE'))) as sequence_access,
          not exists(select 1 from pg_proc p join pg_namespace n on n.oid=p.pronamespace
            where n.nspname='dbos' and not has_function_privilege(current_user,p.oid,'EXECUTE')) as function_access,
          exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname in ('core','social') and c.relkind in ('r','p','v','m','f') and (
              has_table_privilege(current_user,c.oid,'SELECT') or has_table_privilege(current_user,c.oid,'INSERT')
              or has_table_privilege(current_user,c.oid,'UPDATE') or has_table_privilege(current_user,c.oid,'DELETE')
            )) as application_access""")
        result = dict(cur.fetchone())
    from trr_backend.ingestion.workflows import RuntimeConfig

    result["run_migrations"] = RuntimeConfig(
        target.runtime_dsn, target.workflow_version, runtime_role=target.role
    ).dbos_config()["run_migrations"]
    result["checked"] = True
    result["ready"] = bool(
        result["role"] == target.role
        and result["usage"]
        and not result["create"]
        and not result["database_create"]
        and result["tables"] > 0
        and result["table_access"]
        and result["sequence_access"]
        and result["function_access"]
        and not result["application_access"]
        and result["run_migrations"] is False
    )
    return result


def run(target: Target, *, connect=_connect, migrate=_migrate):
    with _quiet_sdk():
        latest = _latest_migration()
    try:
        with contextlib.closing(connect(target.admin_dsn)) as conn, conn:
            conn.set_session(readonly=not target.apply)
            inspected = _inspect_admin(conn, target.role)
            role = inspected["role"]
            if role is not None and not _restricted(role, target.connection_limit):
                raise BootstrapError("EXISTING_ROLE_IS_NOT_RESTRICTED")
            if target.apply and role is None:
                verifier = extensions.encrypt_password(target.password, target.role, conn, algorithm="scram-sha-256")
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("""create role {} login password %s nosuperuser nocreatedb nocreaterole
                        noreplication nobypassrls noinherit connection limit {}""").format(
                            sql.Identifier(target.role), sql.Literal(target.connection_limit)
                        ),
                        (verifier,),
                    )
                    cur.execute(sql.SQL("grant connect on database postgres to {}").format(sql.Identifier(target.role)))
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("ADMIN_INSPECTION_OR_ROLE_CREATION_FAILED") from None
    if target.apply:
        try:
            # Prove the supplied runtime credential before any schema migration
            # or grant. A newly created role may remain after a failed login;
            # inspect and reconcile it rather than rotating credentials blindly.
            with contextlib.closing(connect(target.runtime_dsn)) as conn, conn:
                conn.set_session(readonly=True)
                with conn.cursor() as cur:
                    cur.execute("select current_user,current_database()")
                    if cur.fetchone() != (target.role, "postgres"):
                        raise BootstrapError("RUNTIME_LOGIN_IDENTITY_MISMATCH")
        except BootstrapError:
            raise
        except Exception:
            raise BootstrapError("RUNTIME_LOGIN_FAILED_RECONCILE_ROLE") from None
        try:
            with _quiet_sdk():
                migrate(target.admin_dsn, target.role)
        except Exception:
            raise BootstrapError("MIGRATION_FAILED_RECONCILE_BEFORE_RETRY") from None
        with contextlib.closing(connect(target.admin_dsn)) as conn, conn:
            conn.set_session(readonly=True)
            inspected = _inspect_admin(conn, target.role)
        if not _restricted(inspected["role"], target.connection_limit):
            raise BootstrapError("POST_APPLY_ROLE_PRIVILEGES_UNSAFE")
    runtime = {"ready": False, "checked": False, "run_migrations": None}
    if inspected["role"] is not None and inspected["schema_exists"]:
        try:
            with contextlib.closing(connect(target.runtime_dsn)) as conn, conn:
                conn.set_session(readonly=True)
                runtime = _runtime_checks(conn, target)
        except Exception:
            raise BootstrapError("RUNTIME_READINESS_CHECK_FAILED") from None
    ready = runtime["ready"] and inspected["migration_version"] == latest
    return {
        "mode": "apply" if target.apply else "inspect",
        "project_ref": target.project_ref,
        "environment": target.environment,
        "runtime_role": target.role,
        "database": "postgres",
        "schema": "dbos",
        "schema_exists": inspected["schema_exists"],
        "role_exists": inspected["role"] is not None,
        "migration_version": inspected["migration_version"],
        "required_migration_version": latest,
        "runtime_checks": runtime,
        "ready": ready,
        "credentials_exported": False,
        "next_actions": [] if ready else ["review_missing_schema_role_or_privileges", "apply_only_after_review"],
    }


def parser():
    result = SafeParser(description=__doc__)
    for flag in ("project-ref", "environment", "admin-dsn-env", "runtime-dsn-env", "runtime-role", "workflow-version"):
        result.add_argument("--" + flag, required=True)
    result.add_argument("--password-env")
    result.add_argument("--connection-limit", type=int)
    result.add_argument("--apply", action="store_true")
    return result


def main(argv=None, *, environ=None, connect=_connect, migrate=_migrate):
    try:
        args = parser().parse_args(argv)
        target = load_target(args, os.environ if environ is None else environ)
        result = run(target, connect=connect, migrate=migrate)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["ready"] else 2
    except KeyboardInterrupt:
        print(json.dumps({"ready": False, "error": "INTERRUPTED_RECONCILE_STATE"}))
        return 130
    except BootstrapError as error:
        print(json.dumps({"ready": False, "error": str(error)}))
        return 1
    except Exception:
        print(json.dumps({"ready": False, "error": "BOOTSTRAP_FAILED_RECONCILE_STATE"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
