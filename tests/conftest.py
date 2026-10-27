from __future__ import annotations

import os
import socket
from contextlib import contextmanager

import pytest

_PRISTINE_ENVIRON = dict(os.environ)
_DATABASE_CONNECTION_ENV_NAMES = (
    "TRR_DB_DIRECT_URL",
    "TRR_DB_TRANSACTION_URL",
    "TRR_DB_SESSION_URL",
    "TRR_DB_FALLBACK_URL",
    "TRR_DB_URL",
    "TRR_DB_RUNTIME_LANE",
    "TRR_DB_TRANSACTION_FLIGHT_TEST",
    "SUPABASE_DB_URL",
    "DATABASE_URL",
)

# This runs when pytest imports the root conftest, before it imports test modules.
# Collection-time socket patching is intentionally unsupported: item markers are
# unavailable then, so a global patch cannot preserve explicit live-test ownership
# and can interfere with pytest plugins. The per-test guard below covers execution.
for _database_connection_env_name in _DATABASE_CONNECTION_ENV_NAMES:
    os.environ.pop(_database_connection_env_name, None)

os.environ["TRR_TEST_DISABLE_DOTENV"] = "1"
# Keep application-level CORS middleware deterministic for non-live TestClient
# imports. These are the canonical local Portless web origins, never secrets.
os.environ["CORS_ALLOW_ORIGINS"] = "https://trr.localhost,https://admin.trr.localhost"


def _blocked_non_live_access(*_args: object, **_kwargs: object) -> None:
    raise AssertionError(
        "Non-live tests must not open network or database connections. "
        "Use a fake, or mark an intentionally external test with pytest.mark.live."
    )


def _connect_to_registered_postgres(connect, registered, *args, **kwargs):
    """Allow only a fixture-owned endpoint, with no libpq service/options override."""
    from psycopg2 import ProgrammingError
    from psycopg2.extensions import make_dsn, parse_dsn

    if os.getenv("PGHOSTADDR") or os.getenv("PGSERVICE"):
        return _blocked_non_live_access()
    if len(args) > 1 or (args and "dsn" in kwargs):
        return _blocked_non_live_access()
    parameters = dict(kwargs)
    dsn = args[0] if args else parameters.pop("dsn", None)
    try:
        parsed = parse_dsn(make_dsn(dsn, **parameters))
    except (ProgrammingError, TypeError):
        return _blocked_non_live_access()
    if set(parsed) != {"host", "port", "dbname", "user"}:
        return _blocked_non_live_access()
    endpoint = tuple(parsed[key] for key in ("host", "port", "dbname", "user"))
    if endpoint not in registered:
        return _blocked_non_live_access()
    return connect(*args, **kwargs)


@pytest.fixture(scope="session")
def _isolated_postgres_endpoints():
    return set()


@pytest.fixture(scope="session")
def register_isolated_postgres(_isolated_postgres_endpoints):
    """Register a temporary cluster only for its owning fixture's lifetime."""

    @contextmanager
    def register(*, host, port, dbname, user):
        if host != "127.0.0.1" or not 0 < port < 65536 or not dbname or not user:
            raise ValueError("An explicit loopback PostgreSQL endpoint is required")
        endpoint = (host, str(port), dbname, user)
        if endpoint in _isolated_postgres_endpoints:
            raise ValueError("PostgreSQL endpoint is already registered")
        _isolated_postgres_endpoints.add(endpoint)
        try:
            yield
        finally:
            _isolated_postgres_endpoints.remove(endpoint)

    return register


@pytest.fixture
def pristine_environ() -> dict[str, str]:
    return _PRISTINE_ENVIRON


# Non-live tests pin this fixture name instead of the real ingestion account,
# whose name stays out of the repository.
FIXTURE_INGESTION_USERNAME = "fixture_ingest_account"


@pytest.fixture(autouse=True)
def _pin_fixture_ingestion_username(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.get_closest_marker("live") is not None:
        return
    import hashlib

    monkeypatch.setattr(
        "trr_backend.ingestion.credential_snapshot.AUTHORIZED_USERNAME_SHA256",
        hashlib.sha256(FIXTURE_INGESTION_USERNAME.encode("utf-8")).hexdigest(),
    )


@pytest.fixture(autouse=True)
def _isolate_non_live_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, _isolated_postgres_endpoints
) -> None:
    """Keep the default suite independent of local credentials and services."""
    if request.node.get_closest_marker("live") is not None:
        for name in _DATABASE_CONNECTION_ENV_NAMES:
            if name in _PRISTINE_ENVIRON:
                monkeypatch.setenv(name, _PRISTINE_ENVIRON[name])
        return

    for name in _DATABASE_CONNECTION_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    from trr_backend.db import connection, pg

    connection.resolve_database_url.cache_clear()
    connection.resolve_database_url_candidates.cache_clear()
    connection.resolve_database_url_candidate_details.cache_clear()
    monkeypatch.setattr(socket, "create_connection", _blocked_non_live_access)
    monkeypatch.setattr(socket.socket, "connect", _blocked_non_live_access)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked_non_live_access)
    connect = connection.psycopg2.connect
    monkeypatch.setattr(
        connection.psycopg2,
        "connect",
        lambda *args, **kwargs: _connect_to_registered_postgres(connect, _isolated_postgres_endpoints, *args, **kwargs),
    )
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _blocked_non_live_access)
