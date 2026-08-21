from __future__ import annotations

import os
import socket

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


@pytest.fixture
def pristine_environ() -> dict[str, str]:
    return _PRISTINE_ENVIRON


@pytest.fixture(autouse=True)
def _isolate_non_live_tests(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(connection.psycopg2, "connect", _blocked_non_live_access)
    monkeypatch.setattr(pg, "ThreadedConnectionPool", _blocked_non_live_access)
