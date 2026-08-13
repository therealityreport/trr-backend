from __future__ import annotations

import importlib
import os
import socket
from pathlib import Path

import pytest

from trr_backend.utils import env

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
_COLLECTION_DATABASE_ENVIRONMENT = {name: os.getenv(name) for name in _DATABASE_CONNECTION_ENV_NAMES}
_COLLECTION_DOTENV_PATHS: list[Path] = []
_dotenv_values = env.dotenv_values


def _record_dotenv_read(path: Path) -> dict[str, str]:
    _COLLECTION_DOTENV_PATHS.append(path)
    raise AssertionError(f"Non-live collection must not read dotenv file: {path}")


env.dotenv_values = _record_dotenv_read
try:
    _COLLECTION_DOTENV_RESULT = env.load_env()
finally:
    env.dotenv_values = _dotenv_values

connection = importlib.import_module("trr_backend.db.connection")


def test_non_live_suite_has_no_runtime_database_url() -> None:
    assert connection.resolve_database_url_candidate_details() == ()
    with pytest.raises(connection.DatabaseConnectionError, match="No database URL configured"):
        connection.resolve_database_url()


def test_non_live_collection_strips_database_credentials_and_skips_dotenv() -> None:
    assert os.getenv("TRR_TEST_DISABLE_DOTENV") == "1"
    assert all(value is None for value in _COLLECTION_DATABASE_ENVIRONMENT.values())
    assert _COLLECTION_DOTENV_RESULT is None
    assert _COLLECTION_DOTENV_PATHS == []


def test_non_live_suite_blocks_network_connections() -> None:
    with pytest.raises(AssertionError, match="Non-live tests must not open network"):
        socket.create_connection(("example.com", 443))


def test_non_live_suite_blocks_direct_psycopg_connections() -> None:
    with pytest.raises(AssertionError, match="Non-live tests must not open network"):
        connection.psycopg2.connect("postgresql://example.invalid/postgres")
