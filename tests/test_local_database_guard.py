"""Fixture-owned PostgreSQL does not grant general database or network access."""

import os
import socket
from unittest.mock import Mock

import psycopg2
import pytest
import requests

from tests.conftest import _connect_to_registered_postgres
from trr_backend.db import connection, pg

ENDPOINT = ("127.0.0.1", "49152", "postgres", "isolated_test")
DSN = "postgresql://isolated_test@127.0.0.1:49152/postgres"
DENIED = "Non-live tests must not open network"


@pytest.mark.parametrize(
    "args,kwargs",
    [
        ((DSN,), {}),
        ((), {"dsn": DSN}),
        ((), dict(zip(("host", "port", "dbname", "user"), ENDPOINT, strict=True))),
    ],
)
def test_registered_endpoint_reaches_driver(args, kwargs):
    connect = Mock()
    assert _connect_to_registered_postgres(connect, {ENDPOINT}, *args, **kwargs) is connect.return_value
    connect.assert_called_once_with(*args, **kwargs)


@pytest.mark.parametrize(
    "dsn",
    [
        DSN.replace("127.0.0.1", "remote.invalid"),
        DSN.replace("49152", "49153"),
        DSN.replace("/postgres", "/production"),
        DSN.replace("isolated_test", "other_user"),
        DSN + "?hostaddr=203.0.113.10",
        DSN + "?service=production",
        DSN + "?options=-csearch_path%3Dpublic",
        DSN.replace("127.0.0.1", "127.0.0.1,remote.invalid"),
        "dbname=postgres",
        "invalid DSN",
    ],
)
def test_unregistered_or_overridden_connection_never_reaches_driver(dsn):
    connect = Mock()
    with pytest.raises(AssertionError, match=DENIED):
        _connect_to_registered_postgres(connect, {ENDPOINT}, dsn)
    connect.assert_not_called()


def test_registration_is_removed_after_fixture_context(register_isolated_postgres, _isolated_postgres_endpoints):
    connect = Mock()
    with register_isolated_postgres(host=ENDPOINT[0], port=int(ENDPOINT[1]), dbname=ENDPOINT[2], user=ENDPOINT[3]):
        assert ENDPOINT in _isolated_postgres_endpoints
        _connect_to_registered_postgres(connect, _isolated_postgres_endpoints, DSN)
    assert ENDPOINT not in _isolated_postgres_endpoints
    with pytest.raises(AssertionError, match=DENIED):
        _connect_to_registered_postgres(connect, _isolated_postgres_endpoints, DSN)
    assert connect.call_count == 1


def test_registered_database_keeps_network_and_unregistered_databases_blocked(register_isolated_postgres):
    with register_isolated_postgres(host=ENDPOINT[0], port=int(ENDPOINT[1]), dbname=ENDPOINT[2], user=ENDPOINT[3]):
        assert connection.resolve_database_url_candidate_details() == ()
        assert os.getenv("TRR_TEST_DISABLE_DOTENV") == "1"
        assert os.getenv("DATABASE_URL") is None
        assert os.getenv("TRR_DB_DIRECT_URL") is None
        with pytest.raises(AssertionError, match=DENIED):
            psycopg2.connect(DSN + "?hostaddr=203.0.113.10")
        with pytest.raises(AssertionError, match=DENIED):
            psycopg2.connect(DSN + "?service=production")
        with pytest.raises(AssertionError, match=DENIED):
            psycopg2.connect(DSN.replace("49152", "49153"))
        with pytest.raises(AssertionError, match=DENIED):
            socket.create_connection((ENDPOINT[0], int(ENDPOINT[1])))
        with pytest.raises(AssertionError, match=DENIED):
            requests.get("http://127.0.0.1:49152/", timeout=0.1)
        with pytest.raises(AssertionError, match=DENIED):
            pg.ThreadedConnectionPool(1, 1, dsn=DSN)


def test_registration_requires_explicit_loopback(register_isolated_postgres):
    with pytest.raises(ValueError, match="explicit loopback"):
        with register_isolated_postgres(host="remote.invalid", port=49152, dbname="postgres", user="isolated_test"):
            pytest.fail("remote endpoint registered")


@pytest.mark.parametrize("name", ["PGHOSTADDR", "PGSERVICE"])
def test_libpq_environment_cannot_redirect_registered_endpoint(monkeypatch, name):
    monkeypatch.setenv(name, "production")
    connect = Mock()
    with pytest.raises(AssertionError, match=DENIED):
        _connect_to_registered_postgres(connect, {ENDPOINT}, DSN)
    connect.assert_not_called()
