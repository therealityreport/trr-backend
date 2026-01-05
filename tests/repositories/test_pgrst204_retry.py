from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from trr_backend.db.postgrest_cache import is_pgrst204_error
from trr_backend.repositories.shows import (
    ShowRepositoryError,
    _handle_pgrst204_with_retry,
    insert_show,
    update_show,
)
from trr_backend.models.shows import ShowUpsert


class _FakeResponse:
    def __init__(self, *, data=None, error=None):  # noqa: ANN001
        self.data = data or []
        self.error = error


class _FakePGRST204Error(Exception):
    """Simulates a PGRST204 schema cache error."""

    def __init__(self, column: str = "genres") -> None:
        super().__init__(f"Could not find the '{column}' column of 'core.shows' in the schema cache")
        self.code = "PGRST204"


class _FakeClient:
    """Fake Supabase client for testing retry logic."""

    def __init__(
        self,
        *,
        fail_count: int = 0,
        response: _FakeResponse | None = None,
    ) -> None:
        self._fail_count = fail_count
        self._attempts = 0
        self._response = response or _FakeResponse(data=[{"id": "test-uuid", "name": "Test Show"}])

    def schema(self, _name: str):  # noqa: ANN001
        return self

    def table(self, _name: str):  # noqa: ANN001
        return self

    def select(self, *_args, **_kwargs):  # noqa: ANN001, ANN002
        return self

    def insert(self, _payload: dict):  # noqa: ANN001
        return self

    def update(self, _payload: dict):  # noqa: ANN001
        return self

    def eq(self, _col: str, _val: str):  # noqa: ANN001
        return self

    def limit(self, _n: int):  # noqa: ANN001
        return self

    def execute(self) -> _FakeResponse:
        self._attempts += 1
        if self._attempts <= self._fail_count:
            raise _FakePGRST204Error()
        return self._response


def test_is_pgrst204_error_detects_schema_cache_error() -> None:
    err = _FakePGRST204Error()
    assert is_pgrst204_error(err) is True


def test_is_pgrst204_error_ignores_other_errors() -> None:
    err = RuntimeError("Some other error")
    assert is_pgrst204_error(err) is False


def test_is_pgrst204_error_detects_error_by_message() -> None:
    err = RuntimeError("PGRST204: could not find the 'new_column' column")
    assert is_pgrst204_error(err) is True


def test_handle_pgrst204_with_retry_returns_false_for_non_pgrst204() -> None:
    err = RuntimeError("Some other error")
    assert _handle_pgrst204_with_retry(err, attempt=0, context="test") is False


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
@patch("trr_backend.repositories.shows.time.sleep")
def test_handle_pgrst204_with_retry_returns_true_on_first_attempt(
    mock_sleep: MagicMock,
    mock_reload: MagicMock,
) -> None:
    err = _FakePGRST204Error()
    result = _handle_pgrst204_with_retry(err, attempt=0, context="test")
    assert result is True
    mock_reload.assert_called_once()
    mock_sleep.assert_called_once_with(0.5)


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
def test_handle_pgrst204_with_retry_raises_on_max_retries(mock_reload: MagicMock) -> None:
    err = _FakePGRST204Error()
    with pytest.raises(ShowRepositoryError) as excinfo:
        _handle_pgrst204_with_retry(err, attempt=1, context="inserting show")
    msg = str(excinfo.value)
    assert "PostgREST schema cache may still be stale" in msg
    assert "reload_postgrest_schema.sql" in msg
    mock_reload.assert_not_called()


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
@patch("trr_backend.repositories.shows.time.sleep")
def test_insert_show_retries_on_pgrst204(
    mock_sleep: MagicMock,
    mock_reload: MagicMock,
) -> None:
    # First call fails with PGRST204, second succeeds
    client = _FakeClient(fail_count=1)
    show = ShowUpsert(name="Test Show", description="A test", premiere_date="2025-01-01")

    result = insert_show(client, show)

    assert result["name"] == "Test Show"
    assert client._attempts == 2
    mock_reload.assert_called_once()
    mock_sleep.assert_called_once()


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
def test_insert_show_raises_after_max_retries(mock_reload: MagicMock) -> None:
    # Always fails with PGRST204
    client = _FakeClient(fail_count=100)
    show = ShowUpsert(name="Test Show", description="A test", premiere_date="2025-01-01")

    with pytest.raises(ShowRepositoryError) as excinfo:
        insert_show(client, show)

    msg = str(excinfo.value)
    assert "PostgREST schema cache may still be stale" in msg
    # Should have tried twice (initial + 1 retry)
    assert client._attempts == 2


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
@patch("trr_backend.repositories.shows.time.sleep")
def test_update_show_retries_on_pgrst204(
    mock_sleep: MagicMock,
    mock_reload: MagicMock,
) -> None:
    # First call fails with PGRST204, second succeeds
    client = _FakeClient(fail_count=1)

    result = update_show(client, "test-uuid", {"description": "Updated"})

    assert result["name"] == "Test Show"
    assert client._attempts == 2
    mock_reload.assert_called_once()
    mock_sleep.assert_called_once()


@patch("trr_backend.repositories.shows.reload_postgrest_schema")
def test_update_show_raises_after_max_retries(mock_reload: MagicMock) -> None:
    # Always fails with PGRST204
    client = _FakeClient(fail_count=100)

    with pytest.raises(ShowRepositoryError) as excinfo:
        update_show(client, "test-uuid", {"description": "Updated"})

    msg = str(excinfo.value)
    assert "PostgREST schema cache may still be stale" in msg
    # Should have tried twice (initial + 1 retry)
    assert client._attempts == 2
