from __future__ import annotations

import pytest

from trr_backend.repositories.shows import ShowRepositoryError, assert_core_shows_table_exists


class _FakeResponse:
    def __init__(self, *, error=None):  # noqa: ANN001
        self.error = error
        self.data = []


class _FakeError(Exception):
    def __init__(self, *, code: str | None = None, message: str | None = None) -> None:
        super().__init__(message or "error")
        self.code = code
        self.message = message
        self.details = None
        self.hint = None

    def __str__(self) -> str:
        return self.message or super().__str__()


class _FakeClient:
    def __init__(self, *, response: _FakeResponse | None = None, exc: Exception | None = None) -> None:
        self._response = response
        self._exc = exc

    def schema(self, _name: str):  # noqa: ANN001
        return self

    def table(self, _name: str):  # noqa: ANN001
        return self

    def select(self, *_args, **_kwargs):  # noqa: ANN001, ANN002
        return self

    def limit(self, _n: int):  # noqa: ANN001
        return self

    def execute(self) -> _FakeResponse:
        if self._exc is not None:
            raise self._exc
        if self._response is None:
            raise RuntimeError("missing fake response")
        return self._response


def test_assert_core_shows_table_exists_passes_when_no_error() -> None:
    client = _FakeClient(response=_FakeResponse(error=None))
    assert_core_shows_table_exists(client)  # should not raise


def test_assert_core_shows_table_exists_raises_on_missing_relation_error_code() -> None:
    client = _FakeClient(response=_FakeResponse(error=_FakeError(code="42P01", message='relation "core.shows" does not exist')))
    with pytest.raises(ShowRepositoryError) as excinfo:
        assert_core_shows_table_exists(client)
    msg = str(excinfo.value)
    assert "supabase db push" in msg
    assert "supabase/migrations/0004_core_shows.sql" in msg


def test_assert_core_shows_table_exists_raises_on_missing_relation_exception() -> None:
    client = _FakeClient(exc=RuntimeError('relation "core.shows" does not exist'))
    with pytest.raises(ShowRepositoryError) as excinfo:
        assert_core_shows_table_exists(client)
    msg = str(excinfo.value)
    assert "supabase db push" in msg
    assert "supabase/migrations/0004_core_shows.sql" in msg

