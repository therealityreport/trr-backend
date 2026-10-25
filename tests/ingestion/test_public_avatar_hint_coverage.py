"""Saved owner IDs select one public query path; no source requests."""

from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from trr_backend.ingestion import composition
from trr_backend.ingestion.domain import LostOwnership


class Store:
    def __init__(self, rows, *, lost=False):
        self.rows, self.lost, self.queries = rows, lost, []

    @contextmanager
    def transaction(self):
        yield self

    @contextmanager
    def cursor(self, **kwargs):
        yield self

    def check_owner(self, **kwargs):
        assert kwargs["connection"] is self
        if self.lost:
            raise LostOwnership("fixture")

    def execute(self, query, args):
        self.queries.append((query, args))

    def fetchall(self):
        return self.rows


def lookup(rows, handle="peacock"):
    store = Store(rows)
    result = composition._public_avatar_lookup_hint(store, {}, handle)
    query, args = store.queries[0]
    assert "where username=%s limit 2" in query and args == (handle,)
    return result


@pytest.mark.parametrize(
    "handle,identity",
    [
        ("bravodailydish", "19391525128"),
        ("peacock", "19846578012"),
        ("nbc", "42817733"),
    ],
)
def test_saved_three_handle_shapes_with_missing_native_user(handle, identity):
    assert (
        lookup(
            [
                {
                    "native_user": None,
                    "owner_detail": {"username": handle, "user_id": identity},
                    "owner_username": handle,
                    "owner_user_id": identity,
                }
            ],
            handle,
        )
        == identity
    )


@pytest.mark.parametrize("branch", ["native_user", "native_owner", "owner_detail"])
def test_native_id_keeps_precedence_and_saved_branch_unchanged(branch):
    node = {"username": "peacock", "id": "123", "pk": "456"}
    assert lookup([{branch: node}]) == "123"
    assert node == {"username": "peacock", "id": "123", "pk": "456"}


def test_source_account_match_is_insufficient_without_associated_owner():
    assert (
        lookup(
            [{"owner_username": "bravotv", "owner_user_id": "123", "native_user": {"username": "bravotv", "id": "123"}}]
        )
        is None
    )
    assert lookup([{"owner_user_id": "123", "owner_detail": {"user_id": "123"}}]) is None


@pytest.mark.parametrize(
    "rows",
    [
        [{"owner_username": "peacock", "owner_user_id": "not-an-id"}],
        [{"native_user": {"username": "peacock", "id": "bad", "pk": "123"}}],
        [
            {"owner_username": "peacock", "owner_user_id": "123"},
            {"owner_detail": {"username": "peacock", "user_id": "999"}},
        ],
        [{"owner_username": "peacock", "owner_user_id": "123", "native_owner": {"username": "peacock", "id": "999"}}],
    ],
)
def test_malformed_or_conflicting_eligible_bindings_have_no_hint(rows):
    assert lookup(rows) is None


def test_stale_owner_prevents_any_lookup():
    store = Store([], lost=True)
    with pytest.raises(LostOwnership):
        composition._public_avatar_lookup_hint(store, {}, "peacock")
    assert store.queries == []


def test_initial_graphql_path_has_no_failure_fallback(monkeypatch):
    from trr_backend.ingestion import avatar_transport, profile_transport

    store = Store([{"owner_username": "peacock", "owner_user_id": "19846578012"}])
    source = Mock(side_effect=RuntimeError("fixture source failure"))
    graphql = Mock(return_value=source)
    legacy = Mock()
    monkeypatch.setattr(profile_transport, "PublicGraphQLProfileSource", graphql)
    monkeypatch.setattr(avatar_transport, "AvatarProfileSource", legacy)
    selected = composition._public_avatar_source(store, {}, "peacock", 100)
    graphql.assert_called_once_with(user_id="19846578012", deadline=100)
    with pytest.raises(RuntimeError):
        selected(username="peacock", pre_source_request=Mock())
    assert source.call_count == 1
    legacy.assert_not_called()
