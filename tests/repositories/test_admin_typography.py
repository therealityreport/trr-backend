from __future__ import annotations

from datetime import UTC, datetime

import pytest

from trr_backend.repositories import admin_typography

SET_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ASSIGNMENT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def _roles() -> dict[str, object]:
    return {
        "body": {
            "mobile": {
                "fontFamily": "var(--font-hamburg)",
                "fontSize": "16px",
                "fontWeight": "400",
                "lineHeight": "24px",
                "letterSpacing": "0px",
            },
            "desktop": {
                "fontFamily": "var(--font-hamburg)",
                "fontSize": "18px",
                "fontWeight": "400",
                "lineHeight": "28px",
                "letterSpacing": "0px",
            },
        }
    }


def _set() -> dict[str, object]:
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    return {
        "id": SET_ID,
        "slug": "admin-home",
        "name": "Admin Home",
        "area": "admin",
        "seed_source": "src/app/admin/page.tsx",
        "roles": _roles(),
        "created_at": now,
        "updated_at": now,
    }


def _assignment() -> dict[str, object]:
    now = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    return {
        "id": ASSIGNMENT_ID,
        "area": "admin",
        "page_key": "home",
        "instance_key": None,
        "set_id": SET_ID,
        "source_path": "src/app/admin/page.tsx",
        "notes": None,
        "created_at": now,
        "updated_at": now,
    }


def test_read_empty_state_returns_the_seeded_snapshot_without_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fetch_all(sql: str, *_args, **_kwargs):
        calls.append(sql)
        return []

    monkeypatch.setattr(admin_typography.pg, "fetch_all", fetch_all)
    monkeypatch.setattr(admin_typography.pg, "execute", lambda *_args, **_kwargs: pytest.fail("GET must not seed"))

    state, query_count = admin_typography.read_typography_state()

    assert query_count == 2
    assert state["sets"]
    assert state["assignments"]
    assert all("SELECT" in call for call in calls)


def test_read_missing_table_returns_seeded_snapshot_without_request_time_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    class MissingTypographyTableError(RuntimeError):
        code = "42P01"

    monkeypatch.setattr(
        admin_typography.pg,
        "fetch_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            MissingTypographyTableError('relation "site_typography_sets" does not exist')
        ),
    )
    state, query_count = admin_typography.read_typography_state()

    assert query_count == 2
    assert state["sets"][0]["id"] == "seed-set-1"


def test_create_seeds_only_for_authenticated_write_and_returns_mapped_set(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []
    monkeypatch.setattr(admin_typography, "seed_typography_if_missing", lambda: 28)

    def execute_returning(sql: str, params: list[object]):
        calls.append((sql, params))
        return [_set()]

    monkeypatch.setattr(admin_typography.pg, "execute_returning", execute_returning)

    created, query_count = admin_typography.create_typography_set(
        name=" Admin Home ",
        area="admin",
        seed_source=" src/app/admin/page.tsx ",
        roles=_roles(),
    )

    assert query_count == 29
    assert created["name"] == "Admin Home"
    assert created["roles"] == _roles()
    assert "INSERT INTO public.site_typography_sets" in calls[0][0]
    assert calls[0][1][:4] == ["admin-home", "Admin Home", "admin", "src/app/admin/page.tsx"]


def test_delete_returns_in_use_before_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(admin_typography, "resolve_typography_set_id", lambda _set_id: (SET_ID, 4))
    monkeypatch.setattr(admin_typography.pg, "fetch_one", lambda *_args, **_kwargs: {"count": "1"})
    monkeypatch.setattr(
        admin_typography.pg,
        "execute_returning",
        lambda *_args, **_kwargs: pytest.fail("assigned typography set must not be deleted"),
    )

    outcome, query_count = admin_typography.delete_typography_set(SET_ID)

    assert outcome == "in-use"
    assert query_count == 5


def test_assignment_upsert_preserves_nullable_scope_and_maps_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(admin_typography, "resolve_typography_set_id", lambda _set_id: (SET_ID, 4))
    monkeypatch.setattr(admin_typography.pg, "fetch_one", lambda *_args, **_kwargs: None)
    calls: list[tuple[str, list[object]]] = []

    def execute_returning(sql: str, params: list[object]):
        calls.append((sql, params))
        return [_assignment()]

    monkeypatch.setattr(admin_typography.pg, "execute_returning", execute_returning)

    assignment, query_count = admin_typography.upsert_typography_assignment(
        area="admin",
        page_key="home",
        instance_key=None,
        set_id=SET_ID,
        source_path=" src/app/admin/page.tsx ",
        notes=None,
    )

    assert query_count == 6
    assert assignment["set_id"] == SET_ID
    assert "INSERT INTO public.site_typography_assignments" in calls[0][0]
    assert calls[0][1] == ["admin", "home", None, SET_ID, "src/app/admin/page.tsx", None]
