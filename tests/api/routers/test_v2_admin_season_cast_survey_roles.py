from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.services import season_cast_survey_roles as roles_service

SHOW_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
PERSON_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
OTHER_PERSON_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
ROLE_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
PATH = f"/api/v2/admin/shows/{SHOW_ID}/seasons/3/cast-survey-roles"


def _role(person_id: str = PERSON_ID, role: str = "main") -> dict[str, object]:
    return {
        "id": ROLE_ID,
        "trr_show_id": SHOW_ID,
        "season_number": 3,
        "person_id": person_id,
        "role": role,
        "created_at": datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
    }


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_get_preserves_the_existing_row_envelope_without_requiring_a_core_season(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_list(**kwargs):
        captured.update(kwargs)
        return [_role()], 1

    monkeypatch.setattr(roles_service.roles_repo, "list_roles", fake_list)

    response = TestClient(app).get(PATH)

    assert response.status_code == 200
    assert response.json() == {
        "roles": [
            {
                **_role(),
                "created_at": "2026-07-15T12:00:00Z",
                "updated_at": "2026-07-16T12:00:00Z",
            }
        ]
    }
    assert captured == {"show_id": SHOW_ID, "season_number": 3}


def test_get_preserves_the_prior_empty_result_for_a_non_positive_season_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(roles_service.roles_repo, "list_roles", lambda **_kwargs: ([], 1))

    response = TestClient(app).get(f"/api/v2/admin/shows/{SHOW_ID}/seasons/0/cast-survey-roles")

    assert response.status_code == 200
    assert response.json() == {"roles": []}


def test_post_patch_and_delete_preserve_write_capabilities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_upsert(**kwargs):
        captured["upsert"] = kwargs
        return _role(), 1

    def fake_replace(**kwargs):
        captured["replace"] = kwargs
        return [_role(OTHER_PERSON_ID, "friend_of")], 2

    def fake_delete(**kwargs):
        captured["delete"] = kwargs
        return False, 1

    monkeypatch.setattr(roles_service.roles_repo, "upsert_role", fake_upsert)
    monkeypatch.setattr(roles_service.roles_repo, "replace_roles", fake_replace)
    monkeypatch.setattr(roles_service.roles_repo, "delete_role", fake_delete)
    client = TestClient(app)

    upserted = client.post(PATH, json={"person_id": PERSON_ID, "role": "main"})
    replaced = client.patch(
        PATH,
        json={"roles": [{"person_id": OTHER_PERSON_ID, "role": "friend_of"}]},
    )
    deleted = client.request("DELETE", PATH, json={"person_id": PERSON_ID})

    assert upserted.status_code == 200
    assert upserted.json()["role"]["person_id"] == PERSON_ID
    assert replaced.status_code == 200
    assert replaced.json()["roles"][0]["role"] == "friend_of"
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True, "removed": False}
    assert captured == {
        "upsert": {
            "show_id": SHOW_ID,
            "season_number": 3,
            "person_id": PERSON_ID,
            "role": "main",
        },
        "replace": {
            "show_id": SHOW_ID,
            "season_number": 3,
            "roles": [(OTHER_PERSON_ID, "friend_of")],
        },
        "delete": {
            "show_id": SHOW_ID,
            "season_number": 3,
            "person_id": PERSON_ID,
        },
    }


@pytest.mark.parametrize(
    ("method", "path", "body", "expected_code"),
    [
        ("GET", "/api/v2/admin/shows/not-a-uuid/seasons/3/cast-survey-roles", None, "INVALID_SHOW_ID"),
        (
            "GET",
            f"/api/v2/admin/shows/{SHOW_ID}/seasons/not-an-integer/cast-survey-roles",
            None,
            "INVALID_SEASON_NUMBER",
        ),
        ("POST", PATH, {"person_id": PERSON_ID, "role": "guest"}, "INVALID_SEASON_CAST_SURVEY_ROLE_REQUEST"),
        ("PATCH", PATH, {"roles": [{"person_id": "bad", "role": "main"}]}, "INVALID_SEASON_CAST_SURVEY_ROLES_REQUEST"),
        (
            "PATCH",
            PATH,
            {
                "roles": [
                    {"person_id": PERSON_ID, "role": "main"},
                    {"person_id": PERSON_ID, "role": "friend_of"},
                ]
            },
            "INVALID_SEASON_CAST_SURVEY_ROLES_REQUEST",
        ),
        ("DELETE", PATH, {"person_id": PERSON_ID, "extra": True}, "INVALID_SEASON_CAST_SURVEY_ROLE_DELETE_REQUEST"),
    ],
)
def test_malformed_inputs_use_stable_problem_400(
    method: str,
    path: str,
    body: dict[str, object] | None,
    expected_code: str,
) -> None:
    response = TestClient(app).request(method, path, json=body)

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code


def test_database_capacity_error_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    def unavailable(**_kwargs):
        raise DatabaseServiceUnavailableError("secret topology", reason="pool_capacity")

    monkeypatch.setattr(roles_service.roles_repo, "list_roles", unavailable)

    response = TestClient(app).get(PATH)

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret topology" not in response.text


def test_openapi_is_explicit_bounded_and_internal_admin_secured() -> None:
    operation_set = app.openapi()["paths"]["/api/v2/admin/shows/{show_id}/seasons/{season_number}/cast-survey-roles"]

    assert {method: operation_set[method]["operationId"] for method in ("get", "post", "patch", "delete")} == {
        "get": "listAdminSeasonCastSurveyRolesV2",
        "post": "upsertAdminSeasonCastSurveyRoleV2",
        "patch": "replaceAdminSeasonCastSurveyRolesV2",
        "delete": "deleteAdminSeasonCastSurveyRoleV2",
    }
    for method in ("get", "post", "patch", "delete"):
        operation = operation_set[method]
        assert "422" not in operation["responses"]
        assert {"400", "500", "503"}.issubset(operation["responses"])
        assert operation["security"] == [{"InternalAdminBearer": []}]
