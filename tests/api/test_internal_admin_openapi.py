from __future__ import annotations

from api.main import app

HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
INTERNAL_ADMIN_SECURITY = [{"InternalAdminBearer": []}]


def test_internal_admin_bearer_contract_is_explicit_for_v2_admin_routes() -> None:
    schema = app.openapi()

    assert schema["components"]["securitySchemes"]["InternalAdminBearer"] == {
        "type": "http",
        "description": (
            "An allowlisted Supabase admin JWT or a TRR-signed internal-admin JWT. "
            "The backend applies the same InternalAdminUser authorization policy to both."
        ),
        "scheme": "bearer",
        "bearerFormat": "JWT",
    }
    for path, path_item in schema["paths"].items():
        if not path.startswith("/api/v2/admin/"):
            continue
        for method, operation in path_item.items():
            if method in HTTP_METHODS:
                assert operation["security"] == INTERNAL_ADMIN_SECURITY, f"{method.upper()} {path}"


def test_public_v2_identity_routes_do_not_claim_internal_admin_auth() -> None:
    schema = app.openapi()

    for path, path_item in schema["paths"].items():
        if not path.startswith("/api/v2/identities/"):
            continue
        for method, operation in path_item.items():
            if method in HTTP_METHODS:
                assert "security" not in operation, f"{method.upper()} {path}"
