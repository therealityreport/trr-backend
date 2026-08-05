from __future__ import annotations

from scripts.dev.export_v2_openapi import build_v2_openapi, render_v2_openapi


def source_schema() -> dict:
    return {
        "openapi": "3.1.0",
        "info": {"title": "Example", "version": "1"},
        "paths": {
            "/api/v1/ignored": {
                "get": {
                    "responses": {
                        "200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Ignored"}}}}
                    }
                }
            },
            "/api/v2/example": {
                "get": {
                    "tags": ["v2"],
                    "security": [{"InternalAdmin": []}],
                    "responses": {
                        "200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Envelope"}}}}
                    },
                }
            },
        },
        "components": {
            "schemas": {
                "Envelope": {
                    "type": "object",
                    "properties": {"item": {"$ref": "#/components/schemas/Item"}},
                },
                "Item": {"type": "object", "properties": {"id": {"type": "string"}}},
                "Ignored": {"type": "object"},
            },
            "securitySchemes": {
                "InternalAdmin": {"type": "http", "scheme": "bearer"},
                "IgnoredAuth": {"type": "apiKey", "in": "header", "name": "ignored"},
            },
        },
        "tags": [{"name": "v1"}, {"name": "v2", "description": "Version 2"}],
    }


def test_export_keeps_only_v2_dependency_closure() -> None:
    result = build_v2_openapi(source_schema())

    assert list(result["paths"]) == ["/api/v2/example"]
    assert set(result["components"]["schemas"]) == {"Envelope", "Item"}
    assert set(result["components"]["securitySchemes"]) == {"InternalAdmin"}
    assert result["tags"] == [{"name": "v2", "description": "Version 2"}]


def test_render_is_byte_deterministic() -> None:
    first = render_v2_openapi(source_schema())
    second = render_v2_openapi(source_schema())

    assert first == second
    assert first.endswith("\n")
