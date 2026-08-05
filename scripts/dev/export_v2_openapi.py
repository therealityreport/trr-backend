#!/usr/bin/env python3
"""Export the deterministic, dependency-closed TRR `/api/v2` OpenAPI contract."""

from __future__ import annotations

import argparse
import json
from collections import deque
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = BACKEND_ROOT / "docs" / "api" / "openapi.v2.json"
COMPONENT_REF_PREFIX = "#/components/"


class OpenAPIExportError(ValueError):
    """Raised when the source OpenAPI document cannot produce a closed v2 contract."""


def _walk(value: Any):
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _component_references(value: Any) -> set[tuple[str, str]]:
    references: set[tuple[str, str]] = set()
    for candidate in _walk(value):
        if not isinstance(candidate, dict):
            continue
        reference = candidate.get("$ref")
        if not isinstance(reference, str) or not reference.startswith(COMPONENT_REF_PREFIX):
            continue
        parts = reference.removeprefix(COMPONENT_REF_PREFIX).split("/", 1)
        if len(parts) != 2 or not all(parts):
            raise OpenAPIExportError(f"unsupported component reference: {reference}")
        references.add((parts[0], parts[1]))
    return references


def _security_scheme_names(value: Any) -> set[str]:
    names: set[str] = set()
    for candidate in _walk(value):
        if not isinstance(candidate, dict) or "security" not in candidate:
            continue
        security = candidate["security"]
        if not isinstance(security, list):
            continue
        for requirement in security:
            if isinstance(requirement, dict):
                names.update(str(name) for name in requirement)
    return names


def build_v2_openapi(source: dict[str, Any]) -> dict[str, Any]:
    paths = source.get("paths")
    if not isinstance(paths, dict):
        raise OpenAPIExportError("source OpenAPI document has no paths object")
    selected_paths = {path: value for path, value in paths.items() if path.startswith("/api/v2/")}
    if not selected_paths:
        raise OpenAPIExportError("source OpenAPI document has no /api/v2 paths")

    source_components = source.get("components", {})
    if not isinstance(source_components, dict):
        raise OpenAPIExportError("source OpenAPI components must be an object")
    selected_components: dict[str, dict[str, Any]] = {}
    pending = deque(sorted(_component_references(selected_paths)))
    visited: set[tuple[str, str]] = set()
    while pending:
        group, name = pending.popleft()
        if (group, name) in visited:
            continue
        visited.add((group, name))
        group_values = source_components.get(group)
        if not isinstance(group_values, dict) or name not in group_values:
            raise OpenAPIExportError(f"missing referenced component: {group}/{name}")
        component = group_values[name]
        selected_components.setdefault(group, {})[name] = component
        pending.extend(sorted(_component_references(component) - visited))

    security_schemes = source_components.get("securitySchemes", {})
    for name in sorted(_security_scheme_names(selected_paths)):
        if not isinstance(security_schemes, dict) or name not in security_schemes:
            raise OpenAPIExportError(f"missing referenced security scheme: {name}")
        selected_components.setdefault("securitySchemes", {})[name] = security_schemes[name]

    used_tags = {
        tag
        for path_item in selected_paths.values()
        if isinstance(path_item, dict)
        for operation in path_item.values()
        if isinstance(operation, dict)
        for tag in operation.get("tags", [])
        if isinstance(tag, str)
    }
    selected_tags = [tag for tag in source.get("tags", []) if isinstance(tag, dict) and tag.get("name") in used_tags]

    document: dict[str, Any] = {
        "openapi": source.get("openapi", "3.1.0"),
        "info": source.get("info", {}),
        "paths": selected_paths,
    }
    if selected_components:
        document["components"] = selected_components
    if selected_tags:
        document["tags"] = selected_tags
    if "jsonSchemaDialect" in source:
        document["jsonSchemaDialect"] = source["jsonSchemaDialect"]
    return document


def render_v2_openapi(source: dict[str, Any]) -> str:
    return (
        json.dumps(
            build_v2_openapi(source),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    from api.main import app

    rendered = render_v2_openapi(app.openapi())
    output = args.output.resolve()
    if args.check:
        current = output.read_text(encoding="utf-8") if output.is_file() else None
        if current != rendered:
            print(f"v2-openapi: ERROR stale_or_missing={output}")
            return 1
        print(f"v2-openapi: OK path={output}")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(f"v2-openapi: generated path={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
