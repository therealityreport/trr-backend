"""Deterministic public identity resolution over canonical and direct aliases."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from trr_backend.repositories import public_identities as identity_repo

_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-+[a-z0-9]+)*$")
_MAX_SLUG_LENGTH = 160
_SEASON_NUMBER_RE = re.compile(r"^[0-9]+$")
_MAX_SEASON_NUMBER = 2_147_483_647


@dataclass(slots=True)
class IdentityResolutionError(RuntimeError):
    code: str
    status: int
    message: str
    detail: dict[str, Any]

    def __str__(self) -> str:
        return self.message


def _invalid_slug(resource_type: str, value: object) -> IdentityResolutionError:
    return IdentityResolutionError(
        code="INVALID_IDENTITY_SLUG",
        status=400,
        message="Identity slugs must contain only letters, numbers, and hyphens.",
        detail={"resource_type": resource_type, "slug": str(value or "").strip()},
    )


def normalize_slug(value: object, *, resource_type: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or len(normalized) > _MAX_SLUG_LENGTH or _SLUG_RE.fullmatch(normalized) is None:
        raise _invalid_slug(resource_type, value)
    return normalized


def normalize_show_id(value: object) -> str:
    raw_value = str(value or "").strip()
    try:
        parsed = UUID(raw_value)
    except (AttributeError, TypeError, ValueError):
        parsed = None
    if parsed is None:
        raise IdentityResolutionError(
            code="INVALID_IDENTITY_CONTEXT",
            status=400,
            message="show_id must be a valid UUID.",
            detail={"resource_type": "person", "field": "show_id", "value": raw_value},
        )
    return str(parsed)


def normalize_season_number(value: object) -> int:
    raw_value = str(value or "").strip()
    if isinstance(value, bool) or _SEASON_NUMBER_RE.fullmatch(raw_value) is None:
        raise IdentityResolutionError(
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be a non-negative integer.",
            detail={"resource_type": "season", "field": "season_number", "value": raw_value},
        )
    parsed = int(raw_value)
    if parsed > _MAX_SEASON_NUMBER:
        raise IdentityResolutionError(
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number is outside the supported integer range.",
            detail={"resource_type": "season", "field": "season_number", "value": raw_value},
        )
    return parsed


def _not_found(resource_type: str, **detail: Any) -> IdentityResolutionError:
    return IdentityResolutionError(
        code="IDENTITY_NOT_FOUND",
        status=404,
        message=f"The requested {resource_type} identity was not found.",
        detail={"resource_type": resource_type, **detail},
    )


def _candidate_summary(resource_type: str, row: dict[str, Any]) -> dict[str, str]:
    identity_key = f"{resource_type}_id"
    return {
        identity_key: str(row[identity_key]),
        "canonical_slug": str(row["canonical_slug"]),
    }


def _select_alias_candidate(
    *,
    resource_type: str,
    requested_slug: str,
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    if not candidates:
        raise _not_found(resource_type, slug=requested_slug)

    canonical_matches = [row for row in candidates if bool(row.get("matched_is_canonical"))]
    if len(canonical_matches) == 1:
        return canonical_matches[0], "canonical"
    if len(canonical_matches) > 1:
        raise RuntimeError(f"multiple canonical {resource_type} identities matched one globally unique slug")
    if len(candidates) == 1:
        return candidates[0], "alias"

    raise IdentityResolutionError(
        code="IDENTITY_AMBIGUOUS",
        status=409,
        message=f"The requested {resource_type} alias matches multiple identities.",
        detail={
            "resource_type": resource_type,
            "slug": requested_slug,
            "candidate_count": len(candidates),
            "candidates": [_candidate_summary(resource_type, row) for row in candidates],
        },
    )


def resolve_show(slug: object) -> dict[str, Any]:
    requested_slug = normalize_slug(slug, resource_type="show")
    candidate, match_kind = _select_alias_candidate(
        resource_type="show",
        requested_slug=requested_slug,
        candidates=identity_repo.list_show_slug_candidates(requested_slug),
    )
    canonical_slug = str(candidate["canonical_slug"])
    return {
        "resource_type": "show",
        "show_id": candidate["show_id"],
        "show_name": candidate["show_name"],
        "requested_slug": requested_slug,
        "canonical_slug": canonical_slug,
        "match_kind": match_kind,
        "canonical_path": f"/shows/{canonical_slug}",
    }


def _resolve_show_context_by_id(show_id: str) -> dict[str, Any]:
    show = identity_repo.get_show_identity_by_id(show_id)
    if show is None:
        raise _not_found("show", show_id=show_id)
    return show


def resolve_season(*, show_slug: object, season_number: object) -> dict[str, Any]:
    normalized_season_number = normalize_season_number(season_number)
    show = resolve_show(show_slug)
    season = identity_repo.get_season_identity(
        show_id=str(show["show_id"]),
        season_number=normalized_season_number,
    )
    if season is None:
        raise _not_found(
            "season",
            show_id=show["show_id"],
            canonical_show_slug=show["canonical_slug"],
            season_number=normalized_season_number,
        )

    canonical_show_slug = str(show["canonical_slug"])
    return {
        "resource_type": "season",
        "season_id": season["season_id"],
        "show_id": season["show_id"],
        "show_name": show["show_name"],
        "season_number": season["season_number"],
        "season_title": season["season_title"],
        "requested_show_slug": show["requested_slug"],
        "canonical_show_slug": canonical_show_slug,
        "show_match_kind": show["match_kind"],
        "canonical_path": f"/shows/{canonical_show_slug}/seasons/{season['season_number']}",
    }


def resolve_person(
    slug: object,
    *,
    show_id: object | None = None,
    show_slug: object | None = None,
) -> dict[str, Any]:
    requested_slug = normalize_slug(slug, resource_type="person")
    if show_id is not None and show_slug is not None:
        raise IdentityResolutionError(
            code="INVALID_IDENTITY_CONTEXT",
            status=400,
            message="Provide either show_id or show_slug, not both.",
            detail={"resource_type": "person", "conflicting_fields": ["show_id", "show_slug"]},
        )

    show_context: dict[str, Any] | None = None
    if show_slug is not None:
        show_context = resolve_show(show_slug)
    elif show_id is not None:
        show_context = _resolve_show_context_by_id(normalize_show_id(show_id))

    context_show_id = str(show_context["show_id"]) if show_context is not None else None
    candidate, match_kind = _select_alias_candidate(
        resource_type="person",
        requested_slug=requested_slug,
        candidates=identity_repo.list_person_slug_candidates(slug=requested_slug, show_id=context_show_id),
    )
    canonical_slug = str(candidate["canonical_slug"])
    payload: dict[str, Any] = {
        "resource_type": "person",
        "person_id": candidate["person_id"],
        "full_name": candidate["full_name"],
        "requested_slug": requested_slug,
        "canonical_slug": canonical_slug,
        "match_kind": match_kind,
        "canonical_path": f"/people/{canonical_slug}",
        "show_context": None,
    }
    if show_context is not None:
        payload["show_context"] = {
            "show_id": show_context["show_id"],
            "show_name": show_context["show_name"],
            "canonical_slug": show_context["canonical_slug"],
        }
    return payload
