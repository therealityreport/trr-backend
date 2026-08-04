"""Version-neutral season cast survey-role service."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from trr_backend.repositories import season_cast_survey_roles as roles_repo
from trr_backend.repositories.season_cast_survey_roles import SeasonSurveyCastRole


def list_roles(*, show_id: str, season_number: int) -> tuple[list[dict[str, Any]], int]:
    return roles_repo.list_roles(show_id=show_id, season_number=season_number)


def upsert_role(
    *,
    show_id: str,
    season_number: int,
    person_id: str,
    role: SeasonSurveyCastRole,
) -> tuple[dict[str, Any], int]:
    return roles_repo.upsert_role(
        show_id=show_id,
        season_number=season_number,
        person_id=person_id,
        role=role,
    )


def delete_role(
    *,
    show_id: str,
    season_number: int,
    person_id: str,
) -> tuple[bool, int]:
    return roles_repo.delete_role(
        show_id=show_id,
        season_number=season_number,
        person_id=person_id,
    )


def replace_roles(
    *,
    show_id: str,
    season_number: int,
    roles: Sequence[tuple[str, SeasonSurveyCastRole]],
) -> tuple[list[dict[str, Any]], int]:
    return roles_repo.replace_roles(
        show_id=show_id,
        season_number=season_number,
        roles=roles,
    )
