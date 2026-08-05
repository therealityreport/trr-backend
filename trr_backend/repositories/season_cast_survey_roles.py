"""Backend-owned persistence for season cast survey roles."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

from trr_backend.db import pg

SeasonSurveyCastRole = Literal["main", "friend_of"]

_RETURNING_COLUMNS = """
  id::text AS id,
  trr_show_id::text AS trr_show_id,
  season_number,
  person_id::text AS person_id,
  role,
  created_at,
  updated_at
"""


def list_roles(*, show_id: str, season_number: int) -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(
        f"""
        SELECT {_RETURNING_COLUMNS}
        FROM admin.season_cast_survey_roles
        WHERE trr_show_id = %s::uuid
          AND season_number = %s::int
        ORDER BY role ASC, created_at ASC, id ASC
        """,
        [show_id, season_number],
    )
    return rows, 1


def upsert_role(
    *,
    show_id: str,
    season_number: int,
    person_id: str,
    role: SeasonSurveyCastRole,
) -> tuple[dict[str, Any], int]:
    rows = pg.execute_returning(
        f"""
        INSERT INTO admin.season_cast_survey_roles (
          trr_show_id,
          season_number,
          person_id,
          role
        )
        VALUES (%s::uuid, %s::int, %s::uuid, %s)
        ON CONFLICT (trr_show_id, season_number, person_id)
        DO UPDATE SET role = EXCLUDED.role
        RETURNING {_RETURNING_COLUMNS}
        """,
        [show_id, season_number, person_id, role],
    )
    if not rows:
        raise RuntimeError("Failed to load the season cast survey role after upsert")
    return rows[0], 1


def delete_role(
    *,
    show_id: str,
    season_number: int,
    person_id: str,
) -> tuple[bool, int]:
    rows = pg.execute_returning(
        """
        DELETE FROM admin.season_cast_survey_roles
        WHERE trr_show_id = %s::uuid
          AND season_number = %s::int
          AND person_id = %s::uuid
        RETURNING id::text AS id
        """,
        [show_id, season_number, person_id],
    )
    return bool(rows), 1


def replace_roles(
    *,
    show_id: str,
    season_number: int,
    roles: Sequence[tuple[str, SeasonSurveyCastRole]],
) -> tuple[list[dict[str, Any]], int]:
    with pg.db_connection(label="replace-season-cast-survey-roles") as conn:
        pg.execute(
            """
            DELETE FROM admin.season_cast_survey_roles
            WHERE trr_show_id = %s::uuid
              AND season_number = %s::int
            """,
            [show_id, season_number],
            conn=conn,
        )
        if not roles:
            return [], 1

        values = [(show_id, season_number, person_id, role) for person_id, role in roles]
        rows = pg.execute_values_returning(
            f"""
            INSERT INTO admin.season_cast_survey_roles (
              trr_show_id,
              season_number,
              person_id,
              role
            )
            VALUES %s
            ON CONFLICT (trr_show_id, season_number, person_id)
            DO UPDATE SET role = EXCLUDED.role
            RETURNING {_RETURNING_COLUMNS}
            """,
            values,
            conn=conn,
        )
    return rows, 2
