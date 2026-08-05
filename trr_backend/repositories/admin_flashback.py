"""Backend-owned persistence for Flashback quiz administration."""

from __future__ import annotations

from typing import Any

from trr_backend.db import pg

_QUIZ_COLUMNS = """
  id::text AS id,
  title,
  publish_date::text AS publish_date,
  description,
  is_published,
  created_at,
  updated_at
"""

_EVENT_COLUMNS = """
  id::text AS id,
  quiz_id::text AS quiz_id,
  description,
  image_url,
  year,
  sort_order,
  point_value
"""


def list_quizzes() -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(
        f"""
        SELECT {_QUIZ_COLUMNS}
        FROM public.flashback_quizzes
        ORDER BY publish_date DESC, created_at DESC
        """
    )
    return rows, 1


def create_quiz(
    *,
    title: str,
    publish_date: str,
    description: str | None,
) -> tuple[dict[str, Any], int]:
    rows = pg.execute_returning(
        f"""
        INSERT INTO public.flashback_quizzes (
          title,
          publish_date,
          description,
          is_published
        )
        VALUES (%s, %s::date, %s, false)
        RETURNING {_QUIZ_COLUMNS}
        """,
        [title, publish_date, description],
    )
    if not rows:
        raise RuntimeError("Failed to load the Flashback quiz after creation")
    return rows[0], 1


def set_quiz_published(
    *,
    quiz_id: str,
    is_published: bool,
) -> tuple[dict[str, Any] | None, int]:
    rows = pg.execute_returning(
        f"""
        UPDATE public.flashback_quizzes
        SET is_published = %s,
            updated_at = now()
        WHERE id = %s::uuid
        RETURNING {_QUIZ_COLUMNS}
        """,
        [is_published, quiz_id],
    )
    return (rows[0] if rows else None), 1


def list_events(*, quiz_id: str) -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(
        f"""
        SELECT {_EVENT_COLUMNS}
        FROM public.flashback_events
        WHERE quiz_id = %s::uuid
        ORDER BY sort_order ASC
        """,
        [quiz_id],
    )
    return rows, 1


def create_event(
    *,
    quiz_id: str,
    description: str,
    year: int,
    image_url: str | None,
    point_value: int,
) -> tuple[dict[str, Any] | None, int]:
    """Create an event while serializing sort-order allocation per quiz."""

    with pg.db_connection(label="create-admin-flashback-event") as conn:
        quiz = pg.fetch_one(
            """
            SELECT id::text AS id
            FROM public.flashback_quizzes
            WHERE id = %s::uuid
            FOR UPDATE
            """,
            [quiz_id],
            conn=conn,
        )
        if quiz is None:
            return None, 1

        sort_order_row = pg.fetch_one(
            """
            SELECT COALESCE(MAX(sort_order), 0)::int AS max_sort_order
            FROM public.flashback_events
            WHERE quiz_id = %s::uuid
            """,
            [quiz_id],
            conn=conn,
        )
        next_sort_order = int((sort_order_row or {}).get("max_sort_order") or 0) + 1

        rows = pg.execute_returning(
            f"""
            INSERT INTO public.flashback_events (
              quiz_id,
              description,
              year,
              image_url,
              point_value,
              sort_order
            )
            VALUES (%s::uuid, %s, %s::int, %s, %s::int, %s::int)
            RETURNING {_EVENT_COLUMNS}
            """,
            [quiz_id, description, year, image_url, point_value, next_sort_order],
            conn=conn,
        )
        if not rows:
            raise RuntimeError("Failed to load the Flashback event after creation")
        return rows[0], 3


def delete_event(*, event_id: str) -> tuple[bool, int]:
    """Delete and compact an event under the same per-quiz row lock as create."""

    with pg.db_connection(label="delete-admin-flashback-event") as conn:
        # Lock the parent quiz first. Create uses the same lock before computing
        # MAX(sort_order), so create/delete cannot allocate or compact concurrently.
        parent = pg.fetch_one(
            """
            SELECT quiz.id::text AS quiz_id
            FROM public.flashback_quizzes AS quiz
            JOIN public.flashback_events AS event ON event.quiz_id = quiz.id
            WHERE event.id = %s::uuid
            FOR UPDATE OF quiz
            """,
            [event_id],
            conn=conn,
        )
        if parent is None:
            return False, 1

        event = pg.fetch_one(
            """
            SELECT quiz_id::text AS quiz_id, sort_order
            FROM public.flashback_events
            WHERE id = %s::uuid
            FOR UPDATE
            """,
            [event_id],
            conn=conn,
        )
        if event is None:
            return False, 2

        pg.execute(
            "DELETE FROM public.flashback_events WHERE id = %s::uuid",
            [event_id],
            conn=conn,
        )
        pg.execute(
            """
            UPDATE public.flashback_events
            SET sort_order = sort_order - 1
            WHERE quiz_id = %s::uuid
              AND sort_order > %s::int
            """,
            [event["quiz_id"], event["sort_order"]],
            conn=conn,
        )
        return True, 4
