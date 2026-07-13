from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("socials.threads.posts_scrapling.persistence")


@dataclass(slots=True)
class PersistedThreadsPosts:
    posts_upserted: int
    posts_skipped: int
    catalog_posts_upserted: int = 0
    required_shared_persistence_failures: int = 0
    posts_skipped_by_reason: dict[str, int] = field(default_factory=dict)


def _savepoint_name(*, index: int) -> str:
    return f"threads_posts_scrapling_post_{max(1, int(index))}"


def _execute_savepoint_statement(conn: Any, statement: str) -> None:
    with conn.cursor() as cur:
        cur.execute(statement)


class _PostSavepoint:
    def __init__(self, *, conn: Any, index: int) -> None:
        self._conn = conn
        self._name = _savepoint_name(index=index)

    def __enter__(self) -> None:
        _execute_savepoint_statement(self._conn, f"SAVEPOINT {self._name}")

    def __exit__(self, exc_type, _exc, _tb) -> bool:  # type: ignore[no-untyped-def]
        if exc_type is not None:
            _execute_savepoint_statement(self._conn, f"ROLLBACK TO SAVEPOINT {self._name}")
        _execute_savepoint_statement(self._conn, f"RELEASE SAVEPOINT {self._name}")
        return False


def persist_threads_posts(
    *,
    account_handle: str,
    posts: list[Any],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
    pipeline_ingest_mode: str | None = None,
) -> PersistedThreadsPosts:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    shared_catalog_mode = str(pipeline_ingest_mode or "").strip().lower() == "shared_account_catalog_backfill"
    posts_upserted = 0
    catalog_posts_upserted = 0
    required_shared_persistence_failures = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        normalized_reason = str(reason or "").strip() or "unknown"
        posts_skipped += 1
        posts_skipped_by_reason[normalized_reason] = int(posts_skipped_by_reason.get(normalized_reason) or 0) + 1

    def _record_required_shared_persistence_failure() -> None:
        nonlocal required_shared_persistence_failures
        if shared_catalog_mode:
            required_shared_persistence_failures += 1

    with pg.db_connection(label="threads-posts-scrapling-sync") as conn:
        for index, post in enumerate(posts, start=1):
            post_id = str(getattr(post, "post_id", "") or "").strip()
            if not post_id:
                _record_skip("missing_post_id")
                continue
            row: dict[str, Any] | None = None
            catalog_row: dict[str, Any] | None = None
            try:
                with _PostSavepoint(conn=conn, index=index):
                    row = repo._upsert_meta_threads_post(
                        context,
                        job_id=job_id,
                        account=account_handle,
                        post=post,
                        conn=conn,
                    )
                    if shared_catalog_mode:
                        catalog_row = repo._upsert_shared_catalog_post(
                            platform="threads",
                            run_id=run_id,
                            account_handle=account_handle,
                            post=post,
                            conn=conn,
                        )
            except Exception:
                logger.exception("Failed to upsert Threads post %s via canonical helper", post_id)
                _record_skip("upsert_failed")
                _record_required_shared_persistence_failure()
                continue
            if row:
                posts_upserted += 1
            else:
                _record_skip("canonical_upsert_returned_none")
                _record_required_shared_persistence_failure()
            if shared_catalog_mode:
                if catalog_row:
                    catalog_posts_upserted += 1
                else:
                    _record_skip("catalog_upsert_returned_none")
                    _record_required_shared_persistence_failure()

    return PersistedThreadsPosts(
        posts_upserted=posts_upserted,
        catalog_posts_upserted=catalog_posts_upserted,
        required_shared_persistence_failures=required_shared_persistence_failures,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
