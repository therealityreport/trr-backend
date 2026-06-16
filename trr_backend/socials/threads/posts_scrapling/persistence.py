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
    posts_skipped_by_reason: dict[str, int] = field(default_factory=dict)


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
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        normalized_reason = str(reason or "").strip() or "unknown"
        posts_skipped += 1
        posts_skipped_by_reason[normalized_reason] = int(posts_skipped_by_reason.get(normalized_reason) or 0) + 1

    with pg.db_connection(label="threads-posts-scrapling-sync") as conn:
        for post in posts:
            post_id = str(getattr(post, "post_id", "") or "").strip()
            if not post_id:
                _record_skip("missing_post_id")
                continue
            try:
                row = repo._upsert_meta_threads_post(
                    context,
                    job_id=job_id,
                    account=account_handle,
                    post=post,
                    conn=conn,
                )
                if row:
                    posts_upserted += 1
                else:
                    _record_skip("canonical_upsert_returned_none")
                if shared_catalog_mode:
                    catalog_row = repo._upsert_shared_catalog_post(
                        platform="threads",
                        run_id=run_id,
                        account_handle=account_handle,
                        post=post,
                        conn=conn,
                    )
                    if catalog_row:
                        catalog_posts_upserted += 1
                    else:
                        _record_skip("catalog_upsert_returned_none")
            except Exception:
                logger.exception("Failed to upsert Threads post %s via canonical helper", post_id)
                _record_skip("upsert_failed")

    return PersistedThreadsPosts(
        posts_upserted=posts_upserted,
        catalog_posts_upserted=catalog_posts_upserted,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
