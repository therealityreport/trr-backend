from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("socials.threads.posts_scrapling.persistence")


@dataclass(slots=True)
class PersistedThreadsPosts:
    posts_upserted: int
    posts_skipped: int


def persist_threads_posts(
    *,
    account_handle: str,
    posts: list[Any],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
) -> PersistedThreadsPosts:
    del run_id

    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    posts_upserted = 0
    posts_skipped = 0

    with pg.db_connection(label="threads-posts-scrapling-sync") as conn:
        for post in posts:
            post_id = str(getattr(post, "post_id", "") or "").strip()
            if not post_id:
                posts_skipped += 1
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
                    posts_skipped += 1
            except Exception:
                logger.exception("Failed to upsert Threads post %s via canonical helper", post_id)
                posts_skipped += 1

    return PersistedThreadsPosts(posts_upserted=posts_upserted, posts_skipped=posts_skipped)
