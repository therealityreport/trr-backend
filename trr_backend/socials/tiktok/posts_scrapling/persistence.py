"""Persistence adapter for the TikTok posts Scrapling lane.

Converts raw TikTok API itemList entries into TikTokPost-compatible DTOs,
then persists through the canonical _upsert_tiktok_post() repo helper.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("socials.tiktok.posts_scrapling.persistence")


@dataclass(slots=True)
class PersistedTikTokPosts:
    posts_upserted: int
    posts_skipped: int
    posts_skipped_by_reason: dict[str, int] = field(default_factory=dict)
    catalog_posts_upserted: int = 0
    required_catalog_upsert_failures: int = 0


@dataclass
class _ScraplingTikTokPostDTO:
    """DTO satisfying _upsert_tiktok_post's getattr() contract.

    Fields match TikTokPost (scraper.py:198-237).
    """

    video_id: str
    date_time: str
    create_time: int
    description: str
    hashtags: list[str]
    mentions: list[str]
    likes: int
    comments: int
    shares: int
    saves: int
    views: int
    url: str
    username: str
    author_nickname: str
    duration: int
    music_title: str
    music_author: str
    user_avatar_url: str | None = None
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    _raw_item: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_item)


def _tiktok_item_to_post_dto(item: dict[str, Any], *, account_handle: str) -> _ScraplingTikTokPostDTO:
    """Convert raw TikTok API item to a DTO that _upsert_tiktok_post can read."""
    from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper

    parser = TikTokScraper(cookies={})
    parse_item = dict(item)
    if not parse_item.get("hashtags") and isinstance(parse_item.get("challenges"), list):
        parse_item["hashtags"] = [
            {"name": str(challenge.get("title") or "").strip()}
            for challenge in parse_item.get("challenges") or []
            if isinstance(challenge, dict) and str(challenge.get("title") or "").strip()
        ]
    parsed = parser._parse_post_item(  # noqa: SLF001
        parse_item,
        TikTokScrapeConfig(username=account_handle),
    )

    return _ScraplingTikTokPostDTO(
        video_id=parsed.video_id,
        date_time=parsed.date_time,
        create_time=parsed.create_time,
        description=parsed.description,
        hashtags=list(parsed.hashtags or []),
        mentions=list(parsed.mentions or []),
        likes=parsed.likes,
        comments=parsed.comments,
        shares=parsed.shares,
        saves=parsed.saves,
        views=parsed.views,
        url=parsed.url,
        username=parsed.username,
        author_nickname=parsed.author_nickname,
        duration=parsed.duration,
        music_title=parsed.music_title,
        music_author=parsed.music_author,
        user_avatar_url=parsed.user_avatar_url,
        media_urls=list(parsed.media_urls or []),
        thumbnail_url=parsed.thumbnail_url,
        _raw_item=item,
    )


def _savepoint_name(*, index: int) -> str:
    return f"tiktok_posts_scrapling_post_{max(1, int(index))}"


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


def _persist_tiktok_post_dtos(
    *,
    account_handle: str,
    posts: list[Any],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
    pipeline_ingest_mode: str | None = None,
) -> PersistedTikTokPosts:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo

    context = repo.get_season_context(season_id) if season_id else None
    shared_catalog_mode = str(pipeline_ingest_mode or "").strip().lower() == "shared_account_catalog_backfill"
    posts_upserted = 0
    catalog_posts_upserted = 0
    required_catalog_upsert_failures = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        posts_skipped += 1
        posts_skipped_by_reason[reason] = int(posts_skipped_by_reason.get(reason) or 0) + 1

    def _record_required_catalog_upsert_failure() -> None:
        nonlocal required_catalog_upsert_failures
        if shared_catalog_mode:
            required_catalog_upsert_failures += 1

    with pg.db_connection(label="tiktok-posts-scrapling-sync") as conn:
        for index, post in enumerate(posts, start=1):
            video_id = str(getattr(post, "video_id", "") or "").strip()
            if not video_id:
                _record_skip("missing_video_id")
                continue
            canonical_row: dict[str, Any] | None = None
            catalog_row: dict[str, Any] | None = None
            try:
                with _PostSavepoint(conn=conn, index=index):
                    canonical_row = repo._upsert_tiktok_post(
                        context,
                        job_id=job_id,
                        account=account_handle,
                        post=post,
                        conn=conn,
                    )
                    if shared_catalog_mode:
                        catalog_row = repo._upsert_shared_catalog_post(
                            platform="tiktok",
                            run_id=run_id,
                            account_handle=account_handle,
                            post=post,
                            conn=conn,
                        )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to upsert TikTok post %s via canonical helper", video_id)
                _record_skip("upsert_failed")
                _record_required_catalog_upsert_failure()
                continue
            if canonical_row:
                posts_upserted += 1
            else:
                _record_skip("canonical_upsert_returned_none")
                _record_required_catalog_upsert_failure()
            if shared_catalog_mode:
                if catalog_row:
                    catalog_posts_upserted += 1
                else:
                    _record_skip("catalog_upsert_returned_none")
                    _record_required_catalog_upsert_failure()

    return PersistedTikTokPosts(
        posts_upserted=posts_upserted,
        catalog_posts_upserted=catalog_posts_upserted,
        required_catalog_upsert_failures=required_catalog_upsert_failures,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )


def persist_tiktok_post_dtos(
    *,
    account_handle: str,
    posts: list[Any],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
    pipeline_ingest_mode: str | None = None,
) -> PersistedTikTokPosts:
    """Persist already-adapted TikTokPost-compatible objects."""
    return _persist_tiktok_post_dtos(
        account_handle=account_handle,
        posts=posts,
        run_id=run_id,
        job_id=job_id,
        season_id=season_id,
        pipeline_ingest_mode=pipeline_ingest_mode,
    )


def persist_tiktok_posts(
    *,
    account_handle: str,
    post_items: list[dict[str, Any]],
    run_id: str | None,
    job_id: str | None,
    season_id: str | None = None,
    pipeline_ingest_mode: str | None = None,
) -> PersistedTikTokPosts:
    """Adapt raw TikTok API items and persist through canonical repo helper."""
    posts: list[_ScraplingTikTokPostDTO] = []
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}

    def _record_skip(reason: str) -> None:
        nonlocal posts_skipped
        posts_skipped += 1
        posts_skipped_by_reason[reason] = int(posts_skipped_by_reason.get(reason) or 0) + 1

    for item in post_items:
        if not isinstance(item, dict):
            _record_skip("invalid_item")
            continue
        try:
            dto = _tiktok_item_to_post_dto(item, account_handle=account_handle)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to adapt TikTok post item via canonical parser")
            _record_skip("adapt_failed")
            continue
        if not str(dto.video_id or "").strip():
            _record_skip("missing_video_id")
            continue
        posts.append(dto)

    persisted = _persist_tiktok_post_dtos(
        account_handle=account_handle,
        posts=posts,
        run_id=run_id,
        job_id=job_id,
        season_id=season_id,
        pipeline_ingest_mode=pipeline_ingest_mode,
    )
    posts_skipped += persisted.posts_skipped
    for reason, count in persisted.posts_skipped_by_reason.items():
        posts_skipped_by_reason[reason] = int(posts_skipped_by_reason.get(reason) or 0) + int(count or 0)

    return PersistedTikTokPosts(
        posts_upserted=persisted.posts_upserted,
        catalog_posts_upserted=persisted.catalog_posts_upserted,
        required_catalog_upsert_failures=persisted.required_catalog_upsert_failures,
        posts_skipped=posts_skipped,
        posts_skipped_by_reason=posts_skipped_by_reason,
    )
