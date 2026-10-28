"""Incrementally persist anonymous TikTok listing pages in the catalog workflow."""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.control_plane.job_ownership import _job_claim_identity
from trr_backend.socials.tiktok.transports.public_profile import scrape_public_profile_posts


def scrape_catalog(
    *,
    core: Any,
    hydrate: Callable[..., Any],
    run_id: str | None,
    account_handle: str,
    config: Mapping[str, Any],
    job_id: str,
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from trr_backend.socials.tiktok.scraper import TikTokScraper

    claim_token, worker_id = _job_claim_identity(job_id, claim_token=config.get("claim_token"))
    last_heartbeat = 0.0

    def check_cancelled() -> None:
        nonlocal last_heartbeat
        state = pg.fetch_one(
            "select status from social.scrape_jobs where id=%s::uuid "
            "and (config->>'claim_token') is not distinct from %s::text "
            "and worker_id is not distinct from %s::text",
            [job_id, claim_token, worker_id],
        )
        if not state or state["status"] != "running":
            raise core.SharedStageRuntimeError(
                "TikTok listing cancelled or claim replaced",
                error_code="shared_account_posts_cancelled",
                retryable=False,
            )
        now = time.monotonic()
        if progress_cb and now - last_heartbeat >= 10:
            progress_cb({"phase": "fetch_tiktok_public_listing"})
            last_heartbeat = now

    state = pg.fetch_one("select config from social.scrape_jobs where id=%s::uuid", [job_id]) or {}
    checkpoint = dict((state.get("config") or {}).get("tiktok_public_checkpoint") or {})
    if not checkpoint and run_id:
        run = pg.fetch_one("select config from social.scrape_runs where id=%s::uuid", [run_id]) or {}
        checkpoint = dict((run.get("config") or {}).get("tiktok_public_checkpoint") or {})
    if checkpoint.get("account") != account_handle or checkpoint.get("version") != 2:
        checkpoint = {}
    scope = [str(config.get("date_start") or ""), str(config.get("date_end") or "")]
    if checkpoint.get("scope", scope) != scope:
        checkpoint = {}
    task_identity = {
        key: config.get(key)
        for key in (
            "selected_tasks",
            "effective_selected_tasks",
            "details_refresh_skip_media_followups",
            "source_scope",
        )
    }
    if checkpoint.get("task_identity") != task_identity:
        checkpoint = {}
    committed_ids = set(checkpoint.get("committed_ids") or [])
    saved_ids = set(checkpoint.get("saved_ids") or [])
    rows: list[dict[str, Any]] = []
    counters = {
        "posts_upserted": 0,
        "comments_upserted": 0,
        "materialized_posts_upserted": 0,
        "media_mirror_jobs_enqueued": 0,
    }
    scraper = TikTokScraper(cookies={}, direct_comment_api_enabled_override=False)
    # Defense in depth for jobs queued before the comments policy changed.
    safe_config = dict(config, tiktok_comments_in_posts_stage=False, tiktok_direct_comment_api_override=False)
    for key in ("selected_tasks", "effective_selected_tasks"):
        if key in safe_config:
            safe_config[key] = [task for task in safe_config[key] if task != "comments"]
    expected = core._shared_account_expected_total_posts_from_config(
        config, platform="tiktok", account_handle=account_handle
    )
    start = core._coerce_dt(config.get("date_start"))
    end = core._coerce_dt(config.get("date_end"))
    native_saved = len(committed_ids)

    def persist_page(posts: list[Any], page: dict[str, Any]) -> None:
        nonlocal native_saved, checkpoint
        check_cancelled()
        for post in posts:
            check_cancelled()
            if start and post.create_time < start.timestamp():
                continue
            if end and post.create_time > end.timestamp():
                continue
            row = core._upsert_shared_catalog_post(
                platform="tiktok", run_id=run_id, account_handle=account_handle, post=post
            )
            if not row:
                raise RuntimeError("TikTok catalog upsert returned no row")
            errors_before = counters.get("materialized_post_errors", 0)
            materialized = hydrate(
                scraper=scraper,
                run_id=run_id,
                account_handle=account_handle,
                config=safe_config,
                job_id=job_id,
                post=post,
                catalog_row=row,
                counters=counters,
                comment_fail_reasons=Counter(),
                progress_cb=progress_cb,
            )
            required_tasks = core._shared_tiktok_catalog_effective_tasks(safe_config)
            if (required_tasks & {"post_details", "media"} and not materialized) or (
                counters.get("materialized_post_errors", 0) != errors_before
            ):
                raise RuntimeError("TikTok post materialization failed; page checkpoint retained")
            rows.append(row)
            saved_ids.add(post.video_id)
        committed_ids.update(post.video_id for post in posts)
        native_saved = len(committed_ids)
        counters["posts_upserted"] = len(rows)
        checkpoint = {
            "version": 2,
            "task_identity": task_identity,
            "committed_ids": sorted(committed_ids),
            "saved_ids": sorted(saved_ids),
            "account": account_handle,
            "scope": scope,
            "ids": page["checkpoint_ids"] or checkpoint.get("ids", []),
            "saved_posts": len(saved_ids),
            "pages_scanned": page["pages_scanned"],
        }
        # Each upsert commits before this checkpoint; replay is idempotent if this write fails.
        updated = pg.fetch_one(
            "update social.scrape_jobs set config=coalesce(config,'{}'::jsonb) || "
            "jsonb_build_object('tiktok_public_checkpoint',%s::jsonb) "
            "where id=%s::uuid and status='running' "
            "and (config->>'claim_token') is not distinct from %s::text "
            "and worker_id is not distinct from %s::text returning id",
            [json.dumps(checkpoint), job_id, claim_token, worker_id],
        )
        if not updated:
            raise RuntimeError("TikTok checkpoint rejected after cancellation")
        if run_id:
            pg.fetch_one(
                "update social.scrape_runs set config=coalesce(config,'{}'::jsonb) || "
                "jsonb_build_object('tiktok_public_checkpoint',%s::jsonb) where id=%s::uuid "
                "and exists (select 1 from social.scrape_jobs where id=%s::uuid and status='running' "
                "and (config->>'claim_token') is not distinct from %s::text "
                "and worker_id is not distinct from %s::text) returning id",
                [json.dumps(checkpoint), run_id, job_id, claim_token, worker_id],
            )
        if progress_cb:
            progress_cb(
                {
                    "phase": "persist_catalog_posts",
                    "pages_scanned": page["pages_scanned"],
                    "posts_checked": native_saved,
                    "matched_posts": len(rows),
                    "saved_posts": len(saved_ids),
                    "total_posts": expected or None,
                }
            )

    budget = int(config.get("tiktok_public_max_pages") or 10000)
    _, metadata = scrape_public_profile_posts(
        account_handle=account_handle,
        max_pages=budget,
        page_size=30,
        on_page=persist_page,
        checkpoint_ids=checkpoint.get("ids", []),
        persisted_ids=sorted(committed_ids),
        check_cancelled=check_cancelled,
        max_seconds=1200,
    )
    metadata.update(
        persist_counters=counters,
        posts_checked=native_saved,
        total_posts=max(expected or 0, native_saved),
        source="public_browser",
        tiktok_public_checkpoint=checkpoint,
    )
    if metadata["partial"] or (not start and not end and expected and native_saved < expected):
        raise core.SharedStageRuntimeError(
            "TikTok listing is incomplete; committed pages are retained for resume",
            error_code="catalog_incomplete",
            retryable=True,
            runtime_metadata={"retrieval_meta": metadata},
        )
    return rows, metadata
