"""Durable episode discovery delivery, preserving the legacy result adapter."""

from __future__ import annotations

import hashlib
import random
import threading
import uuid
from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_episode as episode


def safe_snapshot(value: dict, conn: Any) -> dict:
    """Lock and filter source identities before storing episode snapshots."""
    from trr_backend.repositories import reddit_workflows as workflows

    ids: set[str] = set()

    def collect(node):
        if isinstance(node, dict):
            if node.get("reddit_post_id"):
                ids.add(str(node["reddit_post_id"]))
            for item in node.values():
                collect(item)
        elif isinstance(node, list):
            for item in node:
                collect(item)

    collect(value)
    workflows.lock_content(post_ids=sorted(ids), conn=conn)
    deleted = (
        {
            row["reddit_post_id"]
            for row in pg.fetch_all(
                "select reddit_post_id from social.reddit_lifecycle_enrollment "
                "where reddit_post_id=any(%s::text[]) and state in ('deleted','removed')",
                [sorted(ids)],
                conn=conn,
            )
        }
        if ids
        else set()
    )
    filtered = 0

    def clean(node: Any) -> Any:
        nonlocal filtered
        if isinstance(node, dict):
            if node.get("reddit_post_id") in deleted:
                filtered += 1
                return None
            result = {key: clean(item) for key, item in node.items()}
            if result.get("author"):
                result = workflows.scrub_tombstoned_authors([result], conn=conn)[0]
            return result
        if isinstance(node, list):
            return [cleaned for item in node if (cleaned := clean(item)) is not None]
        return node

    result = clean(value)
    if not isinstance(result, dict):
        raise ValueError("Episode snapshot root was filtered by lifecycle enrollment")
    if filtered and "candidates" in result:
        candidates = result["candidates"]
        result["episode_matrix"] = episode._matrix(candidates, result.get("expected_episode_numbers", []))
        result["collection_complete"] = False
        result["lifecycle_filtered"] = filtered
        result["coverage_found_episode_count"] = len({row["episode_number"] for row in candidates})
        result["coverage_found_slots"] = len({(row["episode_number"], row["discussion_type"]) for row in candidates})
    return result


def create_job(community_id: str, payload: dict) -> dict:
    digest = hashlib.sha256(capture._json(payload).encode()).hexdigest()
    with pg.transaction() as conn:
        capture._one(
            "select pg_advisory_xact_lock(hashtextextended(%s,0))",
            ["reddit-episode:" + community_id + ":" + digest],
            conn=conn,
        )
        existing = capture._one(
            """select * from social.reddit_episode_discovery_jobs
            where community_id=%s and config_hash=%s and
            (state in ('queued','running') or (state='completed' and result is not null
             and updated_at>now()-interval '2 minutes'))
            order by created_at desc limit 1""",
            [community_id, digest],
            conn=conn,
        )
        if existing:
            return existing
        created = capture._one(
            """insert into social.reddit_episode_discovery_jobs(community_id,config_hash,payload)
            values(%s,%s,%s::jsonb) returning *""",
            [community_id, digest, capture._json(payload)],
            conn=conn,
        )
        if created is None:
            raise RuntimeError("Episode discovery job insert returned no row")
        return created


def get_job(job_id: str) -> dict | None:
    return pg.fetch_one("select * from social.reddit_episode_discovery_jobs where id=%s", [job_id])


def execute_job(job_id: str) -> dict:
    token = str(uuid.uuid4())
    job = capture._one(
        """update social.reddit_episode_discovery_jobs set state='running',claim_token=%s,
        lease_expires_at=now()+interval '5 minutes',attempt_count=attempt_count+1,updated_at=now()
        where id=%s and ((state='queued' and next_attempt_at<=now())
        or (state='running' and lease_expires_at<now())) returning *""",
        [token, job_id],
    )
    if not job:
        return get_job(job_id) or {}
    stop = threading.Event()
    lost = threading.Event()

    def check(conn: Any = None):
        row = capture._one(
            """select id from social.reddit_episode_discovery_jobs
            where id=%s and claim_token=%s and state='running' and lease_expires_at>now() for update""",
            [job_id, token],
            conn=conn,
        )
        if lost.is_set() or not row:
            raise capture.LostClaim("Episode discovery ownership changed")

    def heartbeat():
        while not stop.wait(30):
            try:
                updated = pg.execute_returning(
                    """update social.reddit_episode_discovery_jobs
                    set lease_expires_at=now()+interval '5 minutes',updated_at=now()
                    where id=%s and claim_token=%s and state='running' and lease_expires_at>now() returning id""",
                    [job_id, token],
                )
                if not updated:
                    lost.set()
                    return
            except Exception:
                lost.set()
                return

    def save_checkpoint(value: dict):
        with pg.transaction() as conn:
            value = safe_snapshot(value, conn)
            check(conn)
            capture._one(
                "update social.reddit_episode_discovery_jobs set checkpoint=%s::jsonb,updated_at=now() "
                "where id=%s returning id",
                [capture._json(value), job_id],
                conn=conn,
            )

    thread = threading.Thread(target=heartbeat, name="reddit-episode-lease", daemon=True)
    thread.start()
    try:
        from contextlib import contextmanager

        from trr_backend.repositories.reddit_transport import build_client

        client = build_client(require_oauth=True)
        guard = client.request_guard

        @contextmanager
        def checked_guard(lane):
            check()
            with guard(lane):
                check()
                yield

        client.request_guard = checked_guard
        payload = dict(job["payload"])
        rules = episode.resolve_episode_rules(payload.get("rules") or payload)
        payload["episodeTitlePatterns"] = rules["effectiveEpisodeTitlePatterns"]
        payload["episodeRequiredFlairs"] = rules["effectiveRequiredFlairs"]
        result = episode.discover_episode(
            payload, client=client, checkpoint=job.get("checkpoint") or None, on_checkpoint=save_checkpoint
        )
        result["resolved_rules"] = rules
        air_dates = {
            str(row["episode_number"]): row.get("air_date")
            for row in payload.get("seasonEpisodes", [])
            if isinstance(row, dict) and row.get("episode_number")
        }
        for candidate in result.get("candidates", []):
            number = candidate.get("episode_number")
            candidate["sync_eligibility"] = episode.evaluate_sync_eligibility(
                {
                    "title": candidate.get("title"),
                    "author": candidate.get("author"),
                    "episodeNumber": number,
                    "seasonNumber": payload.get("seasonNumber"),
                    "episodeAirDateKey": air_dates.get(str(number)) or air_dates.get(number),
                    "postedAt": candidate.get("posted_at"),
                    "discussionType": candidate.get("discussion_type"),
                }
            )
        with pg.transaction() as conn:
            result = safe_snapshot(result, conn)
            check(conn)
            capture._one(
                """update social.reddit_episode_discovery_jobs set
state='completed',result=%s::jsonb,lease_expires_at=null,updated_at=now() where id=%s returning
id""",
                [capture._json(result), job_id],
                conn=conn,
            )
        return result
    except capture.LostClaim:
        return get_job(job_id) or {}
    except Exception as exc:
        from trr_backend.repositories.reddit_transport import RedditTransportError

        transient = isinstance(exc, RedditTransportError) and (
            exc.status is None or exc.status == 429 or exc.status >= 500
        )
        retry = transient and int(job["attempt_count"]) < 5
        delay = max(
            float(getattr(exc, "retry_after_seconds", None) or 0),
            min(3600, 30 * 2 ** max(0, int(job["attempt_count"]) - 1)) + random.random() * 5,
        )
        # Only callbacks advance the durable checkpoint. An exception may carry an
        # in-memory page whose database transaction failed; do not skip that page.
        pg.execute_returning(
            """update social.reddit_episode_discovery_jobs set
state=%s,error=%s,claim_token=null,lease_expires_at=null,next_attempt_at=now()+(%s*interval '1 second'),
updated_at=now() where id=%s and claim_token=%s and lease_expires_at>now() returning id""",
            ["queued" if retry else "failed", type(exc).__name__, delay, job_id, token],
        )
        return get_job(job_id) or {}
    finally:
        stop.set()
        thread.join(timeout=1)
