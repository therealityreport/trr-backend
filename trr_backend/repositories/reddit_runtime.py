"""Live adapters for durable Reddit jobs; transport failure never authorizes deletion."""

from __future__ import annotations

from typing import Any

from trr_backend.db import pg
from trr_backend.repositories import reddit_capture as capture
from trr_backend.repositories import reddit_workflows as workflows


def probe_health() -> dict:
    import hashlib

    from trr_backend.repositories.reddit_transport import build_client

    client = build_client(require_oauth=True, recovery_probe=True)
    identity = hashlib.sha256((client.client_id or "anonymous").encode()).hexdigest()
    budget = (
        pg.fetch_one(
            """select not_before>now() or
        (provider_remaining<=0 and provider_reset_at>now()) as cooling_down
        from social.reddit_request_budgets where identity_key=%s""",
            [identity],
        )
        or {}
    )
    if budget.get("cooling_down"):
        return {
            "configured": bool(client.client_id and client.client_secret),
            "authenticated": False,
            "reachable": None,
            "cooling_down": True,
            "blocked": False,
        }
    health = client.token_probe()
    if health.get("authenticated") and health.get("reachable") and not health.get("blocked"):
        pg.execute_returning(
            "update social.reddit_request_budgets set blocked=false where identity_key=%s returning identity_key",
            [identity],
        )
    return health


def lifecycle_observations(post_ids: list[str]) -> list[dict]:
    from trr_backend.repositories.reddit_transport import build_client

    client = build_client(require_oauth=True)
    comments = pg.fetch_all(
        "select reddit_post_id,reddit_comment_id from social.reddit_comments where reddit_post_id=any(%s::text[])",
        [post_ids],
    )
    identifiers = {f"t3_{pid}": {"reddit_post_id": pid} for pid in post_ids}
    identifiers.update({f"t1_{row['reddit_comment_id']}": dict(row) for row in comments})
    observed = []
    keys = list(identifiers)
    for offset in range(0, len(keys), 100):
        batch = keys[offset : offset + 100]
        response = client.get_json("/api/info", params={"id": ",".join(batch), "raw_json": 1})
        children = response.get("data", {}).get("children")
        if not isinstance(children, list):
            raise ValueError("Malformed lifecycle source listing")
        for child in children:
            data = child.get("data") if isinstance(child, dict) else None
            if not isinstance(data, dict):
                raise ValueError("Malformed lifecycle source content")
            identifier = data.get("name") or f"{child.get('kind')}_{data.get('id')}"
            if identifier not in identifiers:
                continue
            identity = identifiers[identifier]
            content = data.get("body") if child.get("kind") == "t1" else data.get("selftext")
            category = data.get("removed_by_category")
            signal = "present"
            if content == "[deleted]" or category == "deleted":
                signal = "deleted"
            elif content == "[removed]" or category in {"moderator", "automod_filtered", "reddit"}:
                signal = "removed"
            elif data.get("author") == "[deleted]":
                signal = "account_deleted"
            observed.append({**identity, "checked": True, "source": "reddit", "explicit": True, "signal": signal})
        # Missing IDs, empty listing and private content are unresolved, not deletion signals.
    return observed


def delete_object(key: str) -> None:
    from trr_backend.media.s3_mirror import get_object_storage_bucket, get_s3_client

    if not key.startswith("reddit/"):
        raise SharedLegacyObjectError(
            "Legacy object uses shared storage; retain until all-platform references are verified"
        )

    get_s3_client().delete_object(Bucket=get_object_storage_bucket(), Key=key)


class SharedLegacyObjectError(RuntimeError):
    pass


def execute_workflow(job_id: str) -> dict:
    return workflows.execute_job(job_id, lifecycle_provider=lifecycle_observations, object_deleter=delete_object)


def dispatch_tick(dispatch_capture: Any, dispatch_workflow: Any, dispatch_episode: Any) -> dict:
    if not capture.enabled():
        return {"enabled": False}
    jobs = workflows.due_dispatcher()
    deliveries = []
    for job in jobs:
        if dispatch_workflow(str(job["id"])):
            deliveries.append(str(job["id"]))
    episodes = pg.fetch_all(
        "select id from social.reddit_episode_discovery_jobs where (state='queued' and next_attempt_at<=now()) "
        "or (state='running' and lease_expires_at<now()) order by created_at limit 25"
    )
    for job in episodes:
        if dispatch_episode(str(job["id"])):
            deliveries.append(str(job["id"]))
    runs = capture.dispatch_due(dispatch_capture)
    monitor = (
        pg.fetch_one("""select
        (select count(*) from social.reddit_work_targets
         where state in ('failed','retry_wait')) as unresolved_retry_targets,
        (select count(*) from social.reddit_lifecycle_enrollment
         where state='active' and next_check_at<now()-interval '1 hour') as reconciliation_late,
        (select count(*) from social.reddit_refresh_runs r where r.contract_version=2 and r.status='completed'
         and exists(select 1 from social.reddit_work_targets t where t.run_id=r.id and t.required
         and t.state not in ('committed','source_unavailable'))) as invalid_completed_runs""")
        or {}
    )
    if monitor.get("invalid_completed_runs") or monitor.get("reconciliation_late"):
        import logging

        logging.getLogger(__name__).error("Reddit invariant/reconciliation alert: %s", monitor)
    workflows.drain_object_deletions(object_deleter=delete_object)
    return {"enabled": True, "runs_dispatched": runs, "jobs_dispatched": deliveries, "monitor": monitor}


def schedule_capture_template(community_id: str, season_id: str) -> dict:
    """Resolve the current canonical show/community rules for an opt-in schedule."""
    row = pg.fetch_one(
        """select c.*,s.name as show_name,s.alternative_names as show_aliases
        from admin.reddit_communities c join core.shows s on s.id=c.trr_show_id
        join core.seasons season on season.show_id=s.id
        where c.id=%s and season.id=%s and c.is_active""",
        [community_id, season_id],
    )
    if not row:
        raise ValueError("Active community and season must belong to the same show")
    cast_rows = pg.fetch_all(
        """select distinct coalesce(o.full_name_override,p.full_name) as name
        from core.show_cast c join core.people p on p.id=c.person_id
        left join core.people_overrides o on o.person_id=p.id where c.show_id=%s""",
        [row["trr_show_id"]],
    )
    return capture.normalize_policy(
        {
            "community_id": community_id,
            "season_id": season_id,
            "period_key": "scheduled-season",
            "subreddit": row["subreddit"],
            "show_name": row["show_name"],
            "show_aliases": row.get("show_aliases") or [],
            "cast_names": sorted({r["name"] for r in cast_rows if r.get("name")}),
            "is_show_focused": bool(row.get("is_show_focused")),
            "analysis_flairs": row.get("analysis_flairs") or [],
            "analysis_all_flairs": row.get("analysis_all_flairs") or [],
            "force_include_flairs": row.get("analysis_all_flairs") or [],
            "requested_phases": ["discovery", "comments", "media"],
            "refresh_policy": "stale",
            "sort_modes": ["new"],
            "exhaustive_window": True,
            "search_backfill": True,
            "max_pages": 100,
            "max_backfill_queries": 30,
            "max_backfill_pages_per_query": 10,
            "limit_per_mode": 100,
        }
    )
