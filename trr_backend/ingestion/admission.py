"""Read-only legacy/new capacity observation for the existing Modal functions.

This is not an atomic reservation across legacy dispatchers. The new coordinator
must remain singleton, existing readers must count new leases, and Modal's same
physical function caps remain authoritative. Probe connections are short lived
and happen only after the read connection has closed.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from psycopg2.extras import RealDictCursor

from api.schemas.v2.instagram_ingestion import InstagramIngestionLaunchV2
from trr_backend.ingestion.command_service import ServiceUnavailableError
from trr_backend.ingestion.domain import Conflict, RunIntent, Stage

_STAGE = {
    "listing": "shared_account_discovery",
    "post_details": "shared_account_posts",
    "comments": "comments",
    "media": "media_mirror",
}
_PHYSICAL = {
    "posts": "posts",
    "shared_account_discovery": "posts",
    "shared_account_posts": "posts",
    "comments": "comments",
    "media_mirror": "media",
}
_STAGE_ENVS = {
    "posts": ("SOCIAL_WORKER_POOL_POSTS", 8),
    "shared_account_discovery": ("SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY", 3),
    "shared_account_posts": ("SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS", 5),
    "comments": ("SOCIAL_WORKER_POOL_COMMENTS", 4),
    "media_mirror": ("SOCIAL_WORKER_POOL_MEDIA_MIRROR", 6),
}
_TERMINAL = "('completed','failed','cancelled','terminated','error')"
_LEGACY_ACTIVITY = "coalesce(completed_at,heartbeat_at,started_at,claimed_at,created_at)"
# Match the cast-free expression index. Any lease text remains a candidate so
# malformed metadata still reaches the original fail-closed busy predicate.
_LEGACY_MUST_INSPECT = f"""(
    lower(coalesce(status,'')) not in ('completed','failed','cancelled')
    or {_LEGACY_ACTIVITY} is null
    or nullif(metadata->'dispatch'->>'lease_expires_at','') is not null
)"""

_LEGACY_PROJECTION = """select 'legacy:'||id::text as identity,run_id::text,lower(coalesce(platform,'')) as platform,
                    lower(coalesce(config->>'stage',metadata->>'stage',job_type,'')) as raw_stage,
                    lower(ltrim(coalesce(config->>'account',config->>'account_handle',config->>'source_account',
                      config->>'username',''),'@')) as account,
                    lower(coalesce(status,'')) as status,worker_id,claimed_at,
                    coalesce(completed_at,heartbeat_at,started_at,claimed_at,created_at) as last_activity_at,
                    metadata->'dispatch' as dispatch"""


def _read_connection():
    from trr_backend.db import pg

    return pg.db_read_connection(label="instagram-ingestion-admission", pool_name="social_control")


def _fresh_session_probe(**kwargs):
    from trr_backend.db import pg

    return pg.probe_fresh_session_capacity(**kwargs)


def _positive(env, name, default):
    try:
        return max(1, int(env.get(name) or default))
    except (TypeError, ValueError):
        return default


class Admission:
    def __init__(
        self,
        *,
        read_connection: Callable = _read_connection,
        fresh_session_probe: Callable = _fresh_session_probe,
        environ: Mapping[str, str] | None = None,
        identity_key: str | None = None,
    ):
        self.read_connection = read_connection
        self.fresh_session_probe = fresh_session_probe
        self.env = dict(os.environ if environ is None else environ)
        self.identity_key = identity_key
        self.stage_caps = {stage: _positive(self.env, name, default) for stage, (name, default) in _STAGE_ENVS.items()}
        budget_env = (
            "SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET"
            if self.env.get("SOCIAL_INSTAGRAM_DB_SESSION_WORKER_BUDGET")
            else "SOCIAL_INSTAGRAM_COMMENTS_DB_SESSION_BUDGET"
        )
        self.combined_cap = _positive(self.env, budget_env, 10)
        self.catalog_cap = min(32, _positive(self.env, "SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP", 4))
        self.physical_caps = {
            "posts": _positive(self.env, "TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", 8),
            "comments": _positive(self.env, "TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT", 4),
            "media": _positive(self.env, "TRR_MODAL_SOCIAL_MEDIA_JOB_CONCURRENCY_LIMIT", 1),
        }
        self.platform_caps = {
            "posts": _positive(self.env, "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM", 10),
            "comments": _positive(self.env, "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM", 10),
            "shared_account_discovery": _positive(
                self.env, "SOCIAL_SHARED_ACCOUNT_DISCOVERY_PLATFORM_CAP_INSTAGRAM", 1
            ),
            "shared_account_posts": _positive(self.env, "SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM", 1),
            "media_mirror": _positive(self.env, "SOCIAL_MIRROR_PLATFORM_CAP", 2),
        }
        self.scale_accounts = str(self.env.get("SOCIAL_PLATFORM_CAP_PER_ACCOUNT_SCALING", "false")).lower() not in {
            "0",
            "false",
            "off",
            "no",
            "",
        }
        self.probe_limit = _positive(self.env, "SOCIAL_INSTAGRAM_DB_SESSION_POOL_LIMIT", 15)

    def _snapshot(self, account: str, run_id: str | None = None) -> dict[str, Any]:
        # One SQL snapshot; no provider inspection, credential reads or repair.
        # Preserve the established two-hour drain window after business terminal
        # state. Current leases and unaged ambiguity remain occupied; malformed
        # lease metadata fails the whole read closed rather than disappearing.
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""with legacy as materialized (
                  {_LEGACY_PROJECTION}
                  from social.scrape_jobs where {_LEGACY_MUST_INSPECT} is true
                  union all
                  {_LEGACY_PROJECTION}
                  from social.scrape_jobs where {_LEGACY_MUST_INSPECT} is false
                    and {_LEGACY_ACTIVITY}>=statement_timestamp()-interval '2 hours'
                ), legacy_busy as (
                  select identity,run_id,platform,account,
                    case when raw_stage in ('comments','comments_scrapling') then 'comments'
                         when raw_stage in ('posts_scrapling','tiktok_posts_scrapling',
                           'threads_posts_scrapling') then 'posts'
                         when raw_stage in ('media_mirror','instagram_media_mirror','comment_media_mirror')
                           then 'media_mirror'
                         when raw_stage in ('posts','shared_account_discovery','shared_account_posts',
                           'post_classify','season_materialize','analytics_refresh') then raw_stage
                         else 'unknown' end as stage
                  from legacy where (
                    status in ('running','claimed','processing')
                    or (status not in {_TERMINAL} and (nullif(worker_id,'') is not null or claimed_at is not null))
                    or (nullif(dispatch->>'remote_invocation_id','') is not null
                        and lower(coalesce(dispatch->>'remote_invocation_status','unknown')) not in {_TERMINAL})
                    or (lower(coalesce(dispatch->>'remote_invocation_status','unknown')) not in {_TERMINAL}
                        and (status='dispatched' or nullif(dispatch->>'dispatch_reservation_id','') is not null
                          or nullif(dispatch->>'dispatch_requested_at','') is not null
                          or nullif(dispatch->>'lease_expires_at','') is not null)
                        and (nullif(dispatch->>'lease_expires_at','') is null
                          or (dispatch->>'lease_expires_at')::timestamptz>clock_timestamp()))
                  ) and (
                    status not in ('completed','failed','cancelled')
                    or last_activity_at is null
                    or last_activity_at>=clock_timestamp()-interval '2 hours'
                    or (nullif(dispatch->>'lease_expires_at','') is not null
                      and (dispatch->>'lease_expires_at')::timestamptz>clock_timestamp())
                  )
                ), new_busy as (
                  select 'new:'||t.owner_token::text as identity,t.run_id::text,'instagram' as platform,r.account,
                    case t.stage when 'listing' then 'shared_account_discovery'
                         when 'post_details' then 'shared_account_posts'
                         when 'comments' then 'comments' when 'media' then 'media_mirror' else 'unknown' end as stage
                  from social.instagram_ingestion_targets t join social.instagram_ingestion_runs r using(run_id)
                  where t.outcome='unresolved' and t.owner_token is not null
                    and (t.owner_expires_at is null or t.owner_expires_at>clock_timestamp())
                  union
                  select 'new:'||d.owner_token::text,d.run_id::text,'instagram',r.account,
                    case d.stage when 'listing' then 'shared_account_discovery'
                         when 'post_details' then 'shared_account_posts'
                         when 'comments' then 'comments' when 'media' then 'media_mirror' else 'unknown' end
                  from social.instagram_ingestion_dispatches d
                  join social.instagram_ingestion_runs r using(run_id)
                  join social.instagram_ingestion_targets t using(run_id,stage,target_key)
                  where d.state='sending' and t.outcome='unresolved'
                ), busy as (select * from legacy_busy union all select * from new_busy), counts as (
                  select stage,platform,run_id,account,count(*)::int as total from busy
                  where stage not in ('post_classify','season_materialize','analytics_refresh')
                  group by stage,platform,run_id,account
                ) select coalesce((select jsonb_agg(counts) from counts),'[]'::jsonb) as counts,
                  (exists(select 1 from social.account_auth_cooldown where platform='instagram'
                    and account_handle=%s and blocker_kind='checkpoint')
                  or exists(select 1 from social.instagram_detail_request_lanes where identity_key=%s
                    and (blocked or server_retry_after_until>clock_timestamp()))) as blocked,
                  (select jsonb_build_object('run_id',run_id,'account',account,'state',state,'generation',generation,
                    'engine',engine,'workflow_version',workflow_version,'intent',intent,
                    'intent_hash',intent_hash,'execution_placement',
                    coalesce(to_jsonb(r)->>'execution_placement','modal'),
                    'execution_policy',(select to_jsonb(ep) from social.instagram_ingestion_execution_policies ep
                      where ep.run_id=r.run_id))
                    from social.instagram_ingestion_runs r where run_id=%s::uuid and account=%s) as run""",
                (account, self.identity_key, run_id, account),
            )
            row = cur.fetchone()
            if row and row.get("run") and row["run"].get("execution_placement") == "local":
                from trr_backend.ingestion.bounded_recovery import require_window

                require_window(cur, row["run"], environ=self.env)
                if row["run"].get("_bounded_window"):
                    cur.execute(
                        """select exists(select 1 from social.instagram_ingestion_targets
                        where run_id=%s and stage='comments' and outcome='unresolved'
                        and blocked_reason is null and attempts<max_attempts
                        and (next_attempt_at is null or next_attempt_at<=clock_timestamp())
                        and (owner_expires_at is null or owner_expires_at<=clock_timestamp())) as ready""",
                        (run_id,),
                    )
                    row["run"]["_comments_ready"] = cur.fetchone()["ready"]
        if row is None:
            raise ServiceUnavailableError("Capacity snapshot is unavailable")
        return dict(row)

    def _fits(self, snapshot, stage, account, *, run_id=None, requested=None):
        if snapshot["blocked"]:
            return False
        counts = snapshot["counts"]
        if any(row["stage"] not in _PHYSICAL for row in counts):
            return False
        physical = _PHYSICAL[stage]
        stage_active = sum(row["total"] for row in counts if row["stage"] == stage)
        physical_active = sum(row["total"] for row in counts if _PHYSICAL[row["stage"]] == physical)
        if stage_active >= self.stage_caps[stage] or physical_active >= self.physical_caps[physical]:
            return False
        if physical != "media":
            combined = sum(
                row["total"] for row in counts if row["platform"] == "instagram" and row["stage"] != "media_mirror"
            )
            if combined >= self.combined_cap:
                return False
        stage_platform = [row for row in counts if row["stage"] == stage and row["platform"] == "instagram"]
        platform_cap = self.platform_caps[stage]
        if requested is not None:
            platform_cap = max(platform_cap, min(requested, self.stage_caps[stage]))
        if self.scale_accounts:
            accounts = {row["account"] for row in stage_platform if row["account"]} | {account}
            platform_cap = min(self.stage_caps[stage], platform_cap * len(accounts))
        if sum(row["total"] for row in stage_platform) >= platform_cap:
            return False
        if run_id is not None:
            run_active = sum(row["total"] for row in counts if row["run_id"] == run_id)
            if run_active >= self.catalog_cap:
                return False
            if requested is not None:
                lane_active = sum(row["total"] for row in stage_platform if row["run_id"] == run_id)
                if lane_active >= min(
                    requested, self.stage_caps[stage], self.physical_caps[physical], self.catalog_cap
                ):
                    return False
        return True

    def _probe(self):
        try:
            result = self.fresh_session_probe(requested_sessions=1, max_probe_sessions=self.probe_limit)
            return result.get("available") is True
        except Exception:
            return False

    def validate_launch(self, handle: str, payload: InstagramIngestionLaunchV2) -> None:
        # Requested counts remain frozen and are clamped at dispatch, matching
        # existing UI semantics. This check proves one slot, never reserves it.
        account = RunIntent(account=handle).account
        InstagramIngestionLaunchV2.model_validate(payload.model_dump(mode="json"))
        try:
            snapshot = self._snapshot(account)
        except Exception as error:
            raise ServiceUnavailableError("Capacity state could not be read") from error
        if snapshot["blocked"]:
            raise Conflict("Instagram account or selected identity is blocked or cooling down")
        if not self._fits(snapshot, "shared_account_discovery", account) or not self._probe():
            raise ServiceUnavailableError("Instagram launch capacity is unavailable")

    def admit(self, item: Mapping[str, Any]) -> bool:
        try:
            stage = _STAGE[Stage(item["stage"]).value]
            account = RunIntent(account=item["account"]).account
            run_id = str(UUID(str(item["run_id"])))
            snapshot = self._snapshot(account, run_id)
            run = snapshot["run"]
            if not run or run["state"] not in {"queued", "running"} or run["generation"] != item["generation"]:
                return False
            if any(key in item and item[key] != run[key] for key in ("engine", "workflow_version")):
                return False
            intent = RunIntent.from_payload(run["intent"])
            from trr_backend.ingestion.validation_gate import require_allowed_intent

            require_allowed_intent(intent)
            if intent.account != account or (
                item["stage"] != "listing" and Stage(item["stage"]) not in intent.selected_tasks
            ):
                return False
            requested = (
                intent.detail_worker_count
                if stage == "shared_account_posts"
                else (intent.comments_worker_count if stage == "comments" else None)
            )
            if stage == "comments":
                from trr_backend.ingestion.execution_policy import effective_comments_limit_from_run

                requested = effective_comments_limit_from_run(run, run.get("execution_policy"))
            window = run.get("_bounded_window") or {}
            if (window.get("stop_policy") or {}).get("mode") == "useful_progress":
                allowed_stage = {"comments": "comments", "media": "media_mirror"}.get(window.get("phase") or "")
                if stage != allowed_stage:
                    return False
            elif stage == "media_mirror" and window and run.get("_comments_ready"):
                return False
            return self._fits(snapshot, stage, account, run_id=run_id, requested=requested) and self._probe()
        except Exception:
            return False
