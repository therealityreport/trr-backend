"""Audited date-range comment continuations backed by committed attempt pages."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import Conflict, RunIntent, Stage, fingerprint, stable_json
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.validation_gate import require_allowed_intent


def extend_productive_budgets(
    store: Store, *, run_id: str, account: str, generation: int, intent_hash: str, source_revision: str
) -> list[dict]:
    """Grant twelve further attempts only after the exhausted attempt committed a page.

    A page's immutable owner_generation identifies its acquired attempt. The
    page writer requires a live owner_started_at before committing; therefore
    matching generations retain that proof when checkpoint_failure releases the
    owner and clears its start timestamp. When a start remains, check it too.
    Later attempts must add observed IDs or exhausted lanes versus the latest
    prior-owner page. An empty reply page can strengthen that traversal proof.
    """
    if not isinstance(source_revision, str) or not 0 < len(source_revision.strip()) <= 200:
        raise ValueError("Productive budget extension requires a bounded source revision")
    extended = []
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        run = store._execution_run(cur, run_id, account, shared_fence=True)
        intent = RunIntent.from_payload(run["intent"])
        require_allowed_intent(intent)
        if (
            intent.account != account
            or run["generation"] != generation
            or run["state"] not in {"queued", "running"}
            or run.get("execution_hold_reason")
            or run["intent_hash"] != intent_hash
            or intent.digest != intent_hash
            or intent.scope != "bounded_window"
            or intent.date_start is None
            or intent.date_end is None
            or intent.date_start >= intent.date_end
            or intent.effective_source_profile.access_mode != "public"
            or Stage.COMMENTS not in intent.selected_tasks
        ):
            raise Conflict("Productive budget extension requires the exact current unheld public date-range binding")
        cur.execute(
            """select t.*,p.page_id as productive_page_id,previous.page_id as previous_page_id,
            case when p.checkpoint#>>'{comment_pages,version}'='2'
                then (p.checkpoint#>>'{comment_pages,observed_count}')::bigint
                else (select count(*) from jsonb_object_keys(
                    coalesce(p.checkpoint#>'{comment_pages,observed}','{}'::jsonb))) end as observed_count,
            case when previous.checkpoint#>>'{comment_pages,version}'='2'
                then (previous.checkpoint#>>'{comment_pages,observed_count}')::bigint
                else (select count(*) from jsonb_object_keys(
                    coalesce(previous.checkpoint#>'{comment_pages,observed}','{}'::jsonb)))
                end as previous_observed_count,
            case when p.checkpoint#>>'{comment_pages,version}'='2'
                then (p.checkpoint#>>'{comment_pages,exhausted_count}')::bigint
                else (select count(*) from jsonb_each(
                    coalesce(p.checkpoint#>'{comment_pages,lanes}','{}'::jsonb)) lane
                    where lane.value->'exhausted'='true'::jsonb) end as exhausted_count,
            case when previous.checkpoint#>>'{comment_pages,version}'='2'
                then (previous.checkpoint#>>'{comment_pages,exhausted_count}')::bigint
                else (select count(*) from jsonb_each(
                    coalesce(previous.checkpoint#>'{comment_pages,lanes}','{}'::jsonb)) lane
                    where lane.value->'exhausted'='true'::jsonb) end as previous_exhausted_count,
            clock_timestamp() as database_now
            from social.instagram_ingestion_targets t
            join lateral (
                select p.page_id,p.checkpoint from social.instagram_ingestion_comment_pages p
                where p.run_id=t.run_id and p.stage=t.stage and p.target_key=t.target_key
                and p.owner_generation=t.owner_generation
                and (t.owner_started_at is null or p.committed_at>=t.owner_started_at)
                order by p.committed_at desc,p.page_id desc limit 1
            ) p on true
            left join lateral (
                select prior.page_id,prior.checkpoint from social.instagram_ingestion_comment_pages prior
                where prior.run_id=t.run_id and prior.stage=t.stage and prior.target_key=t.target_key
                and prior.owner_generation<t.owner_generation
                order by prior.committed_at desc,prior.page_id desc limit 1
            ) previous on true
            where t.run_id=%s and t.stage='comments' and t.outcome='unresolved'
            and t.attempts>=t.max_attempts and t.blocked_reason is null
            and (t.owner_expires_at is null or t.owner_expires_at<=clock_timestamp())
            and (t.owner_token is null or t.owner_expires_at is not null)
            order by t.target_key for update of t skip locked""",
            (run_id,),
        )
        targets = [dict(row) for row in cur.fetchall()]
        for target in targets:
            if target["previous_page_id"] is not None and not (
                target["observed_count"] > target["previous_observed_count"]
                or target["exhausted_count"] > target["previous_exhausted_count"]
            ):
                continue
            recovery = target["checkpoint"].get("public_recovery") or {}
            no_improvement = recovery.get("no_improvement_passes", 0)
            if type(no_improvement) is not int or no_improvement < 0 or no_improvement >= 2:
                continue
            request = {
                "command": "extend_productive_comment_budget_v1",
                "run_id": run_id,
                "generation": generation,
                "intent_hash": intent_hash,
                "target_key": target["target_key"],
                "checkpoint_sha256": fingerprint(target["checkpoint"]),
                "attempts": target["attempts"],
                "old_max_attempts": target["max_attempts"],
                "new_max_attempts": target["attempts"] + 12,
                "owner_generation": target["owner_generation"],
                "productive_page_id": str(target["productive_page_id"]),
                "previous_page_id": str(target["previous_page_id"]) if target["previous_page_id"] else None,
                "observed_count": target["observed_count"],
                "previous_observed_count": target["previous_observed_count"],
                "exhausted_count": target["exhausted_count"],
                "previous_exhausted_count": target["previous_exhausted_count"],
                "reason": "committed_page_in_exhausted_attempt",
                "source_revision": source_revision,
            }
            command_hash = fingerprint(request)
            key = f"productive-comments:{run_id}:{generation}:{command_hash}"
            previous = _row(
                cur,
                """select command_hash from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, key),
            )
            if previous:
                if previous["command_hash"] != command_hash:
                    raise Conflict("Productive budget journal identity conflicts")
                continue
            result = {**request, "idempotency_key": key, "at": target["database_now"].isoformat()}
            # Preserve retry deadlines, failure classification, source checkpoints,
            # owner fences, rows and every attempt/request counter verbatim.
            cur.execute(
                """update social.instagram_ingestion_targets set max_attempts=%s
                where run_id=%s and stage='comments' and target_key=%s""",
                (request["new_max_attempts"], run_id, target["target_key"]),
            )
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, key, command_hash, run_id, stable_json(result)),
            )
            extended.append(
                {
                    name: result[name]
                    for name in (
                        "target_key",
                        "attempts",
                        "old_max_attempts",
                        "new_max_attempts",
                        "checkpoint_sha256",
                        "idempotency_key",
                    )
                }
            )
    return extended
