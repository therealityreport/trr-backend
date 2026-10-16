"""Read-only progress snapshots. No recovery or workflow dispatch imports."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import ReplayUnavailable, RunIntent, Stage, StageCounts, run_complete
from trr_backend.ingestion.evidence import Evidence


def _read_connection():
    from trr_backend.db import pg

    return pg.db_read_connection(label="instagram-ingestion-progress", pool_name="social_progress")


class Queries:
    def __init__(self, read_connection: Callable = _read_connection):
        self.read_connection = read_connection

    def progress(self, run_id: str, account: str) -> dict[str, Any]:
        # One SQL statement: manifests, counters and lifecycle share a snapshot.
        # Never call legacy progress helpers: several of them perform recovery.
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                select r.*,
                  (select jsonb_agg(s order by s.stage) from (
                    select m.stage,m.discovery_complete,m.sealed_at,
                      count(t.target_key)::int as total,
                      count(*) filter(where t.outcome='committed')::int as committed,
                      count(*) filter(where t.outcome='cached_satisfied')::int as cached_satisfied,
                      count(*) filter(where t.outcome='source_unavailable')::int as source_unavailable,
                      count(*) filter(where t.outcome='unresolved')::int as unresolved,
                      coalesce(sum(t.attempts),0)::int as attempts,
                      coalesce(sum(t.requests),0)::int as requests,
                      min(t.next_attempt_at) filter(where t.outcome='unresolved') as retry_at,
                      array_remove(array_agg(distinct t.blocked_reason),null) as blocked_reasons,
                      max(t.committed_at) as last_committed_at
                    from social.instagram_ingestion_manifests m
                    left join social.instagram_ingestion_targets t using(run_id,stage)
                    where m.run_id=r.run_id group by m.stage,m.discovery_complete,m.sealed_at
                  ) s) as stages,
                  (select count(*)::int from social.instagram_ingestion_evidence e
                    where e.run_id=r.run_id and e.payload is not null and e.expires_at<=clock_timestamp()
                    and r.state<>'completed') as pinned_evidence_count
                from social.instagram_ingestion_runs r where r.run_id=%s and r.account=%s
            """,
                (run_id, account),
            )
            row = cur.fetchone()
        if not row:
            raise LookupError("Instagram run not found for this account")
        result = dict(row)
        result.pop("execution_selection", None)
        result["run_id"] = str(result["run_id"])
        stages = result.pop("stages") or []
        counts = {}
        for stage in stages:
            stage_counts = StageCounts(
                **{
                    field: stage[field]
                    for field in (
                        "total",
                        "committed",
                        "cached_satisfied",
                        "source_unavailable",
                        "unresolved",
                        "discovery_complete",
                    )
                }
            )
            counts[Stage(stage["stage"])] = stage_counts
            stage["complete"] = stage_counts.complete
        result["stages"] = stages
        result["complete"] = run_complete(RunIntent.from_payload(result["intent"]), counts)
        result["allowed_actions"] = self._actions(result["state"], result["complete"])
        return result

    @staticmethod
    def _actions(state: str, complete: bool) -> list[str]:
        if complete:
            return []
        if state in {"queued", "running"}:
            return ["cancel"]
        if state in {"cancelled", "failed", "blocked"}:
            return ["resume"]
        return []

    def recent(self, account: str, *, source_scope: str, limit: int = 20) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("Recent run limit must be 1 to 100")
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select run_id,account,state,intent,engine,workflow_version,created_at,last_committed_at
                from social.instagram_ingestion_runs where account=%s and intent->>'source_scope'=%s
                order by created_at desc,run_id desc limit %s""",
                (account, source_scope, limit),
            )
            return [dict(row, run_id=str(row["run_id"])) for row in cur.fetchall()]

    def evidence(self, evidence_id: str, account: str) -> dict[str, Any]:
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select e.*,r.state<>'completed' as pinned from social.instagram_ingestion_evidence e
                join social.instagram_ingestion_runs r using(run_id) where e.evidence_id=%s and r.account=%s""",
                (evidence_id, account),
            )
            row = cur.fetchone()
        if not row:
            raise LookupError("Source evidence not found for this account")
        if row["payload"] is None:
            raise ReplayUnavailable("Source payload expired; compact provenance remains")
        evidence = Evidence(
            source_identity=row["source_identity"],
            captured_at=row["captured_at"],
            parser_version=row["parser_version"],
            payload_json=row["payload"],
            content_hash=row["content_hash"],
            schema_version=row["schema_version"],
        )
        payload = evidence.read(pinned=row["pinned"])
        return {
            "evidence": evidence,
            "payload": payload,
            "run_id": str(row["run_id"]),
            "stage": row["stage"],
            "target_key": row["target_key"],
            "pinned": row["pinned"],
        }

    def diagnostics(self, run_id: str, account: str, *, limit: int = 100) -> dict[str, Any]:
        """Bounded operator evidence; never select source payloads or owner tokens."""
        if not 1 <= limit <= 100:
            raise ValueError("Diagnostic limit must be 1 to 100")
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select r.run_id,r.account,
                (select coalesce(jsonb_agg(t order by t.stage,t.target_key),'[]'::jsonb) from (
                  select stage,target_key,outcome,attempts,requests,max_attempts,
                    next_attempt_at as retry_at,blocked_reason,checkpoint->>'last_error_code' as last_error_code
                  from social.instagram_ingestion_targets where run_id=r.run_id
                  order by stage,target_key limit %s
                ) t) as targets,
                (select coalesce(jsonb_agg(e order by e.captured_at desc,e.evidence_id),'[]'::jsonb) from (
                  select evidence_id,stage,target_key,source_identity,captured_at,content_hash,
                    payload is not null and (expires_at>clock_timestamp() or r.state<>'completed') as available,
                    expires_at,r.state<>'completed' as pinned,parser_version,schema_version
                  from social.instagram_ingestion_evidence where run_id=r.run_id
                  order by captured_at desc,evidence_id limit %s
                ) e) as evidence
                from social.instagram_ingestion_runs r where r.run_id=%s and r.account=%s""",
                (limit + 1, limit + 1, run_id, account),
            )
            row = cur.fetchone()
        if not row:
            raise LookupError("Instagram run not found for this account")
        targets, evidence = row["targets"], row["evidence"]
        return {
            "run_id": str(row["run_id"]),
            "account": row["account"],
            "targets": targets[:limit],
            "evidence": evidence[:limit],
            "targets_truncated": len(targets) > limit,
            "evidence_truncated": len(evidence) > limit,
        }
