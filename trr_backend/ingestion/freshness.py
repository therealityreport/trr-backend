"""Read-only detail cache eligibility under the run's frozen freshness policy.

A recent scraped_at, a database default zero, or a media mirror flag is not a
complete source observation. The versioned detail proof and its actual source
metrics must be present. This module never settles a target or fetches a source.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import ReplayUnavailable, RunIntent, Stage, utc_timestamp
from trr_backend.ingestion.evidence import Evidence


@dataclass(frozen=True)
class DetailCacheDecision:
    eligible: bool
    reason: str
    observed_at: datetime | None = None
    canonical_identity: dict[str, str] = field(default_factory=dict)
    evidence_reference: dict[str, Any] | None = None


def _time(value: Any) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return datetime.fromisoformat(utc_timestamp(value))


def _source_node(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    items = raw.get("items")
    if isinstance(items, list) and items and isinstance(items[0], Mapping):
        return items[0]
    for outer in (raw.get("data"), raw.get("graphql")):
        if isinstance(outer, Mapping):
            for key in ("xdt_shortcode_media", "shortcode_media"):
                if isinstance(outer.get(key), Mapping):
                    return outer[key]
    return raw


def _metric(node: Mapping[str, Any], *keys: str) -> bool:
    for key in keys:
        value = node.get(key)
        if isinstance(value, Mapping):
            value = value.get("count")
        if type(value) is int and value >= 0:
            return True
    return False


def detail_eligibility(
    *,
    post: Mapping[str, Any] | None,
    sidecar: Mapping[str, Any] | None,
    canonical_identity: Mapping[str, Any],
    intent: RunIntent,
    now: datetime,
) -> DetailCacheDecision:
    """Pure coverage/TTL decision; never infer a source-unavailable outcome."""
    if intent.force:
        return DetailCacheDecision(False, "forced_refetch")
    if Stage.DETAILS not in intent.selected_tasks:
        return DetailCacheDecision(False, "details_not_selected")
    if not post:
        return DetailCacheDecision(False, "post_missing")
    binding = str(post.get("source_account") or post.get("username") or "").strip().lstrip("@").lower()
    if binding != intent.account:
        return DetailCacheDecision(False, "post_account_mismatch")
    if any(not canonical_identity.get(key) for key in ("post_id", "catalog_post_id", "canonical_post_id")):
        return DetailCacheDecision(False, "canonical_mapping_incomplete")
    if str(canonical_identity["post_id"]) != str(post.get("id")):
        return DetailCacheDecision(False, "canonical_post_identity_mismatch")
    raw = sidecar.get("raw_data") if sidecar is not None else None
    if raw is None:
        raw = post.get("raw_data")
    if not isinstance(raw, Mapping):
        return DetailCacheDecision(False, "detail_proof_missing")
    proof = raw.get("detail_snapshot")
    if not isinstance(proof, Mapping):
        return DetailCacheDecision(False, "detail_proof_missing")
    if (
        type(proof.get("version")) is not int
        or proof["version"] != intent.policy_version
        or type(proof.get("coverage_version")) is not int
        or proof["coverage_version"] != 2
        or proof.get("source") != "instagram_response"
        or any(
            proof.get(key) is not True
            for key in ("caption_present", "likes_present", "comments_present", "media_complete")
        )
    ):
        return DetailCacheDecision(False, "detail_coverage_incomplete")
    node = _source_node(raw)
    if any(
        str(node.get(key) or "").lower() in {"true", "1", "yes", "on"}
        for key in (
            "like_and_view_counts_disabled",
            "hide_like_and_view_counts",
            "like_count_hidden",
            "likes_hidden",
            "comment_count_hidden",
            "comments_count_hidden",
        )
    ):
        return DetailCacheDecision(False, "source_metrics_unknown")
    if (
        not _metric(
            node, "like_count", "likesCount", "likes_count", "likes", "edge_liked_by", "edge_media_preview_like"
        )
        or not _metric(
            node,
            "comment_count",
            "commentsCount",
            "comments_count",
            "comments",
            "edge_media_to_comment",
            "edge_media_to_parent_comment",
        )
        or type(post.get("likes")) is not int
        or post["likes"] < 0
        or type(post.get("comments_count")) is not int
        or post["comments_count"] < 0
    ):
        return DetailCacheDecision(False, "source_metrics_unknown")
    media_count, media = proof.get("media_count"), post.get("media_urls")
    if (
        type(media_count) is not int
        or media_count < 1
        or not isinstance(media, list)
        or len(media) < media_count
        or any(not isinstance(url, str) or not url.strip() for url in media)
    ):
        return DetailCacheDecision(False, "media_coverage_incomplete")
    observed = _time(proof.get("observed_at"))
    current = _time(now)
    if current is None:
        raise ValueError("A valid UTC-compatible cache evaluation clock is required")
    if observed is None:
        return DetailCacheDecision(False, "source_observation_unknown")
    age = current - observed
    if age < timedelta(0):
        return DetailCacheDecision(False, "source_observation_in_future")
    if age >= timedelta(days=intent.metadata_ttl_days):
        return DetailCacheDecision(False, "metadata_stale")
    # This proof attests caption/media and likes/comments together. A separately
    # refreshed view_metrics clock cannot refresh its older likes/comments.
    if age >= timedelta(hours=intent.metrics_ttl_hours):
        return DetailCacheDecision(False, "metrics_stale")
    return DetailCacheDecision(
        True,
        "complete_detail_snapshot_fresh",
        observed,
        {key: str(value) for key, value in canonical_identity.items() if value is not None},
    )


class DetailCache:
    def __init__(self, read_connection: Callable):
        self.read_connection = read_connection

    def lookup(
        self,
        run_id: str,
        account: str,
        *,
        target_key: str,
        now: datetime | None = None,
    ) -> DetailCacheDecision:
        current = _time(now or datetime.now(UTC))
        with self.read_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """select r.intent,to_jsonb(p) as post,to_jsonb(s) as sidecar,
                p.id as post_id,c.id as catalog_post_id,q.id as canonical_post_id
                from social.instagram_ingestion_runs r
                join social.instagram_ingestion_targets t on t.run_id=r.run_id and t.stage='post_details'
                    and t.target_key=%s
                left join social.instagram_posts p on p.shortcode=t.target_key
                    and lower(ltrim(trim(coalesce(nullif(p.source_account,''),p.username)), '@'))=r.account
                left join social.instagram_post_payloads s on s.post_id=p.id
                left join social.instagram_account_catalog_posts c on c.source_id=t.target_key
                    and lower(ltrim(trim(c.source_account), '@'))=r.account
                left join social.social_post_legacy_refs l on l.platform='instagram'
                    and l.legacy_schema='social' and l.legacy_table='instagram_posts'
                    and l.legacy_pk=p.id::text and l.legacy_source_id=t.target_key
                left join social.social_posts q on q.platform='instagram' and q.id=l.post_id
                    and q.source_id=t.target_key
                where r.run_id=%s and r.account=%s""",
                (target_key, run_id, account),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError("Detail target not found for this account and run")
            intent = RunIntent.from_payload(row["intent"])
            if intent.account != account:
                raise ValueError("Frozen intent account differs from the selected run")
            decision = detail_eligibility(
                post=row["post"],
                sidecar=row["sidecar"],
                intent=intent,
                now=current,
                canonical_identity={key: row[key] for key in ("post_id", "catalog_post_id", "canonical_post_id")},
            )
            if not decision.eligible:
                return decision
            cur.execute(
                """select e.*,r.state as source_run_state from social.instagram_ingestion_evidence e
                join social.instagram_ingestion_runs r using(run_id)
                where r.account=%s and e.stage='post_details' and e.target_key=%s
                    and e.captured_at=%s and e.payload is not null
                order by (e.run_id=%s) desc,e.evidence_id limit 8""",
                (account, target_key, decision.observed_at, run_id),
            )
            for retained in cur.fetchall():
                reference = _evidence_reference(retained, target_key=target_key, now=current)
                if reference:
                    return DetailCacheDecision(
                        decision.eligible,
                        decision.reason,
                        decision.observed_at,
                        decision.canonical_identity,
                        reference,
                    )
            return decision


def _evidence_reference(row: Mapping[str, Any], *, target_key: str, now: datetime) -> dict[str, Any] | None:
    if row.get("source_identity") != f"instagram:post:{target_key}":
        return None
    try:
        evidence = Evidence(
            source_identity=row["source_identity"],
            captured_at=row["captured_at"],
            parser_version=row["parser_version"],
            payload_json=row["payload"],
            content_hash=row["content_hash"],
            schema_version=row["schema_version"],
        )
        if _time(row.get("expires_at")) != evidence.expires_at:
            return None
        evidence.read(now=now, pinned=row["source_run_state"] != "completed")
    except (KeyError, TypeError, ValueError, ReplayUnavailable):
        return None
    return {
        "evidence_id": str(row["evidence_id"]),
        "run_id": str(row["run_id"]),
        "stage": Stage.DETAILS.value,
        "target_key": target_key,
        "captured_at": utc_timestamp(evidence.captured_at),
        "content_hash": evidence.content_hash,
        "parser_version": evidence.parser_version,
        "schema_version": evidence.schema_version,
    }
