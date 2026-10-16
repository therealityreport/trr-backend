"""Retain native listing evidence, then atomically commit a bounded discovery page.

Transport is injected. No legacy orchestration, session construction or network
requests occur here. Discovery ends at source exhaustion or a recorded bounded
window heuristic; the latter is never represented as provider exhaustion.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from psycopg2.extras import RealDictCursor, execute_values

from trr_backend.ingestion.application import validate_detail
from trr_backend.ingestion.canonical import normalize_post_source, write_post
from trr_backend.ingestion.domain import (
    Conflict,
    LostOwnership,
    RunIntent,
    Stage,
    fingerprint,
    stable_json,
    utc_timestamp,
)
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store, _row
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, ParsedSource
from trr_backend.socials.instagram.graphql_response import ROOT_FIELD, classify_graphql_response
from trr_backend.socials.instagram.post_normalizer import InstagramPost


@dataclass(frozen=True)
class ListingPage:
    posts: tuple[dict[str, Any], ...]
    has_next_page: bool
    end_cursor: str | None


def parse_listing_page(payload: dict[str, Any]) -> ListingPage:
    """Parse the maintained native GraphQL connection, without transport defaults."""
    data = payload.get("data")
    connection = data.get(ROOT_FIELD) if isinstance(data, dict) else None
    if not isinstance(connection, dict):
        raise InvalidSourceError("Native listing connection is missing")
    edges = connection.get("edges")
    info = connection.get("page_info")
    if not isinstance(edges, list) or not isinstance(info, dict) or type(info.get("has_next_page")) is not bool:
        raise InvalidSourceError("Listing requires edges and explicit pagination evidence")
    posts = []
    for edge in edges:
        node = edge.get("node") if isinstance(edge, dict) else None
        if not isinstance(node, dict) or not node:
            raise InvalidSourceError("Listing contains an invalid post node")
        posts.append(node)
    end_cursor = info.get("end_cursor")
    if end_cursor is not None and (not isinstance(end_cursor, str) or not end_cursor.strip()):
        raise InvalidSourceError("Listing cursor must be a nonempty string or null")
    if info["has_next_page"] and end_cursor is None:
        raise InvalidSourceError("More listing pages require a cursor")
    decision = classify_graphql_response(payload)
    if not decision.accepted:
        raise InvalidSourceError("Listing source response is unresolved: " + str(decision.reason))
    return ListingPage(tuple(posts), info["has_next_page"], end_cursor)


def _posted_at(node: dict[str, Any]) -> datetime | None:
    value = next(
        (
            node[key]
            for key in ("taken_at", "taken_at_timestamp", "timestamp", "date_time", "datePublished")
            if node.get(key) not in (None, "", 0, "0")
        ),
        None,
    )
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            raise ValueError("Boolean timestamp")
        if isinstance(value, (int, float)) or str(value).isdigit():
            return datetime.fromtimestamp(float(value), UTC)
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("Missing timezone")
        return result.astimezone(UTC)
    except (ValueError, OverflowError, OSError) as exc:
        raise InvalidSourceError("Listing post timestamp is invalid") from exc


def page_target(cursor: str) -> str:
    return "page:" + fingerprint({"cursor": cursor})


def _date_start_reached(intent: RunIntent, posts: list[tuple[dict[str, Any], datetime | None]]) -> bool:
    """Strengthened maintained three-older policy, evaluated only after the page.

    Missing pin flags retain legacy ordinary-post behavior; this is an ordering
    heuristic, not provider-certified exhaustion. Known pins cannot establish
    the boundary. Explicit malformed/conflicting flags disqualify the page.
    """
    if intent.date_start is None:
        return False
    candidates = []
    for node, posted in posts:
        flags = [node[key] for key in ("is_pinned", "isPinned") if key in node]
        if any(type(flag) is not bool for flag in flags) or (flags and any(flag != flags[0] for flag in flags)):
            return False
        if flags and flags[0]:
            continue
        if posted is None or posted >= intent.date_start:
            return False
        candidates.append(posted)
    return len(candidates) >= 3 and all(
        later <= earlier for earlier, later in zip(candidates, candidates[1:], strict=False)
    )


def _listing_post_source(
    node: dict[str, Any],
    normalized: InstagramPost,
    *,
    evidence: Evidence,
    evidence_id: str,
    run_id: str,
    page_key: str,
) -> dict[str, Any]:
    """A complete native listing observation can also establish detail coverage.

    Keep partial nodes as ordinary listing observations. The existing cache owns
    TTL/force/metric visibility decisions, and its worker settles detail targets.
    No new detail capture or detail-stage evidence is invented here.
    """
    source = dict(node)
    parsed = ParsedSource(Stage.LISTING, normalized.shortcode, evidence.captured_at, source, post=normalized)
    try:
        proof = validate_detail(parsed)
    except InvalidSourceError:
        return source
    proof["evidence_reference"] = {
        "evidence_id": evidence_id,
        "run_id": run_id,
        "stage": Stage.LISTING.value,
        "target_key": page_key,
        "captured_at": utc_timestamp(evidence.captured_at),
        "content_hash": evidence.content_hash,
        "parser_version": evidence.parser_version,
        "schema_version": evidence.schema_version,
    }
    source["detail_snapshot"] = proof
    return source


class ListingActivity:
    def __init__(self, store: Store, *, writer: Callable = write_post, clock: Callable = lambda: datetime.now(UTC)):
        self.store = store
        self.writer = writer
        self.clock = clock

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        fetch: Callable[..., dict[str, Any]],
    ) -> dict[str, Any]:
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "stage": Stage.LISTING,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        # A duplicate completed delivery returns its original atomic receipt; it
        # cannot re-fetch or skip the successor created in the same transaction.
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            run = Store._run(cur, run_id, account)
            if run["generation"] != generation or run["state"] not in {"queued", "running", "completed"}:
                raise LostOwnership("Listing generation is no longer current")
            prior = _row(
                cur,
                """select * from social.instagram_ingestion_receipts
                where run_id=%s and stage='listing' and target_key=%s and owner_token=%s""",
                (run_id, target_key, owner_token),
            )
            if prior:
                return prior
            intent = RunIntent.from_payload(run["intent"])
        target = self.store.acquire(**owned)
        checkpoint = target["checkpoint"]
        if "cursor" not in checkpoint or (
            checkpoint["cursor"] is not None and not isinstance(checkpoint["cursor"], str)
        ):
            raise InvalidSourceError("Listing target has no valid cursor checkpoint")
        cursor = checkpoint["cursor"]
        if target_key != ("page:root" if cursor is None else page_target(cursor)):
            raise Conflict("Listing page identity differs from its cursor checkpoint")
        retained: dict[str, tuple[Evidence, str]] = {}

        def source_observer(payload: dict[str, Any], metadata: dict[str, Any] | None = None):
            # Metadata may contain transport/session secrets; retain typed source
            # content only, before transport parsing or any normalization.
            evidence = Evidence.capture(
                source_identity=f"instagram:listing:{account}:{target_key}",
                captured_at=self.clock(),
                parser_version=PARSER_VERSION,
                payload=payload,
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
            retained[evidence.content_hash] = (evidence, evidence_id)

        payload = fetch(
            cursor=cursor,
            pre_source_request=lambda: self.store.check_owner(**owned, record_request=True),
            source_observer=source_observer,
        )
        if not isinstance(payload, dict):
            raise InvalidSourceError("Listing transport must return its observed native response mapping")
        returned = Evidence.capture(
            source_identity="return-match", captured_at=self.clock(), parser_version=PARSER_VERSION, payload=payload
        )
        if returned.content_hash not in retained:
            raise InvalidSourceError("Listing response was not retained before parsing")
        evidence, evidence_id = retained[returned.content_hash]
        decision = classify_graphql_response(payload)
        if not decision.accepted and decision.reason not in {"graphql_empty_connection", "graphql_invalid_connection"}:
            raise InvalidSourceError("Listing source reported an unsuccessful response")
        page = parse_listing_page(evidence.read(pinned=True))
        if cursor is not None and page.end_cursor == cursor:
            raise InvalidSourceError("Listing cursor repeated; exhaustion is unproven")
        selected = {}
        seen_post_keys = set()
        dated_posts = []
        for node in page.posts:
            normalized = normalize_post_source(node, account=account)
            if (
                not normalized.shortcode
                or not normalized.source_id
                or normalized.owner is None
                or not normalized.owner.username
            ):
                raise InvalidSourceError("Listing post identity or owner is missing")
            posted = _posted_at(node)
            if posted is None and (intent.date_start or intent.date_end):
                raise InvalidSourceError("Bounded listing requires a source post timestamp")
            if normalized.shortcode in seen_post_keys:
                raise InvalidSourceError("Listing contains duplicate post identities")
            seen_post_keys.add(normalized.shortcode)
            dated_posts.append((node, posted))
            if posted is not None and (
                (intent.date_start and posted < intent.date_start) or (intent.date_end and posted >= intent.date_end)
            ):
                continue
            selected[normalized.shortcode] = (node, normalized)

        provider_exhausted = not page.has_next_page
        date_start_reached = _date_start_reached(intent, dated_posts)
        discovery_complete = provider_exhausted or date_start_reached
        coverage = {
            "stop_reason": "provider_exhausted"
            if provider_exhausted
            else "date_start_reached"
            if date_start_reached
            else None,
            "coverage_basis": "provider_exhaustion"
            if provider_exhausted
            else "bounded_window_heuristic_v1"
            if date_start_reached
            else None,
            "provider_exhausted": provider_exhausted,
            "evidence_hash": evidence.content_hash,
        }

        def commit_page(conn):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                manifest = _row(
                    cur,
                    """select * from social.instagram_ingestion_manifests
                    where run_id=%s and stage='listing' for update""",
                    (run_id,),
                )
                if not manifest or manifest["discovery_complete"] or manifest["cursor_state"].get("cursor") != cursor:
                    raise Conflict("Listing cursor is no longer current")
                # Detect A -> B -> A loops, including a false terminal response.
                if page.end_cursor is not None:
                    seen = _row(
                        cur,
                        """select target_key from social.instagram_ingestion_targets
                        where run_id=%s and stage='listing' and checkpoint->>'cursor'=%s limit 1""",
                        (run_id, page.end_cursor),
                    )
                    if seen:
                        raise InvalidSourceError("Listing cursor was already scheduled; exhaustion is unproven")
                identities = []
                downstream_targets = []
                for shortcode, (node, normalized) in sorted(selected.items()):
                    identities.append(
                        self.writer(
                            conn,
                            run_id=run_id,
                            account=account,
                            observed_at=evidence.captured_at,
                            normalized=normalized,
                            source_payload=_listing_post_source(
                                node,
                                normalized,
                                evidence=evidence,
                                evidence_id=evidence_id,
                                run_id=run_id,
                                page_key=target_key,
                            ),
                        )
                    )
                    for stage in intent.selected_tasks:
                        budget = {
                            Stage.DETAILS: 3,
                            Stage.COMMENTS: intent.comments_max_attempts,
                            Stage.MEDIA: intent.media_max_attempts,
                        }[stage]
                        downstream_targets.append((run_id, stage.value, shortcode, budget))
                if downstream_targets:
                    execute_values(
                        cur,
                        """insert into social.instagram_ingestion_targets
                        (run_id,stage,target_key,max_attempts) values %s on conflict do nothing""",
                        downstream_targets,
                        page_size=len(downstream_targets),
                    )
                next_target = None
                if not discovery_complete:
                    next_target = page_target(page.end_cursor)
                    cur.execute(
                        """insert into social.instagram_ingestion_targets
                        (run_id,stage,target_key,max_attempts,checkpoint) values(%s,'listing',%s,%s,%s::jsonb)""",
                        (run_id, next_target, intent.listing_max_attempts, stable_json({"cursor": page.end_cursor})),
                    )
                cursor_state = {
                    "cursor": page.end_cursor if page.end_cursor is not None else cursor,
                    "last_page": target_key,
                    **coverage,
                }
                cur.execute(
                    """update social.instagram_ingestion_manifests set cursor_state=%s::jsonb
                    where run_id=%s and stage='listing' """,
                    (stable_json(cursor_state), run_id),
                )
                cur.execute(
                    """insert into social.instagram_ingestion_discovery_pages(run_id,stage,page_id,request_hash)
                    values(%s,'listing',%s,%s)""",
                    (run_id, target_key, evidence.content_hash),
                )
                if discovery_complete:
                    cur.execute(
                        """update social.instagram_ingestion_manifests set discovery_complete=true,
                        sealed_at=clock_timestamp() where run_id=%s and stage=any(%s)""",
                        (run_id, [Stage.LISTING.value, *(stage.value for stage in intent.selected_tasks)]),
                    )
                return {
                    "page_id": target_key,
                    "post_keys": sorted(selected),
                    "next_target_key": next_target,
                    "discovery_complete": discovery_complete,
                    **coverage,
                    "posts": identities,
                }

        return self.store.commit_target(
            **owned, observed_at=evidence.captured_at, evidence_id=evidence_id, write_canonical=commit_page
        )
