"""Fetch -> retain -> normalize -> validate -> canonical write + receipt.

The injected fetcher owns transport deadlines and identity-wide pressure. No
database transaction spans that call. This service cannot repair credentials.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from trr_backend.ingestion.canonical import write_post
from trr_backend.ingestion.domain import Stage, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, ParsedSource, parse_evidence


def validate_detail(parsed: ParsedSource) -> dict[str, Any]:
    """Require explicit native field coverage; preserve unknown metrics as unknown."""
    post = parsed.post
    if post is None:
        raise InvalidSourceError("Detail evidence is not a post")
    node = post.raw_data

    def metric(*keys: str) -> bool:
        for key in keys:
            value = node.get(key)
            if isinstance(value, dict):
                value = value.get("count")
            if type(value) is int and value >= 0:
                return True
        return False

    caption_present = any(key in node for key in ("caption", "edge_media_to_caption"))
    likes_present = metric("like_count", "edge_media_preview_like", "edge_liked_by")
    comments_present = metric("comment_count", "edge_media_to_comment", "edge_media_to_parent_comment")
    child_count = node.get("carousel_media_count")
    if child_count is None:
        child_count = len(post.child_posts) or 1
    media_complete = type(child_count) is int and child_count > 0 and len(post.media_urls) >= child_count
    if not all((caption_present, likes_present, comments_present, media_complete)):
        raise InvalidSourceError("Detail response lacks required source coverage")
    return {
        "version": 1,
        "coverage_version": 2,
        "source": "instagram_response",
        "observed_at": utc_timestamp(parsed.observed_at),
        "caption_present": caption_present,
        "likes_present": likes_present,
        "comments_present": comments_present,
        "media_complete": media_complete,
        "media_count": child_count,
    }


class PostActivity:
    def __init__(self, store: Store, *, writer: Callable = write_post):
        self.store = store
        self.writer = writer

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        fetch: Callable[..., dict[str, Any]],
        stage: Stage = Stage.DETAILS,
    ) -> dict[str, Any]:
        if stage not in {Stage.LISTING, Stage.DETAILS}:
            raise ValueError("Post activity only accepts listing or details")
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "stage": stage,
            "target_key": target_key,
            "owner_token": owner_token,
        }
        self.store.acquire(**owned)
        # Callbacks provide cancellation and durable request accounting to the
        # transport. They must run before each actual request, including retries.
        retained: list[tuple[Evidence, str]] = []

        def observe(kind, payload):
            evidence = Evidence.capture(
                source_identity=f"instagram:post:{target_key}",
                captured_at=datetime.now(UTC),
                parser_version=PARSER_VERSION,
                payload=payload,
            )
            evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
            retained.append((evidence, evidence_id))

        payload = fetch(
            check_cancelled=lambda: self.store.check_owner(**owned),
            on_request=lambda: self.store.check_owner(**owned, record_request=True),
            source_observer=observe,
        )
        # Match the returned raw payload to a pre-parser capture, including when
        # a provider tried multiple endpoints. Legacy injected callbacks without
        # an observer still retain their raw result before this parser runs.
        candidate = Evidence.capture(
            source_identity=f"instagram:post:{target_key}",
            captured_at=datetime.now(UTC),
            parser_version=PARSER_VERSION,
            payload=payload,
        )
        matching = [item for item in retained if item[0].content_hash == candidate.content_hash]
        if not matching:
            observe("details", payload)
            matching = retained[-1:]
        evidence, evidence_id = matching[-1]
        parsed = parse_evidence(evidence, account=account, stage=stage, target_key=target_key, pinned=True)
        source = dict(parsed.source_payload)
        if stage == Stage.DETAILS:
            source["detail_snapshot"] = validate_detail(parsed)

        def write(conn):
            return self.writer(
                conn,
                run_id=run_id,
                account=account,
                observed_at=parsed.observed_at,
                normalized=parsed.post,
                source_payload=source,
            )

        return self.store.commit_target(
            **owned, observed_at=parsed.observed_at, evidence_id=evidence_id, write_canonical=write
        )
