"""Explicit source parsing and validation, also usable by offline replay.

No browser/session construction, credential repair, source requests or persistence
occurs in this module. Fetch activities retain their source envelope first, then
call this boundary. Parser versions are immutable run/replay inputs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from trr_backend.ingestion.canonical import normalize_comment_source, normalize_post_source
from trr_backend.ingestion.domain import Stage
from trr_backend.ingestion.evidence import Evidence
from trr_backend.socials.instagram.post_normalizer import InstagramComment, InstagramPost

PARSER_VERSION = "instagram-ingestion-v1"


class InvalidSourceError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedSource:
    stage: Stage
    target_key: str
    observed_at: datetime
    source_payload: dict[str, Any]
    post: InstagramPost | None = None
    comments: tuple[InstagramComment, ...] = ()


def parse_evidence(
    evidence: Evidence,
    *,
    account: str,
    stage: Stage,
    target_key: str,
    parser_version: str = PARSER_VERSION,
    pinned: bool = False,
) -> ParsedSource:
    if parser_version != PARSER_VERSION:
        raise InvalidSourceError("Requested parser version is unavailable")
    payload = evidence.read(pinned=pinned)
    if stage in {Stage.LISTING, Stage.DETAILS}:
        post = normalize_post_source(payload, account=account)
        if not post.shortcode or post.shortcode != target_key:
            raise InvalidSourceError("Source post identity does not match its target")
        if not post.source_id:
            raise InvalidSourceError("Source post has no native identity")
        if post.owner is None or not post.owner.username:
            raise InvalidSourceError("Source post has no account identity")
        # A tagged/collaborating account can legitimately list another owner;
        # account membership is decided by listing, not inferred from the author.
        return ParsedSource(stage, target_key, evidence.captured_at, payload, post=post)
    if stage == Stage.COMMENTS:
        nodes = payload.get("comments")
        if not isinstance(nodes, list):
            raise InvalidSourceError("Comment source requires an explicit comments list")
        comments = tuple(normalize_comment_source(node) for node in nodes if isinstance(node, dict))
        if len(comments) != len(nodes) or any(not comment.comment_id for comment in comments):
            raise InvalidSourceError("Comment source contains an invalid identity")
        if len({comment.comment_id for comment in comments}) != len(comments):
            raise InvalidSourceError("Comment source contains duplicate top-level identities")
        # An empty list is valid evidence, but is not proof of exhausted pages.
        return ParsedSource(stage, target_key, evidence.captured_at, payload, comments=comments)
    raise InvalidSourceError("Media replay requires its separate asset manifest parser")
