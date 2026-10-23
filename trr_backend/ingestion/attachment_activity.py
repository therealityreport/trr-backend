"""Fenced native comment attachments, without claiming full post media coverage."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from trr_backend.ingestion.canonical import CanonicalWriteError
from trr_backend.ingestion.domain import Stage, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import (
    MediaIncomplete,
    MediaOwnershipCheck,
    MediaSnapshot,
    _locked,
    _newer,
    source_reference,
    write_comment_media,
)

COMMENT_ATTACHMENT_RESULT_VERSION = "instagram-comment-attachments-v1"


def _selected(snapshot: MediaSnapshot, comment_ids: tuple[str, ...]) -> tuple:
    snapshot.validate()
    if (
        snapshot.unavailable is not None
        or not isinstance(comment_ids, tuple)
        or not 1 <= len(comment_ids) <= 100
        or any(not isinstance(identity, str) or not identity for identity in comment_ids)
        or len(set(comment_ids)) != len(comment_ids)
    ):
        raise MediaIncomplete("attachment_selection_invalid")
    selected = tuple(
        sorted(
            (comment for comment in snapshot.comments if comment.comment_id in comment_ids),
            key=lambda comment: comment.comment_id,
        )
    )
    proof_ids = {comment.comment_id for comment in snapshot.comments if comment.attachment_refresh is not None}
    if {comment.comment_id for comment in selected} != set(comment_ids) or proof_ids != set(comment_ids):
        raise MediaIncomplete("attachment_selection_proof_mismatch")
    for comment in selected:
        if comment.attachment_refresh is None:
            raise MediaIncomplete("attachment_selection_proof_missing")
        comment.attachment_refresh.validate()
    return selected


def write_comment_attachments(
    conn: Any,
    *,
    run_id: str,
    account: str,
    target_key: str,
    observed_at: datetime,
    snapshot: MediaSnapshot,
    comment_ids: tuple[str, ...],
    comment_fields: Mapping[str, Mapping],
) -> dict[str, Any]:
    """The caller supplies Store's fence; this callback never performs transport."""
    selected = _selected(snapshot, comment_ids)
    if (
        conn is None
        or conn.autocommit
        or snapshot.shortcode != target_key
        or observed_at != max(comment.attachment_refresh.captured_at for comment in selected)
        or set(comment_fields) != set(comment_ids)
    ):
        raise CanonicalWriteError("Attachment write differs from its fenced source snapshot")
    for fields in comment_fields.values():
        if (
            not isinstance(fields, Mapping)
            or set(fields) != {"hosted_media_urls", "media_mirror_status", "media_mirror_error"}
            or fields["media_mirror_status"] != "mirrored"
            or fields["media_mirror_error"] is not None
            or not isinstance(fields["hosted_media_urls"], list)
            or len(fields["hosted_media_urls"]) != 1
            or source_reference(fields["hosted_media_urls"][0]) != fields["hosted_media_urls"][0]
        ):
            raise CanonicalWriteError("Attachment result contains unsupported canonical fields")
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:post:" + target_key,))
    post = _locked(conn, "instagram_posts", {"id": snapshot.post_id, "shortcode": target_key})
    canonical = _locked(
        conn, "social_posts", {"id": snapshot.canonical_post_id, "platform": "instagram", "source_id": target_key}
    )
    if (
        not post
        or not canonical
        or (post.get("source_account") or post.get("username") or "").lower() != account.lower()
    ):
        raise CanonicalWriteError("Attachment post identity or account differs from snapshot")
    _newer(snapshot.observed_at, post, canonical)
    write_comment_media(
        conn,
        run_id=run_id,
        post_id=snapshot.post_id,
        observed_at=snapshot.observed_at,
        comments=selected,
        comment_fields=comment_fields,
    )
    return {
        "kind": "comment_attachments",
        "post_id": snapshot.post_id,
        "canonical_post_id": snapshot.canonical_post_id,
        "shortcode": target_key,
        "comment_ids": [comment.comment_id for comment in selected],
        "attachment_evidence_sha256": [comment.attachment_refresh.evidence_sha256 for comment in selected],
    }


class CommentAttachmentActivity:
    def __init__(
        self, store, *, storage, writer: Callable = write_comment_attachments, clock: Callable = time.monotonic
    ):
        self.store, self.storage, self.writer, self.clock = store, storage, writer, clock

    def execute(
        self,
        *,
        run_id: str,
        account: str,
        generation: int,
        target_key: str,
        owner_token: str,
        load: Callable,
        comment_ids: tuple[str, ...],
    ) -> dict[str, Any]:
        owned = {
            "run_id": run_id,
            "account": account,
            "generation": generation,
            "target_key": target_key,
            "owner_token": owner_token,
            "stage": Stage.MEDIA,
        }
        acquired = self.store.acquire(**owned)
        remaining = (acquired["owner_expires_at"] - datetime.now(UTC)).total_seconds() - 2
        check = MediaOwnershipCheck(
            lambda: self.store.check_owner(**owned), deadline=self.clock() + min(590, remaining), clock=self.clock
        )

        def request():
            check.check_deadline()
            self.store.check_owner(**owned, record_request=True)

        check()
        snapshot = load(check_cancelled=check, on_request=request)
        selected = _selected(snapshot, comment_ids)
        if snapshot.shortcode != target_key:
            raise MediaIncomplete("attachment_target_identity_mismatch")
        captured_at = max(comment.attachment_refresh.captured_at for comment in selected)
        for comment in selected:
            self.store.retain_evidence(**owned, evidence=comment.attachment_refresh.evidence())
        week = f"week-{snapshot.week_index}" if snapshot.week_index is not None else "week-unknown"
        root = f"social/instagram/{snapshot.show_id}/{snapshot.season_number}/{week}"
        if self.storage.prefix:
            root = self.storage.prefix + "/" + root
        fields, retained = {}, []
        for comment in selected:
            proof = comment.attachment_refresh
            entry, _ = self.storage.host(
                proof.source_url,
                root=root + "/" + comment.storage_source_key,
                name="media-01",
                check=check,
                request=request,
            )
            if (
                entry.get("content_type") != "image/gif"
                or not re.fullmatch(r"[a-f0-9]{64}", entry.get("sha256") or "")
                or type(entry.get("bytes")) is not int
                or entry["bytes"] <= 0
                or source_reference(entry["url"]) != entry["url"]
                or not isinstance(entry.get("storage_key"), str)
                or not entry["storage_key"]
            ):
                raise MediaIncomplete("attachment_hosted_proof_invalid")
            fields[comment.comment_id] = {
                "hosted_media_urls": [entry["url"]],
                "media_mirror_status": "mirrored",
                "media_mirror_error": None,
            }
            retained.append(
                {
                    "comment_id": comment.comment_id,
                    "row_id": comment.row_id,
                    "attachment_refresh": proof.to_payload(),
                    **fields[comment.comment_id],
                    **{key: entry[key] for key in ("sha256", "storage_key", "bytes", "content_type")},
                }
            )
        check()
        evidence = Evidence.capture(
            source_identity=f"instagram:comment-attachments:{target_key}",
            captured_at=captured_at,
            parser_version=COMMENT_ATTACHMENT_RESULT_VERSION,
            payload={
                "kind": "comment_attachments",
                "version": COMMENT_ATTACHMENT_RESULT_VERSION,
                "post_id": snapshot.post_id,
                "canonical_post_id": snapshot.canonical_post_id,
                "shortcode": target_key,
                "captured_at": utc_timestamp(captured_at),
                "comments": retained,
            },
        )
        evidence_id = self.store.retain_evidence(**owned, evidence=evidence)
        return self.store.commit_target(
            **owned,
            observed_at=captured_at,
            evidence_id=evidence_id,
            write_canonical=lambda conn: self.writer(
                conn,
                run_id=run_id,
                account=account,
                target_key=target_key,
                observed_at=captured_at,
                snapshot=snapshot,
                comment_ids=comment_ids,
                comment_fields=fields,
            ),
        )
