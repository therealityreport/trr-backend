"""Durable offline replay commands, separate from worker completion receipts.

A command transaction binds account/run/evidence, serializes the idempotency key,
then commits canonical effects, an apply receipt and its response together. No
worker lease or target-completion mutation is part of this operation.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

from psycopg2.extras import RealDictCursor

from api.schemas.v2.instagram_ingestion import InstagramReplayResultV2, InstagramReplayV2
from trr_backend.ingestion.application import validate_detail
from trr_backend.ingestion.canonical import CanonicalWriteError, OlderObservation, write_comments, write_post
from trr_backend.ingestion.comments import CommentPage, CommentSourceError
from trr_backend.ingestion.domain import Conflict, ReplayUnavailable, RunIntent, fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.listing import _posted_at
from trr_backend.ingestion.persistence import Store
from trr_backend.ingestion.providers import InvalidSourceError, ParsedSource
from trr_backend.ingestion.replay import MEDIA_APPLY_LIMITATION, ListingReplay, MediaReplay, prepare_replay


class ReplayCommands:
    def __init__(
        self,
        store: Store,
        *,
        detail_writer: Callable = write_post,
        comment_writer: Callable = write_comments,
        media_writer: Callable | None = None,
    ):
        self.store = store
        self.detail_writer = detail_writer
        self.comment_writer = comment_writer
        self.media_writer = media_writer

    def __call__(
        self, handle: str, run_id: UUID, payload: InstagramReplayV2, *, idempotency_key: str, initiated_by: str
    ) -> InstagramReplayResultV2:
        account = RunIntent(account=handle).account
        key = idempotency_key.strip()
        if not key or len(idempotency_key) > 200:
            raise ValueError("Idempotency key must contain 1 to 200 characters")
        request_hash = fingerprint({"command": "replay", "run_id": str(run_id), **payload.model_dump(mode="json")})
        with self.store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", (f"instagram-command:{account}:{key}",))
            # Compact evidence identity remains available after payload expiry;
            # exact idempotent repeats can return the original stored response.
            cur.execute(
                """select e.*,r.state<>'completed' as pinned,r.intent as run_intent
                from social.instagram_ingestion_evidence e
                join social.instagram_ingestion_runs r using(run_id)
                where e.evidence_id=%s and e.run_id=%s and r.account=%s for share of e,r""",
                (str(payload.evidence_id), str(run_id), account),
            )
            bound = cur.fetchone()
            if not bound:
                raise LookupError("Replay evidence does not belong to this account and run")
            cur.execute(
                """select command_hash,result from social.instagram_ingestion_commands
                where account=%s and idempotency_key=%s""",
                (account, key),
            )
            previous = cur.fetchone()
            if previous:
                if previous["command_hash"] != request_hash:
                    raise Conflict("Idempotency key already belongs to a different command")
                return InstagramReplayResultV2.model_validate(previous["result"])
            if bound["payload"] is None:
                raise ReplayUnavailable("Retained source payload has expired")
            evidence = Evidence(
                source_identity=bound["source_identity"],
                captured_at=bound["captured_at"],
                parser_version=bound["parser_version"],
                payload_json=bound["payload"],
                content_hash=bound["content_hash"],
                schema_version=bound["schema_version"],
            )
            retained = {
                "evidence": evidence,
                "stage": bound["stage"],
                "target_key": bound["target_key"],
                "run_id": str(run_id),
                "pinned": bound["pinned"],
            }
            try:
                result, parsed = prepare_replay(
                    str(payload.evidence_id), account, retained, parser_version=payload.parser_version
                )
            except (InvalidSourceError, CommentSourceError) as error:
                raise ReplayUnavailable("Retained evidence cannot satisfy this replay parser") from error
            if payload.apply:
                try:
                    source = None
                    if isinstance(parsed, ParsedSource):
                        source = dict(parsed.source_payload)
                        source["detail_snapshot"] = validate_detail(parsed)
                    identity, writes = self._apply(conn, cur, account, str(run_id), bound, parsed, source)
                except InvalidSourceError as error:
                    raise ReplayUnavailable("Retained source evidence lacks required apply coverage") from error
                except OlderObservation as error:
                    raise Conflict("Replay capture predates a newer canonical observation") from error
                except CanonicalWriteError as error:
                    raise Conflict("Replay cannot safely preserve the canonical data contract") from error
                receipt_id = str(uuid4())
                cur.execute(
                    """insert into social.instagram_ingestion_replay_receipts
                    (receipt_id,account,idempotency_key,run_id,evidence_id,stage,target_key,captured_at,
                     content_hash,capture_parser_version,parser_version,schema_version,canonical_identity,writes,initiated_by)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)""",
                    (
                        receipt_id,
                        account,
                        key,
                        str(run_id),
                        str(payload.evidence_id),
                        bound["stage"],
                        bound["target_key"],
                        evidence.captured_at,
                        evidence.content_hash,
                        evidence.parser_version,
                        payload.parser_version,
                        evidence.schema_version,
                        stable_json(identity),
                        writes,
                        initiated_by,
                    ),
                )
                result.update(mode="apply", writes=writes, receipt_id=receipt_id)
            typed = InstagramReplayResultV2.model_validate(result)
            cur.execute(
                """insert into social.instagram_ingestion_commands
                (account,idempotency_key,command_hash,run_id,result) values(%s,%s,%s,%s,%s::jsonb)""",
                (account, key, request_hash, str(run_id), stable_json(typed.model_dump(mode="json"))),
            )
            return typed

    def _apply(self, conn, cur, account, run_id, bound, parsed, source) -> tuple[dict[str, Any], int]:
        if isinstance(parsed, MediaReplay):
            if parsed.result is None:
                raise ReplayUnavailable(MEDIA_APPLY_LIMITATION)
            if self.media_writer is None:
                from trr_backend.ingestion.media import write_media

                writer = write_media
            else:
                writer = self.media_writer
            identity = writer(
                conn,
                run_id=run_id,
                account=account,
                target_key=bound["target_key"],
                observed_at=parsed.observed_at,
                result=parsed.result,
            )
            return identity, 1 + len(parsed.result.snapshot.comments) + len(parsed.result.avatars)
        if isinstance(parsed, ListingReplay):
            intent = RunIntent.from_payload(bound["run_intent"])
            identities, skipped = [], []
            for post in sorted(parsed.posts, key=lambda item: item.target_key):
                posted = _posted_at(post.source_payload)
                if posted is None and (intent.date_start or intent.date_end):
                    raise InvalidSourceError("Bounded listing replay requires its source post timestamps")
                if posted is not None and (
                    (intent.date_start and posted < intent.date_start)
                    or (intent.date_end and posted >= intent.date_end)
                ):
                    skipped.append(post.target_key)
                    continue
                identities.append(
                    self.detail_writer(
                        conn,
                        run_id=run_id,
                        account=account,
                        observed_at=parsed.observed_at,
                        normalized=post.post,
                        source_payload=post.source_payload,
                    )
                )
            return {"posts": identities, "skipped_outside_window": skipped}, len(identities)
        if isinstance(parsed, ParsedSource):
            identity = self.detail_writer(
                conn,
                run_id=run_id,
                account=account,
                observed_at=parsed.observed_at,
                normalized=parsed.post,
                source_payload=source,
            )
            return identity, 1
        if not isinstance(parsed, CommentPage):
            raise ReplayUnavailable("Unsupported replay capture")
        cur.execute("select id from social.instagram_posts where shortcode=%s", (bound["target_key"],))
        post = cur.fetchone()
        if not post:
            raise Conflict("Comment replay requires its existing canonical parent post")
        from trr_backend.ingestion.media import invalidate_media_inventory

        if parsed.comments:
            invalidate_media_inventory(cur, run_id, bound["target_key"])
        identity = self.comment_writer(
            conn,
            run_id=run_id,
            account=account,
            post_id=str(post["id"]),
            observed_at=parsed.observed_at,
            comments=parsed.comments,
            source_payloads=parsed.source_payloads,
        )
        # Each page stays partial. Missing/deleted marking belongs to a proved
        # full observation, and is deliberately absent from replay commands.
        return identity, len(identity["observed_comment_ids"])
