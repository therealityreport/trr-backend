"""Explicit logged-in WWW comments using durable native page checkpoints."""

from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.ingestion.authenticated_relay import AuthenticatedRelayPageClient
from trr_backend.ingestion.authenticated_relay_contract import AUTH_RELAY_FAMILY, BINDING_KEYS, valid_provenance
from trr_backend.ingestion.comments import (
    COMMENT_PARSER_VERSION,
    CommentFetchSummary,
    CommentSourceError,
    CursorStalledError,
    _advance,
    _progress,
    parse_comment_evidence,
)
from trr_backend.ingestion.evidence import Evidence
from trr_backend.socials.instagram.child_reply_probe import media_id_from_shortcode


def _resume_state(checkpoint, media_id):
    if checkpoint.get("top_level_checkpoint") or checkpoint.get("reply_checkpoints"):
        raise CommentSourceError("Authenticated Relay cannot consume REST cursors")
    if checkpoint.get("public_lane_restarts"):
        raise CommentSourceError("Authenticated Relay cannot consume public lane restarts")
    state = _progress(checkpoint)
    if state["lanes"] and (state.get("adapter_family") != AUTH_RELAY_FAMILY or state.get("adapter_version") != 1):
        raise CommentSourceError("Authenticated Relay cannot resume another adapter family")
    if state.get("adapter_family") not in (None, AUTH_RELAY_FAMILY):
        raise CommentSourceError("Authenticated Relay checkpoint family differs")
    for key, lane in state["lanes"].items():
        if lane.get("repeated"):
            raise CursorStalledError("Authenticated Relay cursor stalled")
        if not lane.get("relay_doc_id"):
            if key == "__top__":
                raise CommentSourceError("Authenticated Relay parent query provenance is missing")
            continue  # Embedded children have no established child operation.
        from trr_backend.ingestion.authenticated_relay_contract import operation

        initial = lane["relay_doc_id"] == operation(parent=key != "__top__", initial=True)[0]
        cursors = lane.get("cursors")
        if not isinstance(cursors, list) or any(not isinstance(item, str) or not item.strip() for item in cursors):
            raise CommentSourceError("Authenticated Relay cursors are malformed")
        proof = dict(
            lane,
            adapter_family=AUTH_RELAY_FAMILY,
            adapter_version=1,
            source="root" if initial else "resume",
            request_cursor=None if initial else "retained",
        )
        if not valid_provenance(proof, parent=key != "__top__") or lane.get("media_id") != media_id:
            raise CommentSourceError("Authenticated Relay query provenance differs")
        if not lane.get("exhausted") and (lane.get("cursor_param") != "after" or not cursors):
            raise CommentSourceError("Authenticated Relay continuation has no native cursor")
    return state


class AuthenticatedRelayCommentSource:
    def __init__(
        self,
        *,
        raw_cookies,
        cookies=None,
        browser_account_id=None,
        expected_count=None,
        client_factory=AuthenticatedRelayPageClient,
    ):
        if expected_count is not None and (type(expected_count) is not int or expected_count < 0):
            raise ValueError("Expected count must be nonnegative or unknown")
        self.cookies = dict(raw_cookies)
        self.expected_count = expected_count
        self.client_factory = client_factory

    def __call__(
        self,
        *,
        shortcode,
        checkpoint,
        resume_inventory=(),
        source_observer,
        pre_source_request,
        source_failure_observer=None,
    ):
        media_id = media_id_from_shortcode(shortcode)
        state = _resume_state(checkpoint, media_id)
        started = datetime.now(UTC)
        expected = self.expected_count if self.expected_count is not None else state.get("expected")

        def record(payload):
            evidence = Evidence.capture(
                source_identity=f"instagram:comments:{shortcode}:graphql",
                captured_at=datetime.now(UTC),
                parser_version=COMMENT_PARSER_VERSION,
                payload=payload,
            )
            page = parse_comment_evidence(evidence, pinned=True)
            _advance(state, page, scan_started_at=started)
            if any(lane.get("repeated") for lane in state["lanes"].values()):
                raise CursorStalledError("Authenticated Relay cursor stalled")

        def pending(parent):
            lane = state["lanes"].get(parent) or {}
            count = state["observed"][parent]["expected_replies"]
            if count == 0:
                return False
            children = sum(item["parent"] == parent for item in state["observed"].values())
            return not (
                lane.get("relay_doc_id")
                and lane.get("exhausted") is True
                and lane.get("started_from_root") is True
                and (count is None or children >= count)
            )

        top = state["lanes"].get("__top__") or {}
        parents = [key for key, item in state["observed"].items() if item["parent"] is None]
        if top.get("exhausted") is True and not any(pending(parent) for parent in parents):
            return CommentFetchSummary(expected_count=expected, full_observation=top.get("started_from_root") is True)

        client = self.client_factory(
            cookies=self.cookies,
            pre_source_request=pre_source_request,
            source_observer=source_observer,
            source_failure_observer=source_failure_observer,
        )
        try:
            client.bootstrap(shortcode)
            # A resumed query must retain the same media and provider semantics.
            context = client._context
            expected_binding = {
                "media_id": media_id,
                "relay_logged_in": context.logged_in,
                "relay_threads_style": context.threads_style,
            }
            if any(
                lane.get("relay_doc_id") and any(lane.get(key) != expected_binding[key] for key in BINDING_KEYS)
                for lane in state["lanes"].values()
            ):
                raise CommentSourceError("Authenticated Relay provider semantics changed")
            if not top:
                record(client.root_page())
            while not state["lanes"]["__top__"]["exhausted"]:
                record(client.page(media_id=media_id, cursor=state["lanes"]["__top__"]["cursors"][-1]))
            for parent in [key for key, item in state["observed"].items() if item["parent"] is None]:
                if not pending(parent):
                    continue
                lane = state["lanes"].get(parent) or {}
                cursor = lane["cursors"][-1] if lane.get("relay_doc_id") and not lane.get("exhausted") else None
                while True:
                    record(client.page(media_id=media_id, cursor=cursor, parent_id=parent))
                    lane = state["lanes"][parent]
                    if lane["exhausted"]:
                        break
                    cursor = lane["cursors"][-1]
        finally:
            client.close()
        return CommentFetchSummary(
            expected_count=expected, full_observation=state["lanes"]["__top__"].get("started_from_root") is True
        )
