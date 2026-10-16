"""Bounded run-specific commenter enumeration before the media manifest seals.

Every cursor advance and target insert shares the active run transaction. Only
settled comment receipts and retained committed pages can create profile work.
No canonical-row scan, source request, or append to a sealed manifest occurs.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION, CommentSourceError, parse_comment_evidence
from trr_backend.ingestion.domain import fingerprint, stable_json, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.persistence import _row
from trr_backend.ingestion.profiles import (
    PROFILE_KIND,
    PROFILE_VERSION,
    ProfileError,
    handle,
    native_id,
    profile_target,
)

INVENTORY_KEY = "commenter_profiles"
_UNAVAILABLE = frozenset({"deleted", "comments_disabled", "restricted", "media_unavailable"})


class ProfileInventoryError(ValueError):
    def __init__(self, code="profile_inventory_proof_missing"):
        self.code = code
        super().__init__(code)


def _object(value, *, code="profile_inventory_proof_invalid"):
    if not isinstance(value, dict):
        raise ProfileInventoryError(code)
    return value


def _validate_state(state):
    """Validate persisted cursor shapes before they become SQL parameters."""
    if type(state.get("version")) is not int or state["version"] != PROFILE_VERSION:
        raise ProfileInventoryError("profile_inventory_version_unsupported")
    if (
        set(state) - {"version", "state", "target_after", "page_id", "page_after", "offset", "error_code"}
        or not isinstance(state.get("state"), str)
        or state["state"] not in {"enumerating", "blocked"}
        or ("error_code" in state and not isinstance(state["error_code"], str))
        or not isinstance(state.get("target_after", ""), str)
        or type(state.get("offset", 0)) is not int
        or state.get("offset", 0) < 0
        or ("offset" in state and "page_id" not in state)
        or ("page_id" in state and "page_after" in state)
    ):
        raise ProfileInventoryError("profile_inventory_cursor_invalid")
    try:
        if "page_id" in state:
            if not isinstance(state["page_id"], str):
                raise ValueError
            UUID(state["page_id"])
        if "page_after" in state:
            after = state["page_after"]
            if not isinstance(after, list) or len(after) != 2 or any(not isinstance(v, str) for v in after):
                raise ValueError
            at = datetime.fromisoformat(after[0].replace("Z", "+00:00"))
            if at.utcoffset() is None:
                raise ValueError
            UUID(after[1])
    except ValueError:
        raise ProfileInventoryError("profile_inventory_cursor_invalid") from None


def _evidence(row):
    return Evidence(
        row["source_identity"],
        row["captured_at"],
        row["parser_version"],
        row["payload"],
        row["content_hash"],
        row["schema_version"],
    )


def _state(cur, run_id, manifest, state, *, seal=False):
    # A corrupt root JSON value cannot be merged. Replace it only with the
    # controlled diagnostic; valid unrelated cursor fields remain untouched.
    cursor = dict(manifest["cursor_state"]) if isinstance(manifest["cursor_state"], dict) else {}
    cursor[INVENTORY_KEY] = state
    cur.execute(
        """update social.instagram_ingestion_manifests set cursor_state=%s::jsonb,
        discovery_complete=%s,sealed_at=case when %s then clock_timestamp() else null end
        where run_id=%s and stage='media' and not discovery_complete""",
        (stable_json(cursor), seal, seal, run_id),
    )
    if cur.rowcount != 1:
        raise ProfileInventoryError("profile_inventory_already_sealed")
    return {"sealed": seal, "blocked": state.get("error_code")}


def _receipt(cur, run_id, target):
    receipt = _row(
        cur,
        """select r.*,e.source_identity,e.captured_at,e.parser_version,e.schema_version,
        e.payload,e.content_hash from social.instagram_ingestion_receipts r
        left join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
        where r.run_id=%s and r.stage='comments' and r.target_key=%s
            and r.outcome=%s and r.observed_at=%s order by r.committed_at desc limit 1""",
        (run_id, target["target_key"], target["outcome"], target["observed_at"]),
    )
    if (
        not receipt
        or not receipt.get("payload")
        or receipt["parser_version"] != COMMENT_PARSER_VERSION
        or not receipt["source_identity"].startswith(f"instagram:comments:{target['target_key']}:")
    ):
        raise ProfileInventoryError()
    try:
        page = parse_comment_evidence(_evidence(receipt), pinned=True)
    except (ValueError, TypeError):
        raise ProfileInventoryError() from None
    identity = _object(receipt["canonical_identity"])
    checkpoint = _object(target["checkpoint"])
    pages = _object(checkpoint.get("comment_pages", {}))
    if target["outcome"] == "source_unavailable":
        if page.unavailable_reason not in _UNAVAILABLE or pages.get("observed"):
            raise ProfileInventoryError()
        return receipt, None, None
    observed = _object(pages.get("observed"))
    observed_ids = identity.get("observed_comment_ids")
    if (
        not isinstance(observed_ids, list)
        or any(not isinstance(value, str) for value in observed_ids)
        or set(observed_ids) != set(observed)
    ):
        raise ProfileInventoryError()
    root_id = pages.get("scan_root_evidence_id")
    try:
        if not isinstance(root_id, str):
            raise ValueError
        UUID(root_id)
    except ValueError:
        raise ProfileInventoryError("profile_inventory_proof_invalid") from None
    root = _row(
        cur,
        """select p.* from social.instagram_ingestion_comment_pages p
        where p.run_id=%s and p.target_key=%s and p.evidence_id=%s""",
        (run_id, target["target_key"], root_id),
    )
    if not root:
        raise ProfileInventoryError()
    return receipt, observed, root


def _page(cur, run_id, target, receipt, root, state):
    params = [run_id, target["target_key"], root["committed_at"], str(root["page_id"]), receipt["committed_at"]]
    condition = ""
    if state.get("page_id"):
        condition = " and p.page_id=%s"
        params.append(state["page_id"])
    elif state.get("page_after"):
        condition = " and (p.committed_at,p.page_id)>(%s::timestamptz,%s::uuid)"
        params.extend(state["page_after"])
    return _row(
        cur,
        """select p.page_id,p.committed_at,p.canonical_identity,e.*
        from social.instagram_ingestion_comment_pages p
        join social.instagram_ingestion_evidence e using(evidence_id,run_id,stage,target_key)
        where p.run_id=%s and p.target_key=%s and (p.committed_at,p.page_id)>=(%s,%s::uuid)
        and p.committed_at<=%s"""
        + condition
        + " order by p.committed_at,p.page_id limit 1",
        tuple(params),
    )


def _insert(cur, run, comment, page):
    user_id = username = None
    try:
        if comment.author.user_id is not None:
            try:
                user_id = native_id(comment.author.user_id)
            except ProfileError:
                raise ProfileInventoryError("profile_inventory_author_id_invalid") from None
        if comment.author.username:
            try:
                username = handle(comment.author.username)
            except ProfileError:
                # An unusable public handle cannot authorize lookup. Keep a
                # native-ID target (cache-only if available), or an explicit
                # unresolved reference when neither identity is usable.
                username = None
        key = profile_target(user_id, username)
        reason = None
    except ProfileError:
        key = "profile:unresolved:" + fingerprint({"post": page["target_key"], "comment": comment.comment_id})
        reason = "profile_identity_unresolved"
    checkpoint = {
        "target_kind": PROFILE_KIND,
        "version": PROFILE_VERSION,
        "user_id": user_id,
        "username": username,
        "source_post_key": page["target_key"],
        "source_comment_id": comment.comment_id,
        "source_evidence_id": str(page["evidence_id"]),
        "discovered_at": utc_timestamp(page["captured_at"]),
    }
    cur.execute(
        """insert into social.instagram_ingestion_targets
        (run_id,stage,target_key,max_attempts,checkpoint,blocked_reason) values(%s,'media',%s,3,%s::jsonb,%s)
        on conflict(run_id,stage,target_key) do update set checkpoint=excluded.checkpoint
        where social.instagram_ingestion_targets.attempts=0
          and social.instagram_ingestion_targets.owner_token is null
          and social.instagram_ingestion_targets.checkpoint->>'target_kind'='commenter_profile'
          and (social.instagram_ingestion_targets.checkpoint->>'discovered_at')::timestamptz<
              (excluded.checkpoint->>'discovered_at')::timestamptz""",
        (run["run_id"], key, stable_json(checkpoint), reason),
    )


def advance(cur, run, *, limit=100):
    """Process at most one retained page and limit author observations per call."""
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Profile discovery batch must contain 1 to 100 authors")
    manifest = _row(
        cur,
        """select * from social.instagram_ingestion_manifests
        where run_id=%s and stage='media' for update""",
        (run["run_id"],),
    )
    if not manifest or manifest["discovery_complete"]:
        return {"sealed": bool(manifest and manifest["discovery_complete"])}
    state = {"version": PROFILE_VERSION, "state": "enumerating"}
    cur.execute("savepoint profile_inventory_batch")
    try:
        cursor = _object(manifest["cursor_state"], code="profile_inventory_cursor_invalid")
        if INVENTORY_KEY in cursor:
            state = dict(_object(cursor[INVENTORY_KEY], code="profile_inventory_cursor_invalid"))
        _validate_state(state)
        ready = _row(
            cur,
            """select discovery_complete and not exists(
            select 1 from social.instagram_ingestion_targets t where t.run_id=m.run_id
                and t.stage='comments' and outcome='unresolved') as ready
            from social.instagram_ingestion_manifests m where run_id=%s and stage='comments'""",
            (run["run_id"],),
        )
        if not ready or not ready["ready"]:
            return {"sealed": False}
        target = _row(
            cur,
            """select * from social.instagram_ingestion_targets where run_id=%s and stage='comments'
            and target_key>%s order by target_key limit 1""",
            (run["run_id"], state.get("target_after", "")),
        )
        if not target:
            return _state(cur, run["run_id"], manifest, dict(state, state="complete"), seal=True)
        receipt, observed, root = _receipt(cur, run["run_id"], target)
        if observed is None:  # Evidence-backed unavailability contributes no old canonical authors.
            return _state(
                cur,
                run["run_id"],
                manifest,
                {"version": PROFILE_VERSION, "state": "enumerating", "target_after": target["target_key"]},
            )
        page = _page(cur, run["run_id"], target, receipt, root, state)
        if not page:
            # Every observed comment must still have committed current-scan page
            # proof. No fallback to global canonical comments is permitted.
            cur.execute(
                """select count(distinct ids.key) filter(where ids.key=any(%s)) as count,
                coalesce(bool_or(jsonb_typeof(p.canonical_identity) is distinct from 'object'
                  or jsonb_typeof(p.canonical_identity->'comment_ids') is distinct from 'object'),false) as invalid
                from social.instagram_ingestion_comment_pages p
                left join lateral jsonb_object_keys(case
                    when jsonb_typeof(p.canonical_identity->'comment_ids')='object'
                    then p.canonical_identity->'comment_ids' else '{}'::jsonb end) ids(key) on true
                where p.run_id=%s and p.target_key=%s and (p.committed_at,p.page_id)>=(%s,%s::uuid)
                  and p.committed_at<=%s""",
                (
                    list(observed),
                    run["run_id"],
                    target["target_key"],
                    root["committed_at"],
                    str(root["page_id"]),
                    receipt["committed_at"],
                ),
            )
            proof = cur.fetchone()
            if proof["invalid"]:
                raise ProfileInventoryError("profile_inventory_proof_invalid")
            if proof["count"] != len(observed):
                raise ProfileInventoryError()
            return _state(
                cur,
                run["run_id"],
                manifest,
                {"version": PROFILE_VERSION, "state": "enumerating", "target_after": target["target_key"]},
            )
        identity = _object(page["canonical_identity"])
        comment_ids = _object(identity.get("comment_ids"))
        if (
            page["parser_version"] != COMMENT_PARSER_VERSION
            or not page["payload"]
            or not page["source_identity"].startswith(f"instagram:comments:{target['target_key']}:")
        ):
            raise ProfileInventoryError()
        parsed = parse_comment_evidence(_evidence(page), pinned=True)
        flat = []

        def visit(comment):
            flat.append(comment)
            for child in comment.replies:
                visit(child)

        for comment in parsed.comments:
            visit(comment)
        if set(comment_ids) != {comment.comment_id for comment in flat}:
            raise ProfileInventoryError()
        offset = state.get("offset", 0)
        if type(offset) is not int or offset < 0 or offset > len(flat):
            raise ProfileInventoryError("profile_inventory_cursor_invalid")
        for comment in flat[offset : offset + limit]:
            if comment.comment_id in observed:
                _insert(cur, run, comment, page)
        done = offset + limit >= len(flat)
        next_state = {"version": PROFILE_VERSION, "state": "enumerating", "target_after": state.get("target_after", "")}
        if done:
            next_state["page_after"] = [utc_timestamp(page["committed_at"]), str(page["page_id"])]
        else:
            next_state.update(page_id=str(page["page_id"]), offset=offset + limit)
        return _state(cur, run["run_id"], manifest, next_state)
    except (ProfileInventoryError, CommentSourceError, TypeError, ValueError) as error:
        cur.execute("rollback to savepoint profile_inventory_batch")
        cur.execute("release savepoint profile_inventory_batch")
        code = error.code if isinstance(error, ProfileInventoryError) else "profile_inventory_proof_invalid"
        return _state(cur, run["run_id"], manifest, dict(state, state="blocked", error_code=code))
