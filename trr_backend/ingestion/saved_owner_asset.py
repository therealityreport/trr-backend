"""Per-post recovery of an explicitly saved owner asset; never a handle alias.

Historical storage and the current native slot are independently sealed. Bytes
are copied unchanged into the current handle's namespace; only ordinary media
receipt writes may promote registry/canonical fields.
"""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import replace
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID

import requests
from psycopg2.extras import RealDictCursor

from trr_backend.ingestion import cached_avatar_alias as cached
from trr_backend.ingestion.domain import fingerprint, stable_json
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import MEDIA_VERSION, MediaIncomplete, snapshot_evidence, source_reference
from trr_backend.socials.media_url_safety import MediaUrlSafetyPolicy, safe_requests_get

INPUT_VERSION = "instagram-saved-owner-asset-input-v1"
AUTH_VERSION = "instagram-saved-owner-asset-authorization-v1"
MAX_ROWS = cached.MAX_ROWS
SHA = re.compile(r"[a-f0-9]{64}")
HANDLE = re.compile(r"[a-z0-9._]{1,30}")
INPUT_KEYS = frozenset(
    """version run_id account generation target_key owner_token owner_generation post_id canonical_post_id
 source_evidence_id source_evidence_sha256 inventory slot input_slot source_clock created_at binding historical_handle
 historical_cohort current_cohort selected_id sha256 source_url source_key
 destination_url destination_key byte_clock""".split()
)
AUTH_KEYS = frozenset(
    """version run_id account generation target_key owner_token owner_generation input_id input_sha256
 origin_input_id origin_input_sha256 parent_id parent_sha256 verified_at old_bytes destination_bytes""".split()
)
BINDING_KEYS = frozenset(
    (
        "post_id canonical_post_id owner_username source_reference post_source_clock hosted_owner_url "
        "canonical_owner_fields_sha256 branches"
    ).split()
)
BYTE_KEYS = frozenset(
    "hosted_url storage_key storage_binding_sha256 sha256 bytes format width height frames verified_at".split()
)


def require(value, code="saved_owner_asset_proof_invalid"):
    if not value:
        raise MediaIncomplete(code)


def stamp(value):
    return cached.stamp(value)


def identity(version, target, reference):
    return version + ":" + target + ":" + fingerprint(reference)


def _slot_valid(value):
    require(isinstance(value, dict) and set(value) == {"username", "role", "comment_id", "source_reference"})
    require(isinstance(value["username"], str) and HANDLE.fullmatch(value["username"]))
    require(value["role"] in {"owner", "tagged", "collaborator", "mention", "comment"})
    require(
        (value["role"] == "comment" and isinstance(value["comment_id"], str) and value["comment_id"])
        or (value["role"] != "comment" and value["comment_id"] is None)
    )
    require(cached.regional_reference(value["source_reference"]) == value["source_reference"])


def _cohort_valid(rows, reference):
    require(isinstance(rows, list) and len(rows) <= MAX_ROWS)
    require(len({r["id"] for r in rows}) == len(rows) and rows == sorted(rows, key=lambda r: r["id"]))
    for r in rows:
        require(
            isinstance(r, dict)
            and set(r) == {"id", "source_reference", "observed_at", "status", "sha256", "hosted_url"}
        )
        UUID(r["id"])
        require(
            cached.regional_reference(r["source_reference"]) == r["source_reference"]
            and urlsplit(r["source_reference"]).path == urlsplit(reference).path
        )
        require(r["observed_at"] is None or stamp(r["observed_at"]) == r["observed_at"])
        require(r["status"] in {"mirrored", "failed", "pending", "unsupported"})
        require(r["sha256"] is None or (isinstance(r["sha256"], str) and SHA.fullmatch(r["sha256"])))
        require(r["hosted_url"] is None or source_reference(r["hosted_url"]) == r["hosted_url"])


def candidate(rows, reference, source_clock):
    """Historical cache permits an existing exact reference, unlike an alias."""
    _cohort_valid(rows, reference)
    hashes = {r["sha256"] for r in rows if r["sha256"] is not None}
    require(len(hashes) <= 1, "saved_owner_asset_hash_ambiguous")
    urls = {r["hosted_url"] for r in rows if r["status"] == "mirrored" and r["sha256"]}
    require(len(urls) <= 1, "saved_owner_asset_url_ambiguous")
    newest = {}
    for r in sorted(rows, key=lambda r: (r["observed_at"] or "", r["id"]), reverse=True):
        newest.setdefault(r["source_reference"], r)
    choices = []
    for r in newest.values():
        proved = r["status"] == "mirrored" and r["sha256"] and r["hosted_url"] and r["observed_at"]
        if not proved and r["observed_at"] and r["observed_at"] > stamp(source_clock):
            raise MediaIncomplete("avatar_source_newer")
        if proved:
            choices.append(r)
    return max(choices, key=lambda r: (r["observed_at"], r["id"])) if choices else None


def _common(p):
    for k in ("run_id", "owner_token"):
        UUID(p[k])
    require(isinstance(p["account"], str) and HANDLE.fullmatch(p["account"]))
    require(isinstance(p["target_key"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", p["target_key"]))
    require(all(type(p[k]) is int and p[k] > 0 for k in ("generation", "owner_generation")))


def validate_payload(p, *, parser_version):
    require(isinstance(p, dict))
    if parser_version == AUTH_VERSION:
        require(set(p) == AUTH_KEYS and p["version"] == AUTH_VERSION)
        _common(p)
        for k in ("input_id", "origin_input_id"):
            UUID(p[k])
        for k in ("input_sha256", "origin_input_sha256"):
            require(isinstance(p[k], str) and SHA.fullmatch(p[k]))
        if p["parent_id"] is None:
            require(
                p["parent_sha256"] is None
                and p["input_id"] == p["origin_input_id"]
                and p["input_sha256"] == p["origin_input_sha256"]
            )
        else:
            UUID(p["parent_id"])
            require(isinstance(p["parent_sha256"], str) and SHA.fullmatch(p["parent_sha256"]))
        require(stamp(p["verified_at"]) == p["verified_at"])
        for k in ("old_bytes", "destination_bytes"):
            b = p[k]
            require(isinstance(b, dict) and set(b) == BYTE_KEYS)
            require(all(isinstance(b[n], str) and SHA.fullmatch(b[n]) for n in ("sha256", "storage_binding_sha256")))
            require(type(b["bytes"]) is int and 0 < b["bytes"] <= cached.MAX_BYTES)
            require(b["format"] in {"JPEG", "PNG", "WEBP", "GIF"})
            require(all(type(b[n]) is int and b[n] > 0 for n in ("width", "height", "frames")))
            require(stamp(b["verified_at"]) == b["verified_at"] <= p["verified_at"])
            require(source_reference(b["hosted_url"]) == b["hosted_url"])
            require(isinstance(b["storage_key"], str) and re.fullmatch(r"[A-Za-z0-9_./-]+", b["storage_key"]))
        require(
            all(
                p["old_bytes"][k] == p["destination_bytes"][k]
                for k in ("sha256", "bytes", "format", "width", "height", "frames")
            )
        )
    else:
        require(parser_version == INPUT_VERSION and set(p) == INPUT_KEYS and p["version"] == INPUT_VERSION)
        _common(p)
        for k in ("post_id", "canonical_post_id", "source_evidence_id", "selected_id"):
            UUID(p[k])
        _slot_valid(p["slot"])
        require(
            isinstance(p["historical_handle"], str)
            and HANDLE.fullmatch(p["historical_handle"])
            and p["historical_handle"] != p["slot"]["username"]
        )
        for k in ("source_evidence_sha256", "sha256"):
            require(isinstance(p[k], str) and SHA.fullmatch(p[k]))
        require(
            isinstance(p["inventory"], dict)
            and set(p["inventory"]) == {"version", "sha256"}
            and p["inventory"]["version"] == "instagram-media-inventory-v1"
            and SHA.fullmatch(p["inventory"]["sha256"])
        )
        require(all(stamp(p[k]) == p[k] for k in ("source_clock", "byte_clock", "created_at")))
        require(p["source_clock"] <= p["created_at"] and p["byte_clock"] <= p["created_at"])
        state = p["input_slot"]
        require(isinstance(state, dict) and set(state) == {"observed_at", "hosted_url", "sha256"})
        if state["hosted_url"] is None:
            require(state["sha256"] is None and state["observed_at"] == p["source_clock"])
        else:
            require(
                state == {"observed_at": p["byte_clock"], "hosted_url": p["destination_url"], "sha256": p["sha256"]}
            )
        require(
            source_reference(p["source_url"]) == p["source_url"]
            and source_reference(p["destination_url"]) == p["destination_url"]
        )
        require(
            p["source_key"] == cached.expected_key(p["historical_handle"], p["sha256"], p["source_url"])
            and p["destination_key"] == cached.expected_key(p["slot"]["username"], p["sha256"], p["source_url"])
        )
        require(
            urlsplit(p["source_url"]).path.endswith("/" + p["source_key"])
            and urlsplit(p["destination_url"]).path.endswith("/" + p["destination_key"])
        )
        b = p["binding"]
        require(isinstance(b, dict) and set(b) == BINDING_KEYS)
        require(
            b["post_id"] == p["post_id"]
            and b["canonical_post_id"] == p["canonical_post_id"]
            and b["source_reference"] == p["slot"]["source_reference"]
        )
        require(isinstance(b["owner_username"], str) and HANDLE.fullmatch(b["owner_username"]))
        require(
            stamp(b["post_source_clock"]) == b["post_source_clock"]
            and isinstance(b["canonical_owner_fields_sha256"], str)
            and SHA.fullmatch(b["canonical_owner_fields_sha256"])
        )
        require(b["hosted_owner_url"] is None or source_reference(b["hosted_owner_url"]) == b["hosted_owner_url"])
        require(isinstance(b["branches"], list) and len(b["branches"]) <= 2)
        require(len({r["location"] for r in b["branches"]}) == len(b["branches"]))
        for r in b["branches"]:
            require(
                set(r) == {"location", "username", "source_reference", "references", "sha256"}
                and r["location"] in {"post", "sidecar"}
                and (r["username"] is None or (isinstance(r["username"], str) and HANDLE.fullmatch(r["username"])))
                and (r["source_reference"] is None or source_reference(r["source_reference"]) == r["source_reference"])
                and isinstance(r["sha256"], str)
                and SHA.fullmatch(r["sha256"])
            )
            require(
                isinstance(r["references"], list)
                and len(r["references"]) <= 2
                and all(source_reference(v) == v for v in r["references"])
            )
        for k in ("historical_cohort", "current_cohort"):
            _cohort_valid(p[k], p["slot"]["source_reference"])
        selected = next((r for r in p["historical_cohort"] if r["id"] == p["selected_id"]), None)
        require(
            selected
            and selected["status"] == "mirrored"
            and selected["sha256"] == p["sha256"]
            and selected["hosted_url"] == p["source_url"]
        )
    require(len(stable_json(p).encode()) <= 4 * 1024 * 1024)
    return p


def binding(post, sidecar, snapshot, avatar):
    branches = []
    for location, row in (("post", post), ("sidecar", sidecar)):
        value = (row.get("raw_data") or {}).get("owner_detail")
        if not isinstance(value, dict):
            continue
        urls = [source_reference(value[k]) for k in ("profile_pic_url_hd", "profile_pic_url") if value.get(k)]
        branches.append(
            {
                "location": location,
                "username": value["username"].lower()
                if isinstance(value.get("username"), str) and value["username"]
                else None,
                "source_reference": urls[0] if urls else None,
                "references": urls,
                "sha256": fingerprint(value),
            }
        )
    return {
        "post_id": str(post["id"]),
        "canonical_post_id": snapshot.canonical_post_id,
        "owner_username": str(post.get("owner_username") or post.get("username") or "").lower(),
        "source_reference": cached.regional_reference(avatar.source_url),
        "post_source_clock": stamp(post["scraped_at"]),
        "hosted_owner_url": post.get("hosted_owner_profile_pic_url") or None,
        "canonical_owner_fields_sha256": fingerprint(
            {k: post.get(k) for k in ("owner_user_id", "owner_username", "source_account", "username")}
        ),
        "branches": branches,
    }


def read_cohort(cur, handle, reference, *, lock=False):
    """Complete matching-path input: unsupported rows must not disappear."""

    class CompleteRows:
        def execute(self, sql, args):
            cur.execute(sql, args)

        def fetchall(self):
            rows = list(cur.fetchall())
            require(
                all(cached.regional_reference(r.get("source_url")) is not None for r in rows),
                "saved_owner_asset_cohort_reference_unsupported",
            )
            return rows

    return cached.read_cohort(CompleteRows(), handle, reference, lock=lock)


def _root_binding(b, slot):
    branches = b["branches"]
    require(
        slot["role"] == "owner"
        and slot["comment_id"] is None
        and b["owner_username"] == slot["username"]
        and b["hosted_owner_url"]
        and branches,
        "saved_owner_asset_root_binding_missing",
    )
    handles = {r["username"] for r in branches}
    require(
        len(handles) == 1 and None not in handles and slot["username"] not in handles,
        "saved_owner_asset_historical_handle_ambiguous",
    )
    require(
        all(
            cached.regional_reference(r["source_reference"]) == r["source_reference"]
            and r["references"]
            and all(
                cached.regional_reference(ref) == ref and urlsplit(ref).path == urlsplit(slot["source_reference"]).path
                for ref in r["references"]
            )
            for r in branches
        ),
        "saved_owner_asset_historical_path_changed",
    )
    return next(iter(handles))


def _read_binding(cur, snapshot, avatar, *, lock=False, candidate_only=False):
    cur.execute(
        "select * from social.instagram_posts where id=%s" + (" for update" if lock else ""), (snapshot.post_id,)
    )
    post = cur.fetchone()
    require(post is not None)
    cur.execute(
        "select * from social.instagram_post_payloads where post_id=%s" + (" for update" if lock else ""),
        (snapshot.post_id,),
    )
    sidecar = cur.fetchone() or {}
    if candidate_only:
        details = [(r.get("raw_data") or {}).get("owner_detail") for r in (post, sidecar)]
        details = [r for r in details if isinstance(r, dict)]
        if any(not isinstance(r.get("username"), str) or not HANDLE.fullmatch(r["username"].lower()) for r in details):
            return None  # A numeric ID/boolean is not an explicit historical handle.
        if (
            not post.get("hosted_owner_profile_pic_url")
            or not details
            or {r["username"].lower() for r in details} == {avatar.username}
        ):
            return None
    return binding(post, sidecar, snapshot, avatar)


def _scope(row, version, target, run_id, *, at=None, pinned=True):
    require(
        str(row["run_id"]) == str(run_id)
        and row["target_key"] == target
        and row["stage"] == "media"
        and row["parser_version"] == version
        and row.get("owner_lineage") is True
    )
    payload = cached.evidence_from_row(row).read(now=at, pinned=pinned)
    require(
        payload["run_id"] == str(run_id)
        and payload["target_key"] == target
        and payload["account"] == row["run_account"]
        and stamp(row["captured_at"]) == payload["created_at" if version == INPUT_VERSION else "verified_at"]
    )
    return payload


def evidence_rows(cur, run_id, target, references):
    cur.execute(
        """select e.*,r.account as run_account,exists(select 1 from social.instagram_ingestion_dispatches d
        where d.run_id=e.run_id and d.stage=e.stage and d.target_key=e.target_key
        and d.owner_token=(e.payload::jsonb->>'owner_token')::uuid
        and d.generation=(e.payload::jsonb->>'generation')::bigint
        and exists(select 1 from social.instagram_ingestion_targets t where t.run_id=e.run_id and t.stage=e.stage
          and t.target_key=e.target_key and t.owner_generation >= (e.payload::jsonb->>'owner_generation')::bigint))
        as owner_lineage from social.instagram_ingestion_evidence e join social.instagram_ingestion_runs r using(run_id)
        where e.run_id=%s and e.stage='media'
        and e.target_key=%s and e.parser_version=%s and e.source_identity=any(%s)
        order by e.evidence_id limit %s""",
        (run_id, target, AUTH_VERSION, [identity(AUTH_VERSION, target, ref) for ref in references], MAX_ROWS + 1),
    )
    rows = list(cur.fetchall())
    require(len(rows) <= MAX_ROWS, "saved_owner_asset_evidence_limit")
    return rows


def _input_row(cur, evidence_id):
    cur.execute(
        """select e.*,r.account as run_account,exists(select 1 from social.instagram_ingestion_dispatches d
        where d.run_id=e.run_id
        and d.stage=e.stage and d.target_key=e.target_key and d.owner_token=(e.payload::jsonb->>'owner_token')::uuid
        and d.generation=(e.payload::jsonb->>'generation')::bigint) as owner_lineage
        from social.instagram_ingestion_evidence e join social.instagram_ingestion_runs r using(run_id)
        where evidence_id=%s""",
        (evidence_id,),
    )
    row = cur.fetchone()
    require(row is not None)
    return row


def _source_row(cur, evidence_id):
    cur.execute("select * from social.instagram_ingestion_evidence where evidence_id=%s", (evidence_id,))
    row = cur.fetchone()
    require(row is not None)
    return row


def validate_source(p, row, *, at=None, pinned=True):
    from trr_backend.ingestion.replay import _media

    require(
        str(row["evidence_id"]) == p["source_evidence_id"]
        and row["content_hash"] == p["source_evidence_sha256"]
        and str(row["run_id"]) == p["run_id"]
        and row["stage"] == "media"
        and row["target_key"] == p["target_key"]
        and row["parser_version"] == MEDIA_VERSION
    )
    evidence = cached.evidence_from_row(row)
    evidence.read(now=at, pinned=pinned)
    require(stamp(evidence.captured_at) <= p["created_at"], "saved_owner_asset_source_clock_invalid")
    parsed = _media(evidence, target_key=p["target_key"], parser_version=MEDIA_VERSION, pinned=True)
    require(parsed.source_unavailable is None)
    source = parsed.inventory
    require(
        source["post_id"] == p["post_id"]
        and source["canonical_post_id"] == p["canonical_post_id"]
        and source["inventory_fingerprint"] == p["inventory"]
    )
    matches = [
        a
        for a in source["avatars"]
        if {
            "username": a["username"],
            "role": a["role"],
            "comment_id": a.get("comment_id"),
            "source_reference": cached.regional_reference(a["profile_pic_url"]),
        }
        == p["slot"]
    ]
    require(len(matches) == 1)
    a = matches[0]
    require(
        {"observed_at": stamp(a["observed_at"]), "hosted_url": a.get("hosted_url"), "sha256": a.get("content_hash")}
        == p["input_slot"]
    )
    if p["slot"]["role"] == "comment":
        comments = [c for c in source["comments"] if c["comment_id"] == p["slot"]["comment_id"]]
        require(len(comments) == 1 and stamp(comments[0]["observed_at"]) == p["source_clock"])
    else:
        require(stamp(evidence.captured_at) == p["source_clock"])


def read_records(cur, run_id, target, references, *, at=None, pinned=True):
    at = at or datetime.now(UTC)
    records = {}
    for row in evidence_rows(cur, run_id, target, references):
        auth = _scope(row, AUTH_VERSION, target, run_id, at=at, pinned=pinned)
        inputs = []
        for name, sha in (("input_id", "input_sha256"), ("origin_input_id", "origin_input_sha256")):
            supplied = _input_row(cur, auth[name])
            p = _scope(supplied, INPUT_VERSION, target, run_id, at=at, pinned=pinned)
            require(
                supplied["content_hash"] == auth[sha]
                and supplied["source_identity"] == identity(INPUT_VERSION, target, p["slot"]["source_reference"])
            )
            validate_source(p, _source_row(cur, p["source_evidence_id"]), at=at, pinned=pinned)
            if at is not None:
                require(p["created_at"] <= stamp(at), "saved_owner_asset_future_input")
            inputs.append(p)
        current, origin = inputs
        require(row["source_identity"] == identity(AUTH_VERSION, target, current["slot"]["source_reference"]))
        require(
            all(
                auth[k] == current[k]
                for k in ("run_id", "account", "generation", "target_key", "owner_token", "owner_generation")
            )
        )
        require(
            all(
                current[k] == origin[k]
                for k in (
                    "run_id",
                    "account",
                    "target_key",
                    "post_id",
                    "canonical_post_id",
                    "historical_handle",
                    "sha256",
                    "source_url",
                    "source_key",
                    "destination_url",
                    "destination_key",
                    "byte_clock",
                )
            )
        )
        require(all(current["slot"][k] == origin["slot"][k] for k in ("username", "source_reference")))
        require(
            origin["slot"]["role"] == "owner"
            and origin["input_slot"]["hosted_url"] is None
            and _root_binding(origin["binding"], origin["slot"]) == origin["historical_handle"]
            and origin["binding"]["hosted_owner_url"] == origin["source_url"]
        )
        selected = candidate(current["historical_cohort"], current["slot"]["source_reference"], origin["source_clock"])
        require(
            selected is not None
            and selected["sha256"] == current["sha256"]
            and selected["hosted_url"] == current["source_url"]
        )
        original_candidate = next(r for r in origin["historical_cohort"] if r["id"] == origin["selected_id"])
        require(origin["byte_clock"] == original_candidate["observed_at"])
        for key, url, storage_key in (
            ("old_bytes", current["source_url"], current["source_key"]),
            ("destination_bytes", current["destination_url"], current["destination_key"]),
        ):
            require(
                auth[key]["hosted_url"] == url
                and auth[key]["storage_key"] == storage_key
                and auth[key]["sha256"] == current["sha256"]
            )
        require(auth["verified_at"] >= current["created_at"])
        if at is not None:
            require(auth["verified_at"] <= stamp(at), "saved_owner_asset_future_authorization")
        records[str(row["evidence_id"])] = {"row": row, "auth": auth, "input": current, "origin": origin}
    for item in records.values():
        auth = item["auth"]
        if auth["parent_id"] is None:
            require(item["input"] == item["origin"])
        else:
            require(auth["parent_id"] in records, "saved_owner_asset_parent_missing")
            parent = records[auth["parent_id"]]
            require(
                parent["auth"]["parent_id"] is None
                and parent["row"]["content_hash"] == auth["parent_sha256"]
                and auth["origin_input_id"] == parent["auth"]["origin_input_id"]
                and auth["origin_input_sha256"] == parent["auth"]["origin_input_sha256"]
                and parent["auth"]["verified_at"] <= auth["verified_at"],
                "saved_owner_asset_parent_invalid",
            )
    return records


def current_projection(p, actual):
    """Generated first row is explicit output, never an invented input UUID."""
    reference = p["slot"]["source_reference"]
    before = p["current_cohort"]
    exact_before = [r for r in before if r["source_reference"] == reference]
    exact_after = [r for r in actual if r["source_reference"] == reference]
    if len(exact_before) > 1 or len(exact_after) != 1:
        return False
    after = exact_after[0]
    if not (
        after["status"] == "mirrored"
        and after["sha256"] == p["sha256"]
        and after["hosted_url"] == p["destination_url"]
        and after["observed_at"] == p["byte_clock"]
    ):
        return False
    if exact_before and after != exact_before[0]:
        return False
    return [r for r in actual if r["source_reference"] != reference] == [
        r for r in before if r["source_reference"] != reference
    ]


def _related(avatar, origin):
    s = cached.slot(avatar)
    return all(s[k] == origin["slot"][k] for k in ("username", "source_reference"))


def applicable(item, snapshot, avatar, historical, current, *, postcommit):
    p = item["input"]
    return (
        cached.slot(avatar) == p["slot"]
        and snapshot.inventory_fingerprint == p["inventory"]
        and historical == p["historical_cohort"]
        and (current_projection(p, current) if postcommit else current == p["current_cohort"])
    )


def target_records(records, snapshot, account):
    for item in records.values():
        require(
            all(
                item[k]["post_id"] == snapshot.post_id
                and item[k]["canonical_post_id"] == snapshot.canonical_post_id
                and item[k]["account"] == account
                for k in ("input", "origin")
            )
        )


def _copy(storage, p, check, request):
    from trr_backend.socials.media_url_safety import UnsafeMediaUrlError

    try:
        return _copy_bytes(storage, p, check, request)
    except (requests.RequestException, UnsafeMediaUrlError, OSError):
        raise MediaIncomplete("saved_owner_asset_copy_failed") from None


def _copy_bytes(storage, p, check, request):
    old = cached.verify_cached_bytes(
        storage,
        username=p["historical_handle"],
        reference=p["slot"]["source_reference"],
        candidate={"hosted_url": p["source_url"], "sha256": p["sha256"]},
        check=check,
        request=request,
    )
    # The stable full-byte verifier intentionally does not export its body.
    # Read the same configured immutable object again for the conditional copy;
    # compare the complete body, not metadata/ETag, to the verified digest.
    request()
    remaining = min(120, check.remaining())
    policy = MediaUrlSafetyPolicy((urlsplit(p["source_url"]).hostname,), allow_test_hosts=False, resolve_dns=False)
    with safe_requests_get(
        requests,
        p["source_url"],
        policy=policy,
        allow_redirects=False,
        stream=True,
        headers={"Accept-Encoding": "identity"},
        timeout=(min(10, remaining), min(60, remaining)),
        total_timeout=remaining,
        body_timeout=min(90, remaining),
        max_bytes=cached.MAX_BYTES,
    ) as response:
        require(
            response.status_code == 200
            and not response.headers.get("Content-Range")
            and response.headers.get("Content-Encoding", "identity").lower() in {"", "identity"},
            "saved_owner_asset_copy_response",
        )
        length = response.headers.get("Content-Length")
        require(
            length is None or (str(length).isdigit() and int(length) == old["bytes"]), "saved_owner_asset_copy_length"
        )
        body, digest, size = io.BytesIO(), hashlib.sha256(), 0
        for chunk in response.iter_content(chunk_size=cached.CHUNK):
            check.body()
            size += len(chunk)
            require(size <= cached.MAX_BYTES, "saved_owner_asset_body_limit")
            digest.update(chunk)
            body.write(chunk)
        require(size == old["bytes"] and digest.hexdigest() == p["sha256"], "saved_owner_asset_copy_bytes")
        check()
        storage.put(
            p["destination_key"],
            body.getvalue(),
            "image/" + {"JPEG": "jpeg"}.get(old["format"], old["format"].lower()),
            check=check,
            request=request,
        )
        body.close()
    destination = cached.verify_cached_bytes(
        storage,
        username=p["slot"]["username"],
        reference=p["slot"]["source_reference"],
        candidate={"hosted_url": p["destination_url"], "sha256": p["sha256"]},
        check=check,
        request=request,
    )
    return old, destination


def _source_clock(snapshot, avatar):
    if avatar.role != "comment":
        return stamp(snapshot.observed_at)
    comments = [c for c in snapshot.comments if c.comment_id == avatar.comment_id]
    require(len(comments) == 1)
    return stamp(comments[0].observed_at or snapshot.observed_at)


def _owner_generation(cur, owned):
    cur.execute(
        """select owner_generation from social.instagram_ingestion_targets
        where run_id=%s and stage='media' and target_key=%s and owner_token=%s""",
        (owned["run_id"], owned["target_key"], owned["owner_token"]),
    )
    row = cur.fetchone()
    require(row is not None)
    return row["owner_generation"]


def _state(avatar, snapshot):
    return {
        "observed_at": stamp(avatar.observed_at or snapshot.observed_at),
        "hosted_url": avatar.cached_hosted_url,
        "sha256": avatar.cached_sha256,
    }


def _current_ok(rows, reference, source_clock, sha, destination, byte_clock):
    # Retain conservative all-alias hash and newer-unproved guards even though
    # only the ordinary exact-reference writer will create/update a cache row.
    found = candidate(rows, reference, source_clock)
    if found:
        require(found["sha256"] == sha and found["hosted_url"] == destination, "saved_owner_asset_current_conflict")
    exact = [r for r in rows if r["source_reference"] == reference]
    require(len(exact) <= 1, "saved_owner_asset_current_ambiguous")
    if exact:
        require(
            exact[0]["status"] == "mirrored"
            and exact[0]["sha256"] == sha
            and exact[0]["hosted_url"] == destination
            and exact[0]["observed_at"] == byte_clock,
            "saved_owner_asset_current_unproved",
        )


def recover(snapshot, *, loader, store, owned, storage_factory, callbacks):
    if snapshot.unavailable is not None:
        return snapshot
    avatars = [a for a in snapshot.avatars if cached.regional_reference(a.source_url)]
    if not avatars:
        return snapshot
    check, request = callbacks["check_cancelled"], callbacks["on_request"]
    bind = {k: owned[k] for k in ("run_id", "account", "generation", "target_key")}
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        store.check_owner(**owned, connection=conn)
        records = read_records(
            cur, owned["run_id"], snapshot.shortcode, [cached.slot(a)["source_reference"] for a in avatars]
        )
        target_records(records, snapshot, owned["account"])
    prepared = snapshot
    for initial in avatars:
        has_origin = any(_related(initial, item["origin"]) for item in records.values())
        if not has_origin and (initial.cached_hosted_url or initial.role != "owner"):
            continue  # Ordinary cached/non-owner slots do not reload inventory or read cohorts.
        native = loader.load(**bind, **callbacks)
        require(native.inventory_fingerprint == snapshot.inventory_fingerprint, "saved_owner_asset_inventory_drift")
        matches = [a for a in native.avatars if cached.slot(a) == cached.slot(initial)]
        require(len(matches) == 1, "saved_owner_asset_slot_drift")
        avatar = matches[0]
        roots = [r for r in records.values() if r["auth"]["parent_id"] is None and _related(avatar, r["origin"])]
        root = min(roots, key=lambda r: str(r["row"]["evidence_id"])) if roots else None
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            if root is None and (avatar.cached_hosted_url or avatar.role != "owner"):
                continue
            b = _read_binding(cur, native, avatar, candidate_only=root is None)
            if b is None:
                continue
            if root is None:
                if avatar.cached_hosted_url or avatar.role != "owner" or not b["branches"] or not b["hosted_owner_url"]:
                    continue
                if {r["username"] for r in b["branches"]} == {avatar.username}:
                    continue
                historical_handle = _root_binding(b, cached.slot(avatar))
                origin_clock = _source_clock(native, avatar)
            else:
                historical_handle, origin_clock = root["origin"]["historical_handle"], root["origin"]["source_clock"]
            historical = read_cohort(cur, historical_handle, cached.slot(avatar)["source_reference"])
            selected = candidate(historical, cached.slot(avatar)["source_reference"], origin_clock)
            require(selected is not None, "saved_owner_asset_historical_unproved")
            old_url, sha = selected["hosted_url"], selected["sha256"]
            byte_clock = selected["observed_at"]
            current = read_cohort(cur, avatar.username, cached.slot(avatar)["source_reference"])
            owner_generation = _owner_generation(cur, owned)
            if root is None:
                require(b["hosted_owner_url"] == old_url, "saved_owner_asset_canonical_source_mismatch")
            else:
                origin = root["origin"]
                require(old_url == origin["source_url"] and sha == origin["sha256"], "saved_owner_asset_origin_changed")
                byte_clock = origin["byte_clock"]
                # Current native roles are proved by their fresh ordinary seal;
                # no historical owner-role or raw-branch inference is reused.
                if any(
                    applicable(item, native, avatar, historical, current, postcommit=True)
                    and item["auth"]["owner_token"] == owned["owner_token"]
                    and b == item["input"]["binding"]
                    for item in records.values()
                    if _related(avatar, item["origin"])
                ):
                    continue
        storage = storage_factory()
        try:
            old_key = cached.expected_key(historical_handle, sha, old_url)
            destination_key = cached.expected_key(avatar.username, sha, old_url)
            destination_url = storage._url(destination_key)
            require(old_url == storage._url(old_key), "saved_owner_asset_storage_binding")
            _current_ok(
                current, cached.slot(avatar)["source_reference"], origin_clock, sha, destination_url, byte_clock
            )
            ordinary = snapshot_evidence(native)
            source_id = store.retain_evidence(**owned, evidence=ordinary)
            check()
            p = {
                "version": INPUT_VERSION,
                **{k: str(owned[k]) for k in ("run_id", "account", "target_key", "owner_token")},
                "generation": owned["generation"],
                "owner_generation": owner_generation,
                "post_id": native.post_id,
                "canonical_post_id": native.canonical_post_id,
                "source_evidence_id": source_id,
                "source_evidence_sha256": ordinary.content_hash,
                "inventory": native.inventory_fingerprint,
                "slot": cached.slot(avatar),
                "input_slot": _state(avatar, native),
                "source_clock": _source_clock(native, avatar),
                "created_at": stamp(datetime.now(UTC)),
                "binding": b,
                "historical_handle": historical_handle,
                "historical_cohort": historical,
                "current_cohort": current,
                "selected_id": selected["id"],
                "sha256": sha,
                "source_url": old_url,
                "source_key": old_key,
                "destination_url": destination_url,
                "destination_key": destination_key,
                "byte_clock": byte_clock,
            }
            input_evidence = Evidence.capture(
                source_identity=identity(INPUT_VERSION, native.shortcode, p["slot"]["source_reference"]),
                captured_at=datetime.fromisoformat(p["created_at"]),
                parser_version=INPUT_VERSION,
                payload=p,
            )
            input_id = store.retain_evidence(**owned, evidence=input_evidence)
            check()
            old_bytes, destination_bytes = _copy(storage, p, check, request)
        finally:
            close = getattr(storage.client, "close", None)
            if callable(close):
                close()
        check()
        require(
            loader.load(**bind, **callbacks).inventory_fingerprint == native.inventory_fingerprint,
            "saved_owner_asset_inventory_drift",
        )
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            require(_read_binding(cur, native, avatar) == b, "saved_owner_asset_canonical_binding_drift")
            require(
                read_cohort(cur, historical_handle, p["slot"]["source_reference"]) == historical
                and read_cohort(cur, avatar.username, p["slot"]["source_reference"]) == current,
                "saved_owner_asset_cohort_drift",
            )
        auth = {
            "version": AUTH_VERSION,
            **{k: p[k] for k in ("run_id", "account", "generation", "target_key", "owner_token", "owner_generation")},
            "input_id": input_id,
            "input_sha256": input_evidence.content_hash,
            "origin_input_id": root["auth"]["origin_input_id"] if root else input_id,
            "origin_input_sha256": root["auth"]["origin_input_sha256"] if root else input_evidence.content_hash,
            "parent_id": str(root["row"]["evidence_id"]) if root else None,
            "parent_sha256": root["row"]["content_hash"] if root else None,
            "verified_at": destination_bytes["verified_at"],
            "old_bytes": old_bytes,
            "destination_bytes": destination_bytes,
        }
        evidence = Evidence.capture(
            source_identity=identity(AUTH_VERSION, native.shortcode, p["slot"]["source_reference"]),
            captured_at=datetime.fromisoformat(auth["verified_at"]),
            parser_version=AUTH_VERSION,
            payload=auth,
        )
        auth_id = store.retain_evidence(**owned, evidence=evidence)
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            records = read_records(
                cur, owned["run_id"], native.shortcode, [cached.slot(a)["source_reference"] for a in avatars]
            )
            require(auth_id in records)
        prepared = replace(
            prepared,
            avatars=tuple(
                replace(
                    a,
                    cached_hosted_url=destination_url,
                    cached_sha256=sha,
                    observed_at=datetime.fromisoformat(byte_clock),
                )
                if cached.slot(a) == cached.slot(avatar)
                else a
                for a in prepared.avatars
            ),
        )
    prepared.validate()
    return prepared


def commit_guard(conn, *, run_id, account, target_key, result):
    """Runs inside the ordinary owner/receipt transaction before any mutation."""
    snapshot = result.snapshot
    avatars = [a for a in snapshot.avatars if cached.regional_reference(a.source_url)]
    if not avatars:
        return
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        records = read_records(cur, run_id, target_key, [cached.slot(a)["source_reference"] for a in avatars])
        if not records:
            return
        target_records(records, snapshot, account)
        cur.execute(
            """select owner_token,owner_generation from social.instagram_ingestion_targets
            where run_id=%s and stage='media' and target_key=%s for update""",
            (run_id, target_key),
        )
        owner = cur.fetchone()
        require(owner is not None)
        cur.execute(
            "select id from social.instagram_comments where post_id=%s order by id for update", (snapshot.post_id,)
        )
        from trr_backend.ingestion.media import read_inventory_fingerprint

        require(
            read_inventory_fingerprint(conn, snapshot.post_id) == snapshot.inventory_fingerprint,
            "saved_owner_asset_inventory_drift",
        )
        # Use one global alphabetical order including all ordinary avatar writes,
        # so the historical namespace cannot invert the existing lock order.
        handles = {a["username"] for a in result.avatars}
        handles.update(r["origin"]["historical_handle"] for r in records.values())
        for handle in sorted(handles):
            cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:avatar:" + handle,))
        for avatar in avatars:
            related = [r for r in records.values() if _related(avatar, r["origin"])]
            if not related:
                continue
            authorized = False
            for item in related:
                p, auth = item["input"], item["auth"]
                require(
                    p["account"] == account
                    and p["post_id"] == snapshot.post_id
                    and p["canonical_post_id"] == snapshot.canonical_post_id
                )
                historical = read_cohort(cur, p["historical_handle"], p["slot"]["source_reference"], lock=True)
                current = read_cohort(cur, p["slot"]["username"], p["slot"]["source_reference"], lock=True)
                if not (
                    applicable(item, snapshot, avatar, historical, current, postcommit=False)
                    and auth["owner_token"] == str(owner["owner_token"])
                    and auth["owner_generation"] == owner["owner_generation"]
                ):
                    continue
                require(
                    _read_binding(cur, snapshot, avatar, lock=True) == p["binding"],
                    "saved_owner_asset_canonical_binding_drift",
                )
                require(
                    avatar.cached_hosted_url == p["destination_url"]
                    and avatar.cached_sha256 == p["sha256"]
                    and stamp(avatar.observed_at) == p["byte_clock"],
                    "saved_owner_asset_result_drift",
                )
                _current_ok(
                    current,
                    p["slot"]["source_reference"],
                    item["origin"]["source_clock"],
                    p["sha256"],
                    p["destination_url"],
                    p["byte_clock"],
                )
                authorized = True
            require(authorized, "saved_owner_asset_current_authorization_missing")
