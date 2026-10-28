"""Owner-fenced bridges between retained copies of one historical avatar.

This proves cached bytes and a sealed handle/slot, never a native account ID or
fresh profile version. Read-only loaders do not call the promotion writer.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import UUID

import requests
from PIL import Image
from psycopg2.extras import RealDictCursor

from trr_backend.ingestion.domain import fingerprint, utc_timestamp
from trr_backend.ingestion.evidence import Evidence
from trr_backend.ingestion.media import (
    MEDIA_VERSION,
    MediaIncomplete,
    read_inventory_fingerprint,
    snapshot_evidence,
    source_reference,
    write_avatar_version,
)
from trr_backend.media.s3_mirror import build_instagram_profile_pic_s3_key
from trr_backend.socials.media_url_safety import MediaUrlSafetyPolicy, UnsafeMediaUrlError, safe_requests_get

VERSION = "instagram-cached-avatar-regional-alias-v1"
MAX_ROWS = 2048
MAX_BYTES = 50 * 1024 * 1024
CHUNK = 64 * 1024
_HOST = re.compile(r"scontent-[a-z]{3}[0-9]+-[0-9]+\.cdninstagram\.com")
_PATH = re.compile(r"/v/t[0-9]+\.[0-9]+-[0-9]+/[A-Za-z0-9_]+_n\.(jpg|jpeg|png|webp|gif)")
_SHA = re.compile(r"[a-f0-9]{64}")


def require(value, code="cached_avatar_alias_proof_invalid"):
    if not value:
        raise MediaIncomplete(code)


def stamp(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    require(parsed.utcoffset() is not None)
    return utc_timestamp(parsed)


def regional_reference(value):
    """Exact encoded path only: do not decode, normalize, or compare basenames."""
    if not isinstance(value, str):
        return None
    try:
        p = urlsplit(value)
        if p.scheme != "https" or not _HOST.fullmatch(p.netloc) or p.fragment or not _PATH.fullmatch(p.path):
            return None
        return "https://" + p.netloc + p.path
    except ValueError:
        return None


def slot(avatar):
    return {
        "username": avatar.username,
        "role": avatar.role,
        "comment_id": avatar.comment_id,
        "source_reference": regional_reference(avatar.source_url),
    }


def auxiliary_identity(target_key, reference):
    # The non-payload envelope remains addressable if retention/corruption
    # removes a payload. A missing payload must not turn a bridge into an
    # ordinary unconditionally trusted exact-reference cache on retry.
    return "instagram:cached-avatar-alias:" + target_key + ":" + fingerprint(reference)


def registry_cohort(rows, username, reference):
    path = urlsplit(reference).path
    found = []
    for row in rows:
        ref = regional_reference(row.get("source_url"))
        if row.get("account_handle") != username or ref is None or urlsplit(ref).path != path:
            continue
        require(row.get("platform") == "instagram")
        found.append(
            {
                "id": str(row["id"]),
                "source_reference": ref,
                "observed_at": stamp(row["last_checked_at"]) if row.get("last_checked_at") else None,
                "status": row.get("status"),
                "sha256": row.get("content_hash"),
                "hosted_url": row.get("hosted_url"),
            }
        )
    require(len(found) <= MAX_ROWS, "cached_avatar_alias_inventory_limit")
    return sorted(found, key=lambda row: row["id"])


def select_candidate(cohort, reference, source_clock):
    """All retained hashes agree; latest per exact reference determines eligibility."""
    hashes = {r["sha256"] for r in cohort if r["sha256"] is not None}
    require(
        len(hashes) <= 1 and all(isinstance(h, str) and _SHA.fullmatch(h) for h in hashes),
        "cached_avatar_alias_hash_ambiguous",
    )
    latest = {}
    for row in sorted(cohort, key=lambda r: (r["observed_at"] or "", r["id"]), reverse=True):
        latest.setdefault(row["source_reference"], row)
    candidates = []
    source_clock = stamp(source_clock)
    for row in latest.values():
        proved = (
            row["status"] == "mirrored"
            and row["observed_at"] is not None
            and row["sha256"] in hashes
            and bool(row["hosted_url"])
        )
        if not proved and row["observed_at"] and row["observed_at"] > source_clock:
            raise MediaIncomplete("avatar_source_newer")
        if proved:
            candidates.append(row)
    urls = {r["hosted_url"] for r in cohort if r["status"] == "mirrored" and r["sha256"]}
    require(len(urls) <= 1, "cached_avatar_alias_hosted_ambiguous")
    # Never replace an exact-reference record, including an older failed one.
    # This extension only fills a missing original reference.
    if reference in latest:
        return None
    return max(candidates, key=lambda r: (r["observed_at"], r["id"])) if candidates else None


def expected_key(username, sha256, hosted_url):
    extension = "." + urlsplit(hosted_url).path.rsplit(".", 1)[1]
    require(extension in {".jpg", ".jpeg", ".png", ".webp", ".gif"}, "cached_avatar_alias_storage_binding")
    # Maintained MediaStorage.host deliberately gives avatars their independent
    # content-addressed root, without the post-media storage prefix.
    return build_instagram_profile_pic_s3_key(username, sha256, extension)


def validate_payload(payload):
    """Dedicated closed evidence schema; no provider responses or signed URLs."""
    top = {
        "version",
        "run_id",
        "account",
        "generation",
        "target_key",
        "owner_token",
        "owner_generation",
        "post_id",
        "canonical_post_id",
        "slot",
        "source_clock",
        "byte_clock",
        "verified_at",
        "source_evidence_id",
        "source_evidence_sha256",
        "sealed_source_inventory",
        "current_inventory",
        "current_slot",
        "current_source_clock",
        "current_source_evidence_id",
        "current_source_evidence_sha256",
        "current_cohort_sha256",
        "parent_evidence_id",
        "parent_evidence_sha256",
        "cohort",
        "cohort_sha256",
        "selected_id",
        "hosted_url",
        "storage_key",
        "storage_binding_sha256",
        "sha256",
        "bytes",
        "format",
        "width",
        "height",
        "frames",
    }
    require(isinstance(payload, dict) and set(payload) == top and payload["version"] == VERSION)
    require(
        isinstance(payload["slot"], dict)
        and set(payload["slot"]) == {"username", "role", "comment_id", "source_reference"}
    )
    s = payload["slot"]
    require(isinstance(s["username"], str) and re.fullmatch(r"[a-z0-9._]{1,30}", s["username"]))
    require(s["role"] in {"owner", "comment", "tagged", "collaborator", "mention"})
    require(
        (s["role"] == "comment" and isinstance(s["comment_id"], str) and bool(s["comment_id"]))
        or (s["role"] != "comment" and s["comment_id"] is None)
    )
    require(regional_reference(s["source_reference"]) == s["source_reference"])
    current = payload["current_slot"]
    require(
        isinstance(current, dict)
        and set(current) == set(s)
        and current["username"] == s["username"]
        and current["source_reference"] == s["source_reference"]
        and current["role"] in {"owner", "comment", "tagged", "collaborator", "mention"}
    )
    require(
        (current["role"] == "comment" and isinstance(current["comment_id"], str) and current["comment_id"])
        or (current["role"] != "comment" and current["comment_id"] is None)
    )
    UUID(payload["current_source_evidence_id"])
    require(stamp(payload["current_source_clock"]) == payload["current_source_clock"])
    require(payload["current_source_clock"] <= payload["verified_at"])
    for name in ("current_source_evidence_sha256", "current_cohort_sha256"):
        require(isinstance(payload[name], str) and _SHA.fullmatch(payload[name]))
    if payload["parent_evidence_id"] is None:
        require(
            payload["parent_evidence_sha256"] is None
            and current == s
            and payload["current_source_clock"] == payload["source_clock"]
            and payload["current_cohort_sha256"] == payload["cohort_sha256"]
        )
    else:
        UUID(payload["parent_evidence_id"])
        require(
            isinstance(payload["parent_evidence_sha256"], str) and _SHA.fullmatch(payload["parent_evidence_sha256"])
        )
    for name in ("run_id", "owner_token", "post_id", "canonical_post_id", "source_evidence_id", "selected_id"):
        UUID(payload[name])
    require(
        type(payload["generation"]) is int
        and payload["generation"] > 0
        and type(payload["owner_generation"]) is int
        and payload["owner_generation"] > 0
    )
    require(isinstance(payload["account"], str) and re.fullmatch(r"[a-z0-9._]{1,30}", payload["account"]))
    require(isinstance(payload["target_key"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", payload["target_key"]))
    for name in ("source_evidence_sha256", "cohort_sha256", "sha256", "storage_binding_sha256"):
        require(isinstance(payload[name], str) and _SHA.fullmatch(payload[name]))
    for name in ("source_clock", "byte_clock", "verified_at"):
        require(stamp(payload[name]) == payload[name])
    require(payload["source_clock"] <= payload["verified_at"] and payload["byte_clock"] <= payload["verified_at"])
    for name in ("sealed_source_inventory", "current_inventory"):
        require(
            isinstance(payload[name], dict)
            and set(payload[name]) == {"version", "sha256"}
            and payload[name]["version"] == "instagram-media-inventory-v1"
            and _SHA.fullmatch(payload[name]["sha256"])
        )
    require(type(payload["bytes"]) is int and 0 < payload["bytes"] <= MAX_BYTES)
    require(payload["format"] in {"JPEG", "PNG", "WEBP", "GIF"})
    require(all(type(payload[n]) is int and payload[n] > 0 for n in ("width", "height", "frames")))
    require(isinstance(payload["cohort"], list) and 0 < len(payload["cohort"]) <= MAX_ROWS)
    rows = []
    for r in payload["cohort"]:
        require(
            isinstance(r, dict)
            and set(r) == {"id", "source_reference", "observed_at", "status", "sha256", "hosted_url"}
        )
        UUID(r["id"])
        require(
            regional_reference(r["source_reference"]) == r["source_reference"]
            and urlsplit(r["source_reference"]).path == urlsplit(s["source_reference"]).path
        )
        if r["observed_at"] is not None:
            require(stamp(r["observed_at"]) == r["observed_at"])
        require(r["sha256"] is None or (isinstance(r["sha256"], str) and _SHA.fullmatch(r["sha256"])))
        require(r["hosted_url"] is None or source_reference(r["hosted_url"]) == r["hosted_url"])
        require(r["status"] in {"mirrored", "failed", "pending", "unsupported"})
        rows.append(r)
    require(len({r["id"] for r in rows}) == len(rows) and rows == sorted(rows, key=lambda r: r["id"]))
    require(fingerprint(rows) == payload["cohort_sha256"])
    chosen = select_candidate(rows, s["source_reference"], payload["source_clock"])
    require(
        chosen is not None
        and chosen["id"] == payload["selected_id"]
        and chosen["sha256"] == payload["sha256"]
        and chosen["hosted_url"] == payload["hosted_url"]
        and chosen["observed_at"] == payload["byte_clock"]
    )
    require(source_reference(payload["hosted_url"]) == payload["hosted_url"])
    key = payload["storage_key"]
    suffix = expected_key(s["username"], payload["sha256"], payload["hosted_url"])
    require(
        isinstance(key, str) and key == suffix and re.fullmatch(r"[A-Za-z0-9_./-]+", key) and ".." not in key.split("/")
    )
    require(urlsplit(payload["hosted_url"]).path.endswith("/" + key))
    return payload


def evidence_from_row(row):
    return Evidence(
        source_identity=row["source_identity"],
        captured_at=row["captured_at"],
        parser_version=row["parser_version"],
        payload_json=row["payload"],
        content_hash=row["content_hash"],
        schema_version=row["schema_version"],
    )


def verify_sealed_source(proof, row, *, now=None, pinned=True):
    from trr_backend.ingestion.replay import _media

    require(row is not None)
    require(
        str(row["evidence_id"]) == proof["source_evidence_id"]
        and str(row["run_id"]) == proof["run_id"]
        and row["stage"] == "media"
        and row["target_key"] == proof["target_key"]
        and row["parser_version"] == MEDIA_VERSION
        and row["content_hash"] == proof["source_evidence_sha256"]
    )
    evidence = evidence_from_row(row)
    source = evidence.read(now=now, pinned=pinned)
    # Expiry was checked against the caller's immutable audit clock above.
    # Reuse the maintained ordinary inventory parser, not a synthetic slot DTO.
    parsed = _media(evidence, target_key=proof["target_key"], parser_version=MEDIA_VERSION, pinned=True)
    require(parsed.source_unavailable is None)
    require(evidence.source_identity == "instagram:media:" + proof["target_key"])
    require(
        source.get("post_id") == proof["post_id"]
        and source.get("canonical_post_id") == proof["canonical_post_id"]
        and source.get("inventory_fingerprint") == proof["sealed_source_inventory"]
    )
    matches = [
        a
        for a in source.get("avatars", [])
        if {
            "username": a.get("username"),
            "role": a.get("role"),
            "comment_id": a.get("comment_id"),
            "source_reference": regional_reference(a.get("profile_pic_url")),
        }
        == proof["slot"]
    ]
    require(len(matches) == 1 and stamp(matches[0]["observed_at"]) == proof["source_clock"])
    # The input is a native/canonical source slot, not an earlier derived cache.
    require(not matches[0].get("hosted_url") and not matches[0].get("content_hash"))
    return source


def verify_current_source(proof, row, *, now=None, pinned=True):
    from trr_backend.ingestion.replay import _media

    require(
        row is not None
        and str(row["evidence_id"]) == proof["current_source_evidence_id"]
        and str(row["run_id"]) == proof["run_id"]
        and row["stage"] == "media"
        and row["target_key"] == proof["target_key"]
        and row["parser_version"] == MEDIA_VERSION
        and row["content_hash"] == proof["current_source_evidence_sha256"]
    )
    evidence = evidence_from_row(row)
    evidence.read(now=now, pinned=pinned)
    parsed = _media(evidence, target_key=proof["target_key"], parser_version=MEDIA_VERSION, pinned=True)
    require(parsed.source_unavailable is None)
    source = parsed.inventory
    require(
        source["post_id"] == proof["post_id"]
        and source["canonical_post_id"] == proof["canonical_post_id"]
        and source["inventory_fingerprint"] == proof["current_inventory"]
    )
    matches = [
        a
        for a in source["avatars"]
        if {
            "username": a.get("username"),
            "role": a.get("role"),
            "comment_id": a.get("comment_id"),
            "source_reference": regional_reference(a.get("profile_pic_url")),
        }
        == proof["current_slot"]
    ]
    require(len(matches) == 1)
    avatar = matches[0]
    if proof["parent_evidence_id"] is None:
        require(
            not avatar.get("hosted_url")
            and not avatar.get("content_hash")
            and stamp(avatar["observed_at"]) == proof["current_source_clock"]
        )
    else:
        require(
            avatar.get("hosted_url") == proof["hosted_url"]
            and avatar.get("content_hash") == proof["sha256"]
            and stamp(avatar["observed_at"]) == proof["byte_clock"]
        )
    if proof["current_slot"]["role"] == "comment":
        comments = [c for c in source["comments"] if c.get("comment_id") == proof["current_slot"]["comment_id"]]
        require(len(comments) == 1)
        source_clock = stamp(comments[0]["observed_at"])
    else:
        source_clock = stamp(evidence.captured_at)
    require(source_clock == proof["current_source_clock"])
    return source


_ORIGIN_FIELDS = (
    "run_id",
    "account",
    "target_key",
    "post_id",
    "canonical_post_id",
    "slot",
    "source_clock",
    "byte_clock",
    "source_evidence_id",
    "source_evidence_sha256",
    "sealed_source_inventory",
    "cohort",
    "cohort_sha256",
    "selected_id",
    "hosted_url",
    "storage_key",
    "sha256",
)


def verify_parent(proof, records):
    if proof["parent_evidence_id"] is None:
        return
    require(proof["parent_evidence_id"] in records, "cached_avatar_alias_parent_missing")
    parent_row, parent = records[proof["parent_evidence_id"]]
    require(
        parent["parent_evidence_id"] is None
        and parent_row["content_hash"] == proof["parent_evidence_sha256"]
        and parent["verified_at"] <= proof["verified_at"]
        and all(parent[k] == proof[k] for k in _ORIGIN_FIELDS),
        "cached_avatar_alias_parent_invalid",
    )


def related(avatar, proof):
    current = slot(avatar)
    return all(current[k] == proof["slot"][k] for k in ("username", "source_reference"))


def authorizes(avatar, snapshot, proof):
    return (
        slot(avatar) == proof["current_slot"]
        and snapshot.inventory_fingerprint == proof["current_inventory"]
        and avatar.cached_sha256 == proof["sha256"]
        and avatar.cached_hosted_url == proof["hosted_url"]
        and stamp(avatar.observed_at) == proof["byte_clock"]
    )


def verify_cached_bytes(storage, *, username, reference, candidate, check, request, clock=lambda: datetime.now(UTC)):
    try:
        return _verify_cached_bytes(
            storage,
            username=username,
            reference=reference,
            candidate=candidate,
            check=check,
            request=request,
            clock=clock,
        )
    except (requests.RequestException, UnsafeMediaUrlError, OSError, Image.DecompressionBombError):
        raise MediaIncomplete("cached_avatar_alias_byte_verification_failed") from None


def _verify_cached_bytes(storage, *, username, reference, candidate, check, request, clock):
    """Full R2 digest and full exact configured CDN body; no native requests."""
    key = expected_key(username, candidate["sha256"], candidate["hosted_url"])
    url = storage._url(key)
    require(candidate["hosted_url"] == url, "cached_avatar_alias_storage_binding")
    p = urlsplit(url)
    require(
        p.scheme == "https"
        and p.hostname
        and not p.username
        and not p.password
        and not p.query
        and not p.fragment
        and not p.port
        and "\\" not in p.path,
        "cached_avatar_alias_storage_binding",
    )
    check()
    storage._storage_budget(check)
    request()
    digest = storage._storage_call("digest_object", check=check, Bucket=storage.bucket, Key=key)
    require(
        isinstance(digest, dict)
        and digest.get("sha256") == candidate["sha256"]
        and type(digest.get("bytes")) is int
        and 0 < digest["bytes"] <= MAX_BYTES,
        "cached_avatar_alias_storage_bytes",
    )
    check()
    request()
    remaining = check.remaining()
    total = min(120, remaining)
    require(total > 0, "target_deadline")
    policy = MediaUrlSafetyPolicy((p.hostname,), allow_test_hosts=False, resolve_dns=False)
    with safe_requests_get(
        requests,
        url,
        policy=policy,
        allow_redirects=False,
        stream=True,
        headers={"Accept-Encoding": "identity"},
        timeout=(min(10, total), min(60, total)),
        total_timeout=total,
        body_timeout=min(90, total),
        max_bytes=MAX_BYTES,
    ) as response:
        require(
            response.status_code == 200
            and not response.headers.get("Content-Range")
            and response.headers.get("Content-Encoding", "identity").lower() in {"", "identity"},
            "cached_avatar_alias_cdn_response",
        )
        length = response.headers.get("Content-Length")
        if length is not None:
            require(str(length).isdigit() and int(length) == digest["bytes"], "cached_avatar_alias_cdn_bytes")
        size, sha = 0, hashlib.sha256()
        with tempfile.TemporaryFile() as body:
            for chunk in response.iter_content(chunk_size=CHUNK):
                check.body()
                size += len(chunk)
                require(size <= MAX_BYTES, "cached_avatar_alias_body_limit")
                sha.update(chunk)
                body.write(chunk)
            check()
            require(size == digest["bytes"] and sha.hexdigest() == digest["sha256"], "cached_avatar_alias_cdn_bytes")
            body.seek(0)
            with Image.open(body) as image:
                fmt, width, height, frames = image.format, image.width, image.height, getattr(image, "n_frames", 1)
                require(fmt in {"JPEG", "PNG", "WEBP", "GIF"}, "cached_avatar_alias_image_invalid")
                for frame in range(frames):
                    check.body()
                    image.seek(frame)
                    image.load()
            check()
    return {
        "hosted_url": url,
        "storage_key": key,
        "storage_binding_sha256": fingerprint({"bucket": storage.bucket, "key": key, "public_url": url}),
        "sha256": sha.hexdigest(),
        "bytes": size,
        "format": fmt,
        "width": width,
        "height": height,
        "frames": frames,
        "verified_at": stamp(clock()),
    }


def read_cohort(cur, username, reference, *, lock=False):
    cur.execute(
        """select * from social.avatar_registry where platform='instagram' and account_handle=%s
        and substring(source_url from '^https://[^/]+(/[^?#]*)')=%s order by id limit %s"""
        + (" for update" if lock else ""),
        (username, urlsplit(reference).path, MAX_ROWS + 1),
    )
    rows = list(cur.fetchall())
    require(len(rows) <= MAX_ROWS, "cached_avatar_alias_inventory_limit")
    return registry_cohort(rows, username, reference)


def read_auxiliary(cur, run_id, target_key, references):
    cur.execute(
        """select e.*,exists(select 1 from social.instagram_ingestion_dispatches d
        where d.run_id=e.run_id and d.stage=e.stage and d.target_key=e.target_key
        and d.owner_token=(e.payload::jsonb->>'owner_token')::uuid
        and d.generation=(e.payload::jsonb->>'generation')::bigint
        and exists(select 1 from social.instagram_ingestion_targets t where t.run_id=e.run_id
          and t.stage=e.stage and t.target_key=e.target_key
          and t.owner_generation >= (e.payload::jsonb->>'owner_generation')::bigint)) as owner_lineage
        from social.instagram_ingestion_evidence e where e.run_id=%s and e.stage='media' and e.target_key=%s
        and e.parser_version=%s and e.source_identity=any(%s) order by e.evidence_id limit %s""",
        (run_id, target_key, VERSION, [auxiliary_identity(target_key, ref) for ref in references], MAX_ROWS + 1),
    )
    found = list(cur.fetchall())
    require(len(found) <= MAX_ROWS, "cached_avatar_alias_inventory_limit")
    return found


def validate_retained(row, *, run_id, target_key, now=None, pinned=True):
    require(
        str(row["run_id"]) == str(run_id)
        and row["stage"] == "media"
        and row["target_key"] == target_key
        and row["parser_version"] == VERSION
        and row["owner_lineage"] is True
    )
    proof = evidence_from_row(row).read(now=now, pinned=pinned)
    require(
        proof["run_id"] == str(run_id)
        and proof["target_key"] == target_key
        and row["source_identity"] == auxiliary_identity(target_key, proof["slot"]["source_reference"])
        and stamp(row["captured_at"]) == proof["verified_at"]
    )
    return proof


def verify_promoted(proof, cohort):
    """Promotion adds one exact source row; retained input rows cannot disappear."""
    before = {r["id"]: r for r in proof["cohort"]}
    now = {r["id"]: r for r in cohort}
    require(all(now.get(k) == v for k, v in before.items()), "cached_avatar_alias_registry_drift")
    exact = [r for r in cohort if r["source_reference"] == proof["slot"]["source_reference"]]
    require(
        len(exact) == 1
        and exact[0]["status"] == "mirrored"
        and exact[0]["sha256"] == proof["sha256"]
        and exact[0]["hosted_url"] == proof["hosted_url"]
        and exact[0]["observed_at"] == proof["byte_clock"],
        "cached_avatar_alias_promoted_row_invalid",
    )
    extras = [r for r in cohort if r["id"] not in before and r not in exact]
    # New matching-path observations still pass all hash/newer-unproved guards.
    combined = proof["cohort"] + extras
    chosen = select_candidate(combined, proof["slot"]["source_reference"], proof["source_clock"])
    require(
        chosen is not None and chosen["sha256"] == proof["sha256"] and chosen["hosted_url"] == proof["hosted_url"],
        "cached_avatar_alias_registry_drift",
    )


def _read_source(cur, evidence_id):
    cur.execute("select * from social.instagram_ingestion_evidence where evidence_id=%s", (evidence_id,))
    found = cur.fetchone()
    require(found is not None)
    return found


def _historical_source(cur, snapshot, avatar, owned):
    cur.execute(
        """select * from social.instagram_ingestion_evidence where run_id=%s and stage='media'
        and target_key=%s and parser_version=%s and payload is not null
        and payload::jsonb->>'post_id'=%s and payload::jsonb->>'canonical_post_id'=%s
        and exists(select 1 from jsonb_array_elements(payload::jsonb->'avatars') a
          where a->>'username'=%s and a->>'role'=%s and a->>'comment_id' is not distinct from %s
          and split_part(a->>'profile_pic_url','?',1)=%s
          and a->>'observed_at'=%s and a->>'hosted_url' is null and a->>'content_hash' is null)
        order by captured_at desc,evidence_id desc limit 1""",
        (
            owned["run_id"],
            owned["target_key"],
            MEDIA_VERSION,
            snapshot.post_id,
            snapshot.canonical_post_id,
            avatar.username,
            avatar.role,
            avatar.comment_id,
            regional_reference(avatar.source_url),
            stamp(avatar.observed_at or snapshot.observed_at),
        ),
    )
    return cur.fetchone()


def _slot_clock(snapshot, avatar):
    if avatar.role != "comment":
        return stamp(snapshot.observed_at)
    comments = [c for c in snapshot.comments if c.comment_id == avatar.comment_id]
    require(len(comments) == 1)
    return stamp(comments[0].observed_at or snapshot.observed_at)


def _locked_snapshot(conn, cur, snapshot):
    cur.execute("select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:post:" + snapshot.shortcode,))
    cur.execute(
        "select id from social.instagram_posts where id=%s and shortcode=%s for update",
        (snapshot.post_id, snapshot.shortcode),
    )
    require(cur.fetchone() is not None, "cached_avatar_alias_source_drift")
    cur.execute(
        "select id from social.social_posts where id=%s and platform='instagram' and source_id=%s for update",
        (snapshot.canonical_post_id, snapshot.shortcode),
    )
    require(cur.fetchone() is not None, "cached_avatar_alias_source_drift")
    cur.execute("select post_id from social.instagram_post_payloads where post_id=%s for update", (snapshot.post_id,))
    cur.execute("select id from social.instagram_comments where post_id=%s order by id for update", (snapshot.post_id,))
    require(
        read_inventory_fingerprint(conn, snapshot.post_id) == snapshot.inventory_fingerprint,
        "cached_avatar_alias_source_drift",
    )


def bridge_cached_avatars(snapshot, *, loader, store, owned, storage_factory, callbacks):
    """Owner-only. Ordinary caches add one bounded lookup and no byte I/O."""
    if getattr(snapshot, "unavailable", None) is not None:
        return snapshot
    avatars = [a for a in snapshot.avatars if regional_reference(a.source_url)]
    if not avatars:
        return snapshot
    check, request = callbacks["check_cancelled"], callbacks["on_request"]
    binding = {k: owned[k] for k in ("run_id", "account", "generation", "target_key")}
    records = {}
    with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        store.check_owner(**owned, connection=conn)
        rows = read_auxiliary(cur, owned["run_id"], owned["target_key"], [slot(a)["source_reference"] for a in avatars])
        for row in rows:
            proof = validate_retained(row, run_id=owned["run_id"], target_key=owned["target_key"])
            require(
                proof["account"] == owned["account"]
                and proof["post_id"] == snapshot.post_id
                and proof["canonical_post_id"] == snapshot.canonical_post_id
            )
            verify_sealed_source(proof, _read_source(cur, proof["source_evidence_id"]))
            verify_current_source(proof, _read_source(cur, proof["current_source_evidence_id"]))
            records[str(row["evidence_id"])] = row, proof
        for _, proof in records.values():
            verify_parent(proof, records)

    # Refresh each slot after previous partial progress. One asset may occupy
    # several roles; a newly cached role still needs its own current-slot seal.
    for original in avatars:
        matches = [a for a in snapshot.avatars if slot(a) == slot(original)]
        require(len(matches) == 1, "cached_avatar_alias_source_drift")
        avatar = matches[0]
        reference = regional_reference(avatar.source_url)
        relevant = [(r, p) for r, p in records.values() if related(avatar, p)]
        parent_row = parent = None
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            if avatar.cached_hosted_url and not relevant:
                continue  # Ordinary native exact-reference cached version.
            if reference is None:
                raise MediaIncomplete("cached_avatar_alias_proof_invalid")
            cohort = read_cohort(cur, avatar.username, reference)
            if avatar.cached_hosted_url:
                roots = [(r, p) for r, p in relevant if p["parent_evidence_id"] is None]
                require(bool(roots), "cached_avatar_alias_parent_missing")
                for _, root in roots:
                    verify_promoted(root, cohort)
                if any(
                    authorizes(avatar, snapshot, p)
                    and (p["parent_evidence_id"] is None or p["current_cohort_sha256"] == fingerprint(cohort))
                    for _, p in relevant
                ):
                    continue
                parent_row, parent = min(roots, key=lambda item: str(item[0]["evidence_id"]))
                require(
                    avatar.cached_sha256 == parent["sha256"]
                    and avatar.cached_hosted_url == parent["hosted_url"]
                    and stamp(avatar.observed_at) == parent["byte_clock"],
                    "cached_avatar_alias_promoted_row_invalid",
                )
                candidate = {"sha256": parent["sha256"], "hosted_url": parent["hosted_url"]}
                source = _read_source(cur, parent["source_evidence_id"])
            else:
                candidate = select_candidate(cohort, reference, avatar.observed_at or snapshot.observed_at)
                if candidate is None:
                    continue
                source = _historical_source(cur, snapshot, avatar, owned)
            cur.execute(
                """select owner_generation from social.instagram_ingestion_targets
                where run_id=%s and stage='media' and target_key=%s and owner_token=%s""",
                (owned["run_id"], owned["target_key"], owned["owner_token"]),
            )
            owner_generation = cur.fetchone()["owner_generation"]

        # This is the same complete ordinary seal used by MediaActivity, not a
        # special source projection. It precedes all bridge I/O, including on a
        # legitimate first attempt with no previous retained media snapshot.
        snapshot.validate()
        ordinary = snapshot_evidence(snapshot)
        current_id = store.retain_evidence(**owned, evidence=ordinary)
        check()
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            current_source = _read_source(cur, current_id)
        if source is None:
            source = current_source
        storage = storage_factory()
        try:
            verified = verify_cached_bytes(
                storage,
                username=avatar.username,
                reference=reference,
                candidate=candidate,
                check=check,
                request=request,
            )
        finally:
            close = getattr(storage.client, "close", None)
            if callable(close):
                close()
        if parent is None:
            origin = {
                "slot": slot(avatar),
                "source_clock": stamp(avatar.observed_at or snapshot.observed_at),
                "byte_clock": candidate["observed_at"],
                "source_evidence_id": str(source["evidence_id"]),
                "source_evidence_sha256": source["content_hash"],
                "sealed_source_inventory": evidence_from_row(source).read(pinned=True)["inventory_fingerprint"],
                "cohort": cohort,
                "cohort_sha256": fingerprint(cohort),
                "selected_id": candidate["id"],
            }
        else:
            origin = {
                k: parent[k]
                for k in (
                    "slot",
                    "source_clock",
                    "byte_clock",
                    "source_evidence_id",
                    "source_evidence_sha256",
                    "sealed_source_inventory",
                    "cohort",
                    "cohort_sha256",
                    "selected_id",
                )
            }
        proof = {
            "version": VERSION,
            **{k: str(owned[k]) for k in ("run_id", "account", "target_key", "owner_token")},
            "generation": owned["generation"],
            "owner_generation": owner_generation,
            "post_id": snapshot.post_id,
            "canonical_post_id": snapshot.canonical_post_id,
            **origin,
            "current_inventory": snapshot.inventory_fingerprint,
            "current_slot": slot(avatar),
            "current_source_clock": _slot_clock(snapshot, avatar),
            "current_source_evidence_id": current_id,
            "current_source_evidence_sha256": ordinary.content_hash,
            "current_cohort_sha256": fingerprint(cohort),
            "parent_evidence_id": str(parent_row["evidence_id"]) if parent_row is not None else None,
            "parent_evidence_sha256": parent_row["content_hash"] if parent_row is not None else None,
            **verified,
        }
        validate_payload(proof)
        verify_sealed_source(proof, source)
        verify_current_source(proof, current_source)
        verify_parent(proof, records)
        evidence = Evidence.capture(
            source_identity=auxiliary_identity(snapshot.shortcode, reference),
            captured_at=datetime.fromisoformat(proof["verified_at"]),
            parser_version=VERSION,
            payload=proof,
        )
        proof_id = store.retain_evidence(**owned, evidence=evidence)
        check()
        require(loader.load(**binding, **callbacks) == snapshot, "cached_avatar_alias_source_drift")
        with store.transaction() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            store.check_owner(**owned, connection=conn)
            _locked_snapshot(conn, cur, snapshot)
            cur.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))", ("instagram:avatar:" + avatar.username,)
            )
            require(
                read_cohort(cur, avatar.username, reference, lock=True) == cohort, "cached_avatar_alias_registry_drift"
            )
            durable = {
                str(r["evidence_id"]): (r, validate_retained(r, run_id=owned["run_id"], target_key=snapshot.shortcode))
                for r in read_auxiliary(cur, owned["run_id"], snapshot.shortcode, [reference])
            }
            require(
                proof_id in durable
                and durable[proof_id][0]["content_hash"] == evidence.content_hash
                and durable[proof_id][1] == proof
            )
            # Re-read both seals and root lineage after network work, under the
            # current source/owner/cohort fence, before any cache promotion.
            verify_sealed_source(proof, _read_source(cur, proof["source_evidence_id"]))
            verify_current_source(proof, _read_source(cur, proof["current_source_evidence_id"]))
            for _, saved in durable.values():
                verify_parent(saved, durable)
            if parent is None:
                write_avatar_version(
                    conn,
                    username=avatar.username,
                    source_url=reference,
                    hosted_url=proof["hosted_url"],
                    content_hash=proof["sha256"],
                    observed_at=datetime.fromisoformat(proof["byte_clock"]),
                )
            else:
                verify_promoted(proof, cohort)  # unchanged exact row; no rewrite
        records[proof_id] = durable[proof_id]
        snapshot = loader.load(**binding, **callbacks)
    return snapshot
