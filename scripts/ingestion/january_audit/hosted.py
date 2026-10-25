#!/usr/bin/env python3
"""Read complete configured storage/CDN objects and decode all selected bytes.

No Instagram requests, database connections, uploads, replay or controls. A
passing record-audit subset remains a subset. Missing original hashes stay
unknown; observed hashes and verification clocks are recorded separately.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import UUID

from . import common
from .posts import digest

CHUNK_BYTES = 64 * 1024
PUBLIC_STORAGE_USER_AGENT = "TRR-January-Media-Audit/1.0"
_SOURCE_PLACEHOLDER_SHA256 = {"05e81bfed1fda5743fea1da04586e635c4f82cd2258c2716265b538508efd5a9"}


def require(value, code):
    common.require(value, code)


def object_key(url, expected, base):
    """Bind exact URL origin/prefix to an approved bucket key, including aliases."""
    parsed = urlsplit(url)
    bases = [base] if isinstance(base, str) else base
    require(
        parsed.scheme == "https"
        and not parsed.query
        and not parsed.fragment
        and parsed.username is None
        and parsed.password is None
        and "\\" not in parsed.path,
        "object_url_outside_configured_public_storage",
    )
    for value in bases:
        root = urlsplit(value)
        prefix = root.path.rstrip("/") + "/"
        if parsed.netloc != root.netloc or not parsed.path.startswith(prefix):
            continue
        key = unquote(parsed.path[len(prefix) :])
        require(
            bool(key)
            and not key.startswith("/")
            and "\\" not in key
            and all(part not in {"", ".", ".."} for part in key.split("/")),
            "object_key_invalid",
        )
        require(expected is None or expected == key, "object_storage_key_mismatch")
        return key
    raise common.GuardError("object_url_outside_configured_public_storage")


def copy_body(reader, path, expected_size):
    total = 0
    hashed = hashlib.sha256()
    with path.open("xb") as handle:
        while chunk := reader(CHUNK_BYTES):
            require(isinstance(chunk, bytes), "body_chunk_not_bytes")
            total += len(chunk)
            require(expected_size is None or total <= expected_size, "body_exceeds_claimed_length")
            handle.write(chunk)
            hashed.update(chunk)
    require(total > 0, "empty_object")
    require(expected_size is None or total == expected_size, "full_body_length_mismatch")
    return total, hashed.hexdigest()


def mime(value):
    value = (value or "").split(";", 1)[0].strip().lower()
    return {"image/jpg": "image/jpeg"}.get(value, value)


def decode_file(path, metadata):
    from PIL import Image

    mode = metadata["required_decode_mode"]
    if mode == "video_full_stream":
        hint = (metadata.get("format") or Path(metadata["storage_key"]).suffix.lstrip(".")).lower()
        require(hint in {"mp4", "mov", "m4v", "webm"}, "supported_self_contained_video_format_required")
        demuxer = "matroska" if hint == "webm" else "mov"
        # Force a self-contained container; image/playlist demuxers and all
        # network protocols are excluded. MOV external data references stay off.
        controls = ["-protocol_whitelist", "file,pipe", "-format_whitelist", "mov,matroska,webm", "-f", demuxer]
        if demuxer == "mov":
            controls += ["-enable_drefs", "0", "-use_absolute_path", "0"]
        probe = subprocess.run(
            [
                shutil.which("ffprobe") or "ffprobe",
                "-v",
                "error",
                *controls,
                "-show_entries",
                "stream=index,codec_type,codec_name,width,height,duration,nb_frames:format=format_name,duration,size",
                "-of",
                "json",
                "-i",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        require(probe.returncode == 0, "video_probe_failed")
        info = json.loads(probe.stdout)
        streams = info.get("streams", [])
        require(any(s.get("codec_type") == "video" for s in streams), "video_stream_missing")
        formats = set(info.get("format", {}).get("format_name", "").split(","))
        require(
            bool(formats & ({"matroska", "webm"} if hint == "webm" else {"mov", "mp4", "m4v"})),
            "actual_video_container_mismatch",
        )
        for field in ("width", "height"):
            if metadata.get(field) is not None:
                require(
                    any(s.get("codec_type") == "video" and s.get(field) == metadata[field] for s in streams),
                    "video_" + field + "_mismatch",
                )
        decoded = subprocess.run(
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-xerror",
                "-threads",
                "1",
                *controls,
                "-i",
                str(path),
                "-map",
                "0:v?",
                "-map",
                "0:a?",
                "-progress",
                "pipe:1",
                "-nostats",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        require(decoded.returncode == 0, "complete_video_or_audio_decode_failed")
        progress = dict(line.split("=", 1) for line in decoded.stdout.splitlines() if "=" in line)
        require(
            progress.get("progress") == "end" and int(progress.get("frame", "0")) > 0, "decoded_video_frames_missing"
        )
        return {
            "mode": mode,
            "all_video_audio_streams_decoded": True,
            "decoded_video_frames": int(progress["frame"]),
            "streams": streams,
            "format": info.get("format", {}),
            "actual_mime_types": ["video/webm"] if hint == "webm" else ["video/mp4", "video/quicktime", "video/x-m4v"],
        }
    require(mode in {"image_full_decode", "gif_all_frames_native_identity"}, "required_decode_mode_unknown")
    with Image.open(path) as image:
        image.verify()
    with Image.open(path) as image:
        fmt = image.format
        size = image.size
        frames = getattr(image, "n_frames", 1)
        durations = []
        loop = image.info.get("loop")
        require(frames > 0, "image_frames_missing")
        for i in range(frames):
            image.seek(i)
            image.load()
            durations.append(image.info.get("duration"))
        wanted = (metadata.get("format") or "").upper()
        require(not wanted or {"JPG": "JPEG"}.get(wanted, wanted) == fmt, "image_format_mismatch")
        for field, value in zip(("width", "height"), size, strict=True):
            require(metadata.get(field) is None or metadata[field] == value, "image_" + field + "_mismatch")
        if mode == "gif_all_frames_native_identity":
            require(fmt == "GIF" and frames > 1, "native_gif_animation_not_delivered")
        return {
            "mode": mode,
            "format": fmt,
            "width": size[0],
            "height": size[1],
            "all_frames_decoded": True,
            "frame_count": frames,
            "frame_durations_ms": durations,
            "loop": loop,
            "actual_mime_types": [
                {"JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp", "GIF": "image/gif"}[fmt]
            ],
        }


def physical_plan(report, base):
    require(
        report.get("operator_scope") == "media_records_and_byte_plan_not_final_certifier"
        and report.get("run_id") == common.RUN
        and report.get("intent_hash") == common.INTENT
        and report.get("full_history_gate_closed") is True,
        "media_record_proof_binding_invalid",
    )
    rows = report.get("verified", [])
    verified = {entry["shortcode"]: entry for entry in rows}
    require(len(verified) == len(rows) == report.get("verified_targets"), "verified_record_cardinality_mismatch")
    require(report["status"] in {"passed", "pending", "blocked"}, "record_status_invalid")
    if report["status"] == "passed":
        require(
            not report.get("failures")
            and not report.get("pending")
            and len(rows) == report.get("audited_january_count"),
            "passed_record_inventory_incomplete",
        )
    plan = report.get("byte_read_plan", {}).get("objects", [])
    require(
        bool(plan) and report["byte_read_plan"].get("status") == "ready_for_verified_subset",
        "verified_byte_plan_not_ready",
    )
    role_ids = [(r["shortcode"], r["role"]) for r in plan]
    require(len(role_ids) == len(set(role_ids)), "duplicate_byte_role_identity")
    require({r[0] for r in role_ids} == set(verified), "verified_target_roles_missing")
    require(
        all(sum(r[0] == key for r in role_ids) == row["object_roles"] for key, row in verified.items()),
        "verified_object_role_count_mismatch",
    )
    require(
        all(
            set(row["object_role_sha256"]) == {role for target, role in role_ids if target == key}
            for key, row in verified.items()
        ),
        "verified_object_role_manifest_mismatch",
    )
    require(
        report["byte_read_plan"].get("physical_urls") == len({r["url"] for r in plan}), "physical_url_count_mismatch"
    )
    physical = {}
    for entry in plan:
        require(entry["shortcode"] in verified, "object_not_in_verified_record_subset")
        for field in (
            "role",
            "original_observed_at",
            "source_evidence_id",
            "source_evidence_sha256",
            "source_evidence_parser",
            "source_evidence_schema",
            "required_decode_mode",
        ):
            require(entry.get(field) is not None, "object_plan_provenance_missing")
        require(
            entry["source_evidence_sha256"] == verified[entry["shortcode"]]["source_hash"],
            "object_evidence_hash_not_verified",
        )
        record = verified[entry["shortcode"]]
        require(
            record["object_role_sha256"][entry["role"]] == digest(entry), "object_role_provenance_fingerprint_changed"
        )
        require(
            all(entry.get(k) == v for k, v in record["record_identity"].items()),
            "object_record_canonical_identity_mismatch",
        )
        require(
            entry["source_evidence_id"] == record["source_evidence_id"]
            and entry["source_evidence_parser"] == "instagram-media-result-v1"
            and type(entry["source_evidence_schema"]) is int
            and entry["source_evidence_schema"] == 1,
            "object_evidence_identity_or_version_mismatch",
        )
        require(
            re.fullmatch("[a-f0-9]{64}", entry["source_evidence_sha256"]) is not None, "object_evidence_hash_invalid"
        )
        require(common.stamp(entry["original_observed_at"]).utcoffset() is not None, "object_original_clock_invalid")
        for field in ("legacy_post_id", "canonical_post_id", "source_evidence_id"):
            UUID(entry[field])
        require(
            entry["legacy_post_id"] == record["comment_binding"]["post_id"]
            and entry["media_reservation"] == record["comment_binding"]["media_reservation"]
            and common.stamp(entry["original_post_observed_at"]) == common.stamp(record["original_observed_at"]),
            "object_record_identity_or_clock_mismatch",
        )
        mode = entry["required_decode_mode"]
        role = entry["role"]
        require(
            mode in {"image_full_decode", "video_full_stream", "gif_all_frames_native_identity"},
            "object_decode_mode_invalid",
        )
        if role.startswith("avatar:"):
            identity = entry.get("avatar_identity") or {}
            require(
                identity in record["avatar_slots"]
                and identity.get("registry_status") == "mirrored"
                and common.stamp(entry["original_observed_at"]) == common.stamp(identity["registry_observed_at"])
                and entry["source_reference"] == identity["source_reference"],
                "avatar_role_provenance_mismatch",
            )
        elif role.startswith("saved_owner_asset_source:"):
            identity = entry.get("saved_owner_asset_source") or {}
            require(
                identity in record.get("saved_owner_asset_sources", [])
                and identity.get("source_url") == entry["url"]
                and identity.get("source_key") == entry["storage_key"]
                and identity.get("sha256") == entry["historical_sha256"]
                and identity.get("source_reference") == entry["source_reference"]
                and common.stamp(identity.get("byte_clock")) == common.stamp(entry["original_observed_at"])
                and identity.get("historical_handle") != identity.get("current_handle")
                and mode == "image_full_decode",
                "saved_owner_asset_source_provenance_mismatch",
            )
        elif role.startswith("comment:"):
            require(
                entry.get("comment_id") in record["comment_binding"]["observed_comment_ids"],
                "comment_role_inventory_mismatch",
            )
            UUID(entry["comment_row_id"])
        else:
            UUID(entry["canonical_asset_id"])
            require(
                common.stamp(entry["original_observed_at"]) == common.stamp(record["original_observed_at"]),
                "post_role_original_clock_mismatch",
            )
        native = entry.get("native_gif_proof") or {}
        require(bool(native.get("native_gif")) == bool(entry.get("native_gif")), "native_gif_proof_flag_mismatch")
        require(
            bool(entry.get("native_gif")) == (mode == "gif_all_frames_native_identity"),
            "native_gif_decode_mode_mismatch",
        )
        if mode == "gif_all_frames_native_identity":
            require(
                role.startswith("comment:")
                and native.get("native_gif") is True
                and entry.get("required_animation") == "all_frames_native_identity_no_unavailable_placeholder",
                "native_gif_bound_proof_missing",
            )
            if native.get("kind") == "attachment_refresh":
                from trr_backend.ingestion.comment_attachment_refresh import AttachmentRefresh

                refresh = AttachmentRefresh.from_payload(native["proof"])
                require(
                    refresh.row_id == entry["comment_row_id"]
                    and refresh.comment_id == entry["comment_id"]
                    and refresh.post_id == entry["legacy_post_id"]
                    and refresh.shortcode == entry["shortcode"]
                    and refresh.source_url == entry["source_reference"]
                    and refresh.captured_at >= common.stamp(entry["original_observed_at"]),
                    "native_gif_refresh_role_binding_mismatch",
                )
            else:
                from trr_backend.ingestion.comment_attachment_refresh import _retained_giphy, _selected_url

                require(
                    native.get("kind") == "native_giphy"
                    and native.get("native_gif_id")
                    and native.get("giphy_media_info", {}).get("id") == native["native_gif_id"]
                    and native.get("row_id") == entry["comment_row_id"]
                    and native.get("comment_id") == entry["comment_id"]
                    and native.get("post_id") == entry["legacy_post_id"]
                    and _selected_url(_retained_giphy(native["giphy_media_info"])) == entry["source_reference"],
                    "native_gif_current_role_binding_mismatch",
                )
        key = object_key(entry["url"], entry.get("storage_key"), base)
        data = physical.setdefault(key, {"storage_key": key, "url": entry["url"], "roles": []})
        data.setdefault("urls", [data["url"]])
        if entry["url"] not in data["urls"]:
            data["urls"].append(entry["url"])
        for field in (
            "expected_bytes",
            "historical_sha256",
            "format",
            "content_type",
            "width",
            "height",
            "required_decode_mode",
        ):
            value = entry.get(field)
            if value is not None:
                require(field not in data or data[field] == value, "shared_object_metadata_conflict")
                data[field] = value
        if entry.get("historical_sha256") is not None:
            require(
                re.fullmatch("[a-f0-9]{64}", entry["historical_sha256"]) is not None, "historical_object_hash_invalid"
            )
        data["roles"].append(entry)
    for data in physical.values():
        for field in ("expected_bytes", "historical_sha256", "format", "content_type", "width", "height"):
            data.setdefault(field, None)
        require(
            data["expected_bytes"] is None or type(data["expected_bytes"]) is int and 0 < data["expected_bytes"],
            "expected_object_length_invalid",
        )
    return list(physical.values())


def inspect_object(item, client, config, *, stop=None):
    import requests

    signature = digest(item)
    retry_after_seconds = None
    public_status = None
    provider_code = None
    request_id = None
    try:
        require(stop is None or not stop.is_set(), "hosted_reads_paused_after_rate_limit")
        with tempfile.TemporaryDirectory(prefix="trr-january-byte-audit-") as folder:
            storage = Path(folder) / "storage.bin"
            response = client.get_object(Bucket=config.bucket, Key=item["storage_key"])
            body = response["Body"]
            try:
                require(
                    response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
                    and not response.get("ContentRange"),
                    "storage_full_success_required",
                )
                length = int(response["ContentLength"])
                require(length > 0, "storage_length_invalid")
                size, hashed = copy_body(body.read, storage, length)
            finally:
                body.close()
            require(item["expected_bytes"] is None or size == item["expected_bytes"], "retained_object_length_mismatch")
            require(
                item["historical_sha256"] is None or hashed == item["historical_sha256"],
                "retained_original_hash_mismatch",
            )
            require(hashed not in _SOURCE_PLACEHOLDER_SHA256, "known_unavailable_placeholder_delivered")
            decoded = decode_file(storage, item)
            storage_type = mime(response.get("ContentType"))
            require(storage_type in decoded["actual_mime_types"], "storage_mime_does_not_match_decoded_bytes")
            if item["content_type"]:
                require(storage_type == mime(item["content_type"]), "storage_content_type_mismatch")
            urls = item.get("urls", [item["url"]])
            for index, url in enumerate(urls):
                require(stop is None or not stop.is_set(), "hosted_reads_paused_after_rate_limit")
                cdn = Path(folder) / f"cdn-{index}.bin"
                with requests.Session() as session:
                    session.trust_env = False
                    with session.get(
                        url,
                        headers={"Accept-Encoding": "identity", "User-Agent": PUBLIC_STORAGE_USER_AGENT},
                        stream=True,
                        allow_redirects=False,
                        timeout=(10, 60),
                    ) as public:
                        public_status = public.status_code
                        ray = public.headers.get("CF-Ray", "")
                        if re.fullmatch(r"[A-Za-z0-9-]{1,80}", ray):
                            request_id = ray
                        if public.status_code == 403:
                            # Retain only the bounded provider code, never the
                            # error body. A client-signature denial says nothing
                            # about the stored bytes or Instagram availability.
                            denial = public.raw.read(512)
                            match = re.search(rb"\berror code:\s*(\d{4})\b", denial, re.IGNORECASE)
                            provider_code = match.group(1).decode("ascii") if match else None
                            raise common.GuardError(
                                "cdn_client_signature_denied" if provider_code == "1010" else "cdn_access_denied"
                            )
                        if public.status_code == 429:
                            if stop is not None:
                                stop.set()
                            value = public.headers.get("Retry-After", "")
                            retry_after_seconds = 60
                            if value.isascii() and value.isdigit() and len(value) <= 12:
                                retry_after_seconds = max(1, int(value))
                            else:
                                try:
                                    retry_after_seconds = max(
                                        1, int((parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
                                    )
                                except (ValueError, TypeError, OverflowError):
                                    pass
                            raise common.GuardError("cdn_rate_limited")
                        require(
                            public.status_code == 200 and not public.headers.get("Content-Range"),
                            "cdn_full_success_required",
                        )
                        require(
                            public.headers.get("Content-Encoding", "identity").lower() in {"", "identity"},
                            "cdn_encoded_body_not_exact_object",
                        )
                        claimed = public.headers.get("Content-Length")
                        if claimed is not None:
                            require(int(claimed) == size, "cdn_header_length_mismatch")
                        require(mime(public.headers.get("Content-Type")) == storage_type, "cdn_content_type_mismatch")
                        public.raw.decode_content = False
                        public_size, public_hash = copy_body(public.raw.read, cdn, size)
                require(public_size == size and public_hash == hashed, "storage_and_cdn_full_bytes_differ")
                cdn.unlink()  # Dedicated task temporary body, keep disk bounded per worker.
            return {
                "status": "verified",
                "object_signature": signature,
                "storage_key": item["storage_key"],
                "roles": item["roles"],
                "verified_at": datetime.now(UTC).isoformat(),
                "bytes": size,
                "observed_sha256": hashed,
                "verified_public_urls": urls,
                "original_hash_comparison": "matched_retained_hash"
                if item["historical_sha256"]
                else "historical_hash_unavailable_current_bytes_only",
                "storage_cdn_full_byte_equality": True,
                "decode": decoded,
                "storage_gets": 1,
                "cdn_gets": len(urls),
            }
    except Exception as exc:
        code = str(exc) if isinstance(exc, common.GuardError) else type(exc).__name__
        return {
            "status": "failed",
            "object_signature": signature,
            "storage_key": item["storage_key"],
            "roles": [{"shortcode": role["shortcode"], "role": role["role"]} for role in item["roles"]],
            "error_code": code,
            "retry_after_seconds": retry_after_seconds,
            "cdn_http_status": public_status,
            "cdn_provider_code": provider_code,
            "cdn_request_id": request_id,
        }
