"""Extract and serve source subtitles for retained cast-screentime video assets."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from trr_backend.object_storage import build_s3_client, load_object_storage_config
from trr_backend.repositories import cast_screentime

SRT_CONTENT_TYPE = "application/x-subrip; charset=utf-8"
CUE_JSON_CONTENT_TYPE = "application/json; charset=utf-8"
SCHEMA_VERSION = "screenalytics.subtitle_cues.v1"
OBJECT_SCHEMA_VERSION = "screenalytics.subtitle.v1"
TEXT_SUBTITLE_CODECS = frozenset({"mov_text", "subrip", "srt", "webvtt", "ass", "ssa", "text"})
SRT_TIMESTAMP_RE = re.compile(
    r"^(?P<sh>\d{2,}):(?P<sm>\d{2}):(?P<ss>\d{2})[,.](?P<sms>\d{3})\s+-->\s+"
    r"(?P<eh>\d{2,}):(?P<em>\d{2}):(?P<es>\d{2})[,.](?P<ems>\d{3})(?:\s+.*)?$"
)
HTML_TAG_RE = re.compile(r"</?(?:b|i|u|font)(?:\s+[^>]*)?>", re.IGNORECASE)
ASS_OVERRIDE_RE = re.compile(r"\{\\[^}]+\}")


class SubtitleExtractionError(RuntimeError):
    """Safe extraction error that may be persisted without subtitle contents."""


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(str(os.getenv(name) or default).strip()))
    except ValueError:
        return default


def normalize_subtitle_language(value: Any) -> str | None:
    raw = str(value or "").strip().replace("_", "-").lower()
    if raw == "eng" or raw == "en" or raw.startswith("en-"):
        return "en"
    return raw or None


def _subtitle_selection(*, codec_name: str, language_raw: Any) -> tuple[str, str]:
    normalized = normalize_subtitle_language(language_raw)
    if codec_name.lower() not in TEXT_SUBTITLE_CODECS:
        return "unsupported_codec", "unsupported"
    if normalized is None:
        return "skipped_unknown_language", "skipped"
    if normalized != "en":
        return "skipped_non_english", "skipped"
    return "eligible_english", "detected"


def probe_subtitle_streams(video_path: str | Path) -> list[dict[str, Any]]:
    """Inventory subtitle streams with ffprobe and classify English text tracks."""
    timeout = _env_int("TRR_MODAL_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS", 7200)
    command = ["ffprobe", "-v", "error", "-show_streams", "-of", "json", str(video_path)]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SubtitleExtractionError(f"ffprobe_unavailable:{type(exc).__name__}") from exc
    if result.returncode != 0:
        detail = (result.stderr or "ffprobe failed").strip()[-1000:]
        raise SubtitleExtractionError(f"ffprobe_failed:{detail}")
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise SubtitleExtractionError("ffprobe_invalid_json") from exc

    inventory: list[dict[str, Any]] = []
    for raw_stream in payload.get("streams", []):
        if not isinstance(raw_stream, dict) or raw_stream.get("codec_type") != "subtitle":
            continue
        raw_index = raw_stream.get("index")
        if isinstance(raw_index, bool):
            continue
        try:
            stream_index = int(raw_index)
        except (TypeError, ValueError):
            continue
        if stream_index < 0 or isinstance(raw_index, float) and not raw_index.is_integer():
            continue
        tags = raw_stream.get("tags") if isinstance(raw_stream.get("tags"), dict) else {}
        disposition = raw_stream.get("disposition") if isinstance(raw_stream.get("disposition"), dict) else {}
        codec_name = str(raw_stream.get("codec_name") or "unknown").strip().lower()
        language_raw = str(tags.get("language") or "").strip() or None
        selection_status, extraction_status = _subtitle_selection(codec_name=codec_name, language_raw=language_raw)
        inventory.append(
            {
                "stream_index": stream_index,
                "codec_name": codec_name,
                "language_raw": language_raw,
                "language_normalized": normalize_subtitle_language(language_raw),
                "title": str(tags.get("title") or "").strip() or None,
                "handler_name": str(tags.get("handler_name") or "").strip() or None,
                "is_default": bool(disposition.get("default")),
                "is_forced": bool(disposition.get("forced")),
                "selection_status": selection_status,
                "extraction_status": extraction_status,
                "metadata": {"ffprobe": {"codec_long_name": raw_stream.get("codec_long_name")}},
            }
        )
    max_tracks = _env_int("CAST_SCREENTIME_SUBTITLE_MAX_TRACKS", 16)
    if len(inventory) > max_tracks:
        raise SubtitleExtractionError("subtitle_track_limit_exceeded")
    return inventory


def _timestamp_ms(match: re.Match[str], prefix: str) -> int:
    hours = int(match.group(f"{prefix}h"))
    minutes = int(match.group(f"{prefix}m"))
    seconds = int(match.group(f"{prefix}s"))
    millis = int(match.group(f"{prefix}ms"))
    if minutes >= 60 or seconds >= 60:
        raise SubtitleExtractionError("srt_invalid_timestamp")
    return (((hours * 60) + minutes) * 60 + seconds) * 1000 + millis


def _format_timestamp(value_ms: int) -> str:
    hours, remainder = divmod(value_ms, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{millis:03d}"


def _plain_text(value: str) -> str:
    return html.unescape(ASS_OVERRIDE_RE.sub("", HTML_TAG_RE.sub("", value)))


def parse_and_normalize_srt(raw: bytes) -> tuple[bytes, list[dict[str, Any]]]:
    """Validate SRT, normalize encoding/line endings/ordinals, and return parsed cues."""
    max_bytes = _env_int("CAST_SCREENTIME_SUBTITLE_MAX_SRT_BYTES", 26_214_400)
    if len(raw) > max_bytes:
        raise SubtitleExtractionError("subtitle_srt_size_limit_exceeded")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SubtitleExtractionError("subtitle_srt_not_utf8") from exc
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        raise SubtitleExtractionError("subtitle_srt_empty")
    blocks = re.split(r"\n{2,}", text)
    cues: list[dict[str, Any]] = []
    rendered: list[str] = []
    for block in blocks:
        lines = block.split("\n")
        if lines and lines[0].strip().isdigit():
            lines = lines[1:]
        if len(lines) < 2:
            raise SubtitleExtractionError("subtitle_srt_malformed_cue")
        match = SRT_TIMESTAMP_RE.match(lines[0].strip())
        if match is None:
            raise SubtitleExtractionError("subtitle_srt_malformed_timestamp")
        start_ms = _timestamp_ms(match, "s")
        end_ms = _timestamp_ms(match, "e")
        if end_ms < start_ms:
            raise SubtitleExtractionError("subtitle_srt_end_before_start")
        cue_text = "\n".join(lines[1:]).strip()
        if not cue_text:
            raise SubtitleExtractionError("subtitle_srt_empty_cue")
        ordinal = len(cues) + 1
        cues.append(
            {
                "ordinal": ordinal,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "text": cue_text,
                "plain_text": _plain_text(cue_text),
            }
        )
        rendered.append(f"{ordinal}\n{_format_timestamp(start_ms)} --> {_format_timestamp(end_ms)}\n{cue_text}")
    max_cues = _env_int("CAST_SCREENTIME_SUBTITLE_MAX_CUES", 200_000)
    if not cues or len(cues) > max_cues:
        raise SubtitleExtractionError("subtitle_cue_limit_exceeded")
    # Terminate the final cue with the same blank-line separator used between
    # cues. This preserves canonical FFmpeg SRT bytes for already-normalized input.
    normalized = ("\n\n".join(rendered) + "\n\n").encode("utf-8")
    if len(normalized) > max_bytes:
        raise SubtitleExtractionError("subtitle_srt_size_limit_exceeded")
    return normalized, cues


def extract_srt_stream(video_path: str | Path, stream_index: int, output_path: str | Path) -> None:
    timeout = _env_int("TRR_MODAL_CAST_SCREENTIME_SUBTITLE_TIMEOUT_SECONDS", 7200)
    command = [
        "ffmpeg",
        "-v",
        "error",
        "-y",
        "-i",
        str(video_path),
        "-map",
        f"0:{stream_index}",
        "-c:s",
        "srt",
        "-f",
        "srt",
        str(output_path),
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SubtitleExtractionError(f"ffmpeg_unavailable:{type(exc).__name__}") from exc
    if result.returncode != 0:
        detail = (result.stderr or b"ffmpeg failed").decode("utf-8", errors="replace").strip()[-1000:]
        raise SubtitleExtractionError(f"ffmpeg_failed:{detail}")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _primary_sort_key(track: dict[str, Any]) -> tuple[int, int]:
    is_default = bool(track.get("is_default"))
    is_forced = bool(track.get("is_forced"))
    if is_default and not is_forced:
        rank = 0
    elif not is_forced:
        rank = 1
    elif is_default:
        rank = 2
    else:
        rank = 3
    return rank, int(track["stream_index"])


def _cue_payload(
    *,
    video_asset_id: str,
    track: dict[str, Any],
    srt_sha256: str,
    cues: list[dict[str, Any]],
    is_primary: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "video_asset_id": video_asset_id,
        "subtitle_track_id": str(track["id"]),
        "source": "embedded",
        "stream_index": int(track["stream_index"]),
        "codec_name": track["codec_name"],
        "language": track.get("language_normalized") or track.get("language") or "en",
        "language_raw": track.get("language_raw"),
        "title": track.get("title"),
        "is_default": bool(track.get("is_default")),
        "is_forced": bool(track.get("is_forced")),
        "is_primary": is_primary,
        "srt_sha256": srt_sha256,
        "cue_count": len(cues),
        "first_cue_start_ms": cues[0]["start_ms"],
        "last_cue_end_ms": cues[-1]["end_ms"],
        "cues": cues,
    }


def _object_metadata(video_asset_id: str, track: dict[str, Any], srt_sha256: str) -> dict[str, str]:
    return {
        "video-asset-id": video_asset_id,
        "subtitle-track-id": str(track["id"]),
        "stream-index": str(track["stream_index"]),
        "language": str(track.get("language_normalized") or track.get("language") or "en"),
        "codec": str(track["codec_name"]),
        "source": "embedded",
        "schema-version": OBJECT_SCHEMA_VERSION,
        "srt-sha256": srt_sha256,
    }


def _put_object_pair(
    client: Any,
    bucket: str,
    *,
    srt_key: str,
    cue_key: str,
    srt: bytes,
    cue_json: bytes,
    metadata: dict[str, str],
) -> tuple[set[str], dict[str, dict[str, Any]]]:
    def _head(key: str) -> dict[str, Any] | None:
        try:
            return client.head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            response = getattr(exc, "response", {})
            code = str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            # Minimal/fake clients often use KeyError for a missing object.
            if isinstance(exc, KeyError):
                return None
            raise

    uploaded: list[str] = []
    replaced: dict[str, dict[str, Any]] = {}
    try:
        if _head(srt_key) is None:
            client.put_object(
                Bucket=bucket,
                Key=srt_key,
                Body=srt,
                ContentType=SRT_CONTENT_TYPE,
                CacheControl="private, no-store",
                Metadata=metadata,
            )
            uploaded.append(srt_key)
        cue_head = _head(cue_key)
        if cue_head is not None:
            response = client.get_object(Bucket=bucket, Key=cue_key)
            body = response.get("Body")
            if body is None:
                raise SubtitleExtractionError("existing_cue_object_body_missing")
            replaced[cue_key] = {
                "Body": body.read(),
                "ContentType": cue_head.get("ContentType") or CUE_JSON_CONTENT_TYPE,
                "CacheControl": cue_head.get("CacheControl") or "private, no-store",
                "Metadata": cue_head.get("Metadata") or {},
            }
        client.put_object(
            Bucket=bucket,
            Key=cue_key,
            Body=cue_json,
            ContentType=CUE_JSON_CONTENT_TYPE,
            CacheControl="private, no-store",
            Metadata=metadata,
        )
        if cue_head is None:
            uploaded.append(cue_key)
        client.head_object(Bucket=bucket, Key=srt_key)
        client.head_object(Bucket=bucket, Key=cue_key)
        return set(uploaded), replaced
    except Exception:
        for key, prior in replaced.items():
            try:
                client.put_object(Bucket=bucket, Key=key, **prior)
            except Exception:
                pass
        for key in uploaded:
            try:
                client.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
        raise


def _download_source(client: Any, bucket: str, object_key: str, destination: Path) -> None:
    with destination.open("wb") as output:
        response = client.get_object(Bucket=bucket, Key=object_key)
        body = response.get("Body")
        if body is None:
            raise SubtitleExtractionError("source_object_body_missing")
        while True:
            chunk = body.read(8 * 1024 * 1024)
            if not chunk:
                break
            output.write(chunk)


def _resolve_storage(storage_client: Any | None, bucket: str | None) -> tuple[Any, str]:
    if storage_client is not None and bucket:
        return storage_client, bucket
    config = load_object_storage_config(require_bucket=True)
    return storage_client or build_s3_client(config), bucket or config.bucket


def extract_video_asset_subtitles(
    video_asset_id: str,
    force: bool = False,
    *,
    local_video_path: str | Path | None = None,
    storage_client: Any | None = None,
    bucket: str | None = None,
) -> dict[str, Any]:
    """Worker entrypoint: extract all eligible English tracks and publish them atomically."""
    asset = cast_screentime.get_video_asset(video_asset_id)
    if not asset:
        raise SubtitleExtractionError("video_asset_not_found")
    claimed = cast_screentime.claim_subtitle_extraction(video_asset_id, force=force)
    if not claimed:
        summary = cast_screentime.get_subtitle_summary(video_asset_id, include_skipped=True)
        return summary or {"video_asset_id": video_asset_id, "status": "not_found"}

    source_json = asset.get("source_json") if isinstance(asset.get("source_json"), dict) else {}
    source_key = str(source_json.get("object_key") or "").strip()
    try:
        client, storage_bucket = _resolve_storage(storage_client, bucket)
        with tempfile.TemporaryDirectory(prefix="trr-subtitles-") as temp_dir:
            temp_root = Path(temp_dir)
            if local_video_path is None:
                if not source_key:
                    raise SubtitleExtractionError("video_asset_source_object_missing")
                suffix = Path(source_key).suffix or ".mp4"
                video_path = temp_root / f"source{suffix}"
                _download_source(client, storage_bucket, source_key, video_path)
            else:
                video_path = Path(local_video_path)
                if not video_path.is_file():
                    raise SubtitleExtractionError("local_video_not_found")

            inventory = probe_subtitle_streams(video_path)
            persisted_tracks = cast_screentime.upsert_subtitle_track_inventory(video_asset_id, inventory)
            eligible = sorted(
                (track for track in persisted_tracks if track.get("selection_status") == "eligible_english"),
                key=_primary_sort_key,
            )
            primary_selected = False

            for track in eligible:
                track_id = str(track["id"])
                had_completed = track.get("extraction_status") == "complete"
                cast_screentime.update_subtitle_track(track_id, {"extraction_status": "extracting", "error_text": None})
                raw_path = temp_root / f"stream-{track['stream_index']}.srt"
                created_keys: set[str] = set()
                replaced_objects: dict[str, dict[str, Any]] = {}
                try:
                    extract_srt_stream(video_path, int(track["stream_index"]), raw_path)
                    normalized_srt, cues = parse_and_normalize_srt(raw_path.read_bytes())
                    srt_hash = _sha256(normalized_srt)
                    # Tracks are processed in primary-preference order. This remains
                    # false until a candidate has fully published, so a failed first
                    # candidate promotes the next successful track in both JSON and DB.
                    is_primary = not primary_selected
                    cue_payload = _cue_payload(
                        video_asset_id=video_asset_id,
                        track=track,
                        srt_sha256=srt_hash,
                        cues=cues,
                        is_primary=is_primary,
                    )
                    cue_json = json.dumps(cue_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    cue_hash = _sha256(cue_json)
                    base_key = f"source/videos/{video_asset_id}/subtitles/stream-{track['stream_index']}/v1-{srt_hash}"
                    srt_key = f"{base_key}/captions.srt"
                    # Cue JSON includes mutable track-level facts such as is_primary,
                    # so its own checksum must participate in the object key. The SRT
                    # remains independently content-addressed by the source text hash.
                    cue_key = f"{base_key}/cues-{cue_hash}.json"
                    created_keys, replaced_objects = _put_object_pair(
                        client,
                        storage_bucket,
                        srt_key=srt_key,
                        cue_key=cue_key,
                        srt=normalized_srt,
                        cue_json=cue_json,
                        metadata=_object_metadata(video_asset_id, track, srt_hash),
                    )
                    previous_keys = (track.get("srt_object_key"), track.get("cue_json_object_key"))
                    cast_screentime.update_subtitle_track(
                        track_id,
                        {
                            "extraction_status": "complete",
                            "srt_object_key": srt_key,
                            "cue_json_object_key": cue_key,
                            "srt_content_type": SRT_CONTENT_TYPE,
                            "cue_json_content_type": CUE_JSON_CONTENT_TYPE,
                            "cue_count": len(cues),
                            "first_cue_start_ms": cues[0]["start_ms"],
                            "last_cue_end_ms": cues[-1]["end_ms"],
                            "srt_size_bytes": len(normalized_srt),
                            "cue_json_size_bytes": len(cue_json),
                            "srt_sha256": srt_hash,
                            "cue_json_sha256": cue_hash,
                            "error_text": None,
                        },
                    )
                    if is_primary:
                        primary_selected = True
                    for old_key in previous_keys:
                        if old_key and old_key not in {srt_key, cue_key}:
                            try:
                                client.delete_object(Bucket=storage_bucket, Key=old_key)
                            except Exception:
                                pass
                except Exception as exc:
                    for replaced_key, prior in replaced_objects.items():
                        try:
                            client.put_object(Bucket=storage_bucket, Key=replaced_key, **prior)
                        except Exception:
                            pass
                    for created_key in created_keys:
                        try:
                            client.delete_object(Bucket=storage_bucket, Key=created_key)
                        except Exception:
                            pass
                    safe_error = str(exc).strip()[-1000:] or type(exc).__name__
                    cast_screentime.update_subtitle_track(
                        track_id,
                        {
                            "extraction_status": "complete" if had_completed else "failed",
                            "error_text": safe_error,
                        },
                    )
                    if had_completed and not primary_selected:
                        primary_selected = True

            completed = [
                track
                for track in (cast_screentime.get_subtitle_summary(video_asset_id, True) or {}).get("tracks", [])
                if track.get("selection_status") == "eligible_english" and track.get("extraction_status") == "complete"
            ]
            if completed:
                primary = min(completed, key=_primary_sort_key)
                cast_screentime.set_primary_subtitle_track(video_asset_id, str(primary["id"]))
            else:
                cast_screentime.clear_primary_subtitle_track(video_asset_id)
            cast_screentime.finalize_subtitle_extraction(video_asset_id)
    except Exception as exc:
        safe_error = str(exc).strip()[-1000:] or type(exc).__name__
        cast_screentime.fail_subtitle_extraction(video_asset_id, safe_error)

    return cast_screentime.get_subtitle_summary(video_asset_id, include_skipped=True) or {
        "video_asset_id": video_asset_id,
        "status": "failed",
    }


def load_subtitle_cues(
    video_asset_id: str,
    track_id: str,
    *,
    offset: int = 0,
    limit: int = 200,
    query: str | None = None,
    storage_client: Any | None = None,
    bucket: str | None = None,
) -> dict[str, Any]:
    if offset < 0 or limit < 1 or limit > 1000 or len(query or "") > 200:
        raise ValueError("invalid subtitle cue pagination or search")
    track = cast_screentime.get_subtitle_track(video_asset_id, track_id)
    if not track:
        raise KeyError("subtitle_track_not_found")
    if track.get("extraction_status") != "complete" or not track.get("cue_json_object_key"):
        raise SubtitleExtractionError("subtitle_track_incomplete")
    client, storage_bucket = _resolve_storage(storage_client, bucket)
    response = client.get_object(Bucket=storage_bucket, Key=track["cue_json_object_key"])
    body = response.get("Body")
    if body is None:
        raise SubtitleExtractionError("subtitle_cue_object_missing")
    payload = json.loads(body.read())
    cues = payload.get("cues") if isinstance(payload.get("cues"), list) else []
    normalized_query = str(query or "").strip().casefold()
    matches = (
        [cue for cue in cues if normalized_query in str(cue.get("plain_text") or "").casefold()]
        if normalized_query
        else cues
    )
    return {
        "video_asset_id": video_asset_id,
        "track_id": track_id,
        "offset": offset,
        "limit": limit,
        "total_cues": len(cues),
        "matched_cues": len(matches),
        "items": matches[offset : offset + limit],
    }


def _safe_download_stem(asset: dict[str, Any]) -> str:
    source_json = asset.get("source_json") if isinstance(asset.get("source_json"), dict) else {}
    metadata = asset.get("metadata") if isinstance(asset.get("metadata"), dict) else {}
    raw = str(
        source_json.get("original_filename")
        or metadata.get("original_filename")
        or source_json.get("object_key")
        or "video"
    )
    stem = Path(raw).stem
    stem = re.sub(r"[\\/\x00-\x1f\x7f\";]+", "_", stem).strip(" ._")
    return stem[:180] or "video"


def generate_subtitle_download_url(
    video_asset_id: str,
    track_id: str,
    *,
    storage_client: Any | None = None,
    bucket: str | None = None,
) -> dict[str, Any]:
    track = cast_screentime.get_subtitle_track(video_asset_id, track_id)
    asset = cast_screentime.get_video_asset(video_asset_id)
    if not track or not asset:
        raise KeyError("subtitle_track_not_found")
    if track.get("extraction_status") != "complete" or not track.get("srt_object_key"):
        raise SubtitleExtractionError("subtitle_track_incomplete")
    client, storage_bucket = _resolve_storage(storage_client, bucket)
    ttl = _env_int("CAST_SCREENTIME_SUBTITLE_DOWNLOAD_TTL_SECONDS", 300)
    language = str(track.get("language_normalized") or track.get("language") or "en")
    filename = f"{_safe_download_stem(asset)}.stream-{track['stream_index']}.{language}.srt"
    url = client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": storage_bucket,
            "Key": track["srt_object_key"],
            "ResponseContentType": SRT_CONTENT_TYPE,
            "ResponseContentDisposition": f'attachment; filename="{filename}"',
        },
        ExpiresIn=ttl,
    )
    expires_at = datetime.now(UTC) + timedelta(seconds=ttl)
    return {
        "video_asset_id": video_asset_id,
        "track_id": track_id,
        "filename": filename,
        "content_type": SRT_CONTENT_TYPE,
        "expires_in_seconds": ttl,
        "expires_at": expires_at.isoformat(),
        "download_url": url,
    }
