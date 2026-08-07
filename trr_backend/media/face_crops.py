from __future__ import annotations

import hashlib
import io
import json
import logging
from collections.abc import Mapping
from typing import Any, cast

import requests
from PIL import Image, ImageOps

from trr_backend.media.s3_mirror import build_hosted_url, get_s3_bucket, get_s3_client

logger = logging.getLogger(__name__)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalize_square_bbox_from_face_box(
    box: Mapping[str, Any],
    *,
    padding: float = 0.35,
) -> list[float] | None:
    x = box.get("x")
    y = box.get("y")
    width = box.get("width")
    height = box.get("height")
    if not (
        isinstance(x, (int, float))
        and isinstance(y, (int, float))
        and isinstance(width, (int, float))
        and isinstance(height, (int, float))
    ):
        return None

    x = float(x)
    y = float(y)
    width = max(0.0, float(width))
    height = max(0.0, float(height))
    if width <= 0 or height <= 0:
        return None

    side = max(width, height) * (1.0 + (2.0 * max(0.0, float(padding))))
    cx = x + (width / 2.0)
    cy = y + (height / 2.0)
    left = cx - (side / 2.0)
    top = cy - (side / 2.0)
    right = left + side
    bottom = top + side

    if left < 0:
        right -= left
        left = 0.0
    if top < 0:
        bottom -= top
        top = 0.0
    if right > 1.0:
        shift = right - 1.0
        left -= shift
        right = 1.0
    if bottom > 1.0:
        shift = bottom - 1.0
        top -= shift
        bottom = 1.0

    left = _clamp(left, 0.0, 1.0)
    top = _clamp(top, 0.0, 1.0)
    right = _clamp(max(left + 1e-6, right), 0.0, 1.0)
    bottom = _clamp(max(top + 1e-6, bottom), 0.0, 1.0)
    return [left, top, right, bottom]


def _extract_square_bbox(box: Mapping[str, Any]) -> list[float] | None:
    value = box.get("square_crop_bbox")
    if isinstance(value, list) and len(value) >= 4:
        try:
            left = _clamp(float(value[0]), 0.0, 1.0)
            top = _clamp(float(value[1]), 0.0, 1.0)
            right = _clamp(float(value[2]), 0.0, 1.0)
            bottom = _clamp(float(value[3]), 0.0, 1.0)
        except (TypeError, ValueError):
            return None
        if right <= left or bottom <= top:
            return None
        return [left, top, right, bottom]
    return _normalize_square_bbox_from_face_box(box)


def _face_crop_signature(box: Mapping[str, Any], square_bbox: list[float], size: int) -> str:
    payload = {
        "index": int(box.get("index") or 0),
        "bbox": [round(v, 6) for v in square_bbox],
        "size": int(size),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _face_crop_key(entity_kind: str, entity_id: str, signature: str, *, ext: str = "jpg") -> str:
    safe_kind = "".join(ch for ch in str(entity_kind).lower() if ch.isalnum() or ch in {"-", "_"}).strip() or "asset"
    safe_id = "".join(ch for ch in str(entity_id).lower() if ch.isalnum() or ch in {"-", "_"}).strip() or "unknown"
    return f"face-crops/{safe_kind}/{safe_id}/{signature}.{ext}"


def _download_image_bytes(image_url: str) -> bytes | None:
    try:
        response = requests.get(image_url, timeout=(5, 40), stream=True)
        response.raise_for_status()
        payload = response.content or b""
        return payload if payload else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to download face-crop source image %s: %s", image_url, exc)
        return None


def generate_and_upload_face_crops(
    *,
    entity_kind: str,
    entity_id: str,
    image_url: str,
    face_boxes: list[Mapping[str, Any]],
    size: int = 256,
) -> list[dict[str, Any]]:
    if not image_url or not face_boxes:
        return []

    image_bytes = _download_image_bytes(image_url)
    if not image_bytes:
        return []

    try:
        image = Image.open(io.BytesIO(image_bytes))
        image = ImageOps.exif_transpose(image).convert("RGB")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to decode face-crop source image %s: %s", image_url, exc)
        return []

    width, height = image.size
    if width <= 0 or height <= 0:
        return []

    try:
        bucket = get_s3_bucket()
        s3 = get_s3_client()
    except Exception as exc:  # noqa: BLE001
        logger.warning("S3 unavailable for face crop caching: %s", exc)
        return []

    out: list[dict[str, Any]] = []
    # Older Pillow exposes LANCZOS only at module level; cast keeps the fallback attribute access as-is.
    resample = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else cast(Any, Image).LANCZOS

    for box in sorted(face_boxes, key=lambda item: int(item.get("index") or 0)):
        square_bbox = _extract_square_bbox(box)
        if square_bbox is None:
            continue
        left = int(round(square_bbox[0] * width))
        top = int(round(square_bbox[1] * height))
        right = int(round(square_bbox[2] * width))
        bottom = int(round(square_bbox[3] * height))
        left = max(0, min(left, width - 1))
        top = max(0, min(top, height - 1))
        right = max(left + 1, min(right, width))
        bottom = max(top + 1, min(bottom, height))

        crop = image.crop((left, top, right, bottom)).resize((size, size), resample)
        encoded = io.BytesIO()
        crop.save(encoded, format="JPEG", quality=85, optimize=True, progressive=True)
        payload = encoded.getvalue()
        signature = _face_crop_signature(box, square_bbox, size)
        key = _face_crop_key(entity_kind, entity_id, signature)
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload,
                ContentType="image/jpeg",
                CacheControl="public, max-age=31536000, immutable",
            )
            url = build_hosted_url(key)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to upload face crop for %s/%s: %s", entity_kind, entity_id, exc)
            url = None

        out.append(
            {
                "index": int(box.get("index") or len(out) + 1),
                "x": round(square_bbox[0], 6),
                "y": round(square_bbox[1], 6),
                "width": round(max(0.0, square_bbox[2] - square_bbox[0]), 6),
                "height": round(max(0.0, square_bbox[3] - square_bbox[1]), 6),
                "variant_key": key if url else None,
                "variant_url": url,
                "size": int(size),
            }
        )
    return out
