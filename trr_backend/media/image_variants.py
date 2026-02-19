"""Generate and persist optimized media asset variants (base + crop)."""

from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Any

import requests
from PIL import Image, ImageOps

from trr_backend.media.s3_mirror import build_hosted_url, get_s3_bucket, get_s3_client

logger = logging.getLogger(__name__)

_BASE_VARIANT_WIDTHS: tuple[tuple[str, int], ...] = (
    ("thumb", 320),
    ("card", 720),
    ("detail", 1440),
)

_CROP_VARIANTS: tuple[tuple[str, int, int], ...] = (
    ("crop_card", 720, 900),
    ("crop_detail", 1440, 1800),
)


@dataclass(slots=True)
class CropSpec:
    mode: str
    x: float
    y: float
    zoom: float


@dataclass(slots=True)
class VariantResult:
    variant_key: str
    format: str
    hosted_url: str
    width: int
    height: int
    bytes: int
    crop_signature: str


def _resample_filter() -> int:
    if hasattr(Image, "Resampling"):
        return Image.Resampling.LANCZOS  # type: ignore[attr-defined]
    return Image.LANCZOS  # type: ignore[attr-defined]


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalize_crop(crop: dict[str, Any] | None) -> CropSpec | None:
    if not isinstance(crop, dict):
        return None
    try:
        x = float(crop.get("x"))
        y = float(crop.get("y"))
        zoom = float(crop.get("zoom"))
    except (TypeError, ValueError):
        return None

    mode_raw = crop.get("mode")
    mode = str(mode_raw).strip().lower() if isinstance(mode_raw, str) else "auto"
    if mode not in {"auto", "manual"}:
        mode = "auto"

    return CropSpec(
        mode=mode,
        x=_clamp(x, 0.0, 100.0),
        y=_clamp(y, 0.0, 100.0),
        zoom=_clamp(zoom, 1.0, 4.0),
    )


def _crop_signature(crop: CropSpec | None) -> str:
    if crop is None:
        return "base"
    return f"{crop.mode}:{crop.x:.3f}:{crop.y:.3f}:{crop.zoom:.3f}"


def _variant_key_for(asset_id: str, crop_signature: str, variant_key: str, ext: str) -> str:
    signature_hash = hashlib.sha1(crop_signature.encode("utf-8")).hexdigest()[:12]
    return f"media-variants/{asset_id}/{signature_hash}/{variant_key}.{ext}"


def _cast_variant_key_for(photo_id: str, crop_signature: str, variant_key: str, ext: str) -> str:
    signature_hash = hashlib.sha1(crop_signature.encode("utf-8")).hexdigest()[:12]
    return f"cast-photo-variants/{photo_id}/{signature_hash}/{variant_key}.{ext}"


def _load_image_bytes(asset_row: dict[str, Any]) -> bytes:
    hosted_bucket = asset_row.get("hosted_bucket")
    hosted_key = asset_row.get("hosted_key")
    if isinstance(hosted_bucket, str) and hosted_bucket and isinstance(hosted_key, str) and hosted_key:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=hosted_bucket, Key=hosted_key)
        return response["Body"].read()

    hosted_url = asset_row.get("hosted_url")
    if isinstance(hosted_url, str) and hosted_url:
        resp = requests.get(hosted_url, timeout=45)
        resp.raise_for_status()
        return resp.content

    raise RuntimeError("Asset has no hosted source to generate variants")


def _load_cast_photo_image_bytes(photo_row: dict[str, Any]) -> bytes:
    hosted_bucket = photo_row.get("hosted_bucket")
    hosted_key = photo_row.get("hosted_key")
    if isinstance(hosted_bucket, str) and hosted_bucket and isinstance(hosted_key, str) and hosted_key:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=hosted_bucket, Key=hosted_key)
        return response["Body"].read()

    hosted_url = photo_row.get("hosted_url")
    if isinstance(hosted_url, str) and hosted_url:
        resp = requests.get(hosted_url, timeout=45)
        resp.raise_for_status()
        return resp.content

    raise RuntimeError("Cast photo has no hosted source to generate variants")


def _resize_to_width(image: Image.Image, width: int) -> Image.Image:
    if image.width <= width:
        return image.copy()
    ratio = width / float(image.width)
    height = max(1, int(round(image.height * ratio)))
    return image.resize((width, height), _resample_filter())


def _focus_crop(image: Image.Image, crop: CropSpec, out_width: int, out_height: int) -> Image.Image:
    src_w = float(image.width)
    src_h = float(image.height)
    out_aspect = out_width / float(out_height)

    if src_w / src_h >= out_aspect:
        base_h = src_h
        base_w = src_h * out_aspect
    else:
        base_w = src_w
        base_h = src_w / out_aspect

    crop_w = max(1.0, base_w / crop.zoom)
    crop_h = max(1.0, base_h / crop.zoom)

    cx = (crop.x / 100.0) * src_w
    cy = (crop.y / 100.0) * src_h

    left = _clamp(cx - crop_w / 2.0, 0.0, src_w - crop_w)
    top = _clamp(cy - crop_h / 2.0, 0.0, src_h - crop_h)
    right = left + crop_w
    bottom = top + crop_h

    cropped = image.crop((int(round(left)), int(round(top)), int(round(right)), int(round(bottom))))
    return cropped.resize((out_width, out_height), _resample_filter())


def _encode_image(image: Image.Image, fmt: str) -> tuple[bytes, str, str]:
    output = io.BytesIO()
    if fmt == "jpg":
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        image.save(output, format="JPEG", quality=80, optimize=True, progressive=True)
        return output.getvalue(), "image/jpeg", "jpg"

    if fmt == "webp":
        if image.mode not in ("RGB", "RGBA", "L"):
            image = image.convert("RGB")
        image.save(output, format="WEBP", quality=78, method=6)
        return output.getvalue(), "image/webp", "webp"

    raise ValueError(f"Unsupported format: {fmt}")


def _get_asset_row(db, asset_id: str) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("media_assets")
        .select("id, hosted_url, hosted_bucket, hosted_key, metadata")
        .eq("id", asset_id)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError("Database error fetching media asset")
    if not response.data:
        raise RuntimeError("Media asset not found")
    return response.data[0]


def _get_cast_photo_row(db, photo_id: str) -> dict[str, Any]:
    response = (
        db.schema("core")
        .table("cast_photos")
        .select("id, hosted_url, hosted_bucket, hosted_key, metadata")
        .eq("id", photo_id)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError("Database error fetching cast photo")
    if not response.data:
        raise RuntimeError("Cast photo not found")
    return response.data[0]


def _existing_variants(db, asset_id: str, crop_signature: str) -> dict[tuple[str, str], dict[str, Any]]:
    response = (
        db.schema("core")
        .table("media_asset_variants")
        .select("id, variant_key, format, hosted_url, width, height, bytes")
        .eq("media_asset_id", asset_id)
        .eq("crop_signature", crop_signature)
        .limit(200)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        return {}
    rows = response.data or []
    return {
        (str(row.get("variant_key")), str(row.get("format"))): row
        for row in rows
        if row.get("variant_key") and row.get("format")
    }


def _upsert_variant_row(
    db,
    *,
    asset_id: str,
    variant_key: str,
    fmt: str,
    width: int,
    height: int,
    bytes_len: int,
    hosted_bucket: str,
    hosted_key: str,
    hosted_url: str,
    crop: CropSpec | None,
    crop_signature: str,
) -> None:
    row: dict[str, Any] = {
        "media_asset_id": asset_id,
        "variant_key": variant_key,
        "format": fmt,
        "width": width,
        "height": height,
        "bytes": bytes_len,
        "hosted_bucket": hosted_bucket,
        "hosted_key": hosted_key,
        "hosted_url": hosted_url,
        "crop_signature": crop_signature,
    }
    if crop is not None:
        row.update(
            {
                "crop_mode": crop.mode,
                "crop_x": crop.x,
                "crop_y": crop.y,
                "crop_zoom": crop.zoom,
            }
        )

    (
        db.schema("core")
        .table("media_asset_variants")
        .upsert(row, on_conflict="media_asset_id,variant_key,format,crop_signature")
        .execute()
    )


def _update_asset_variant_metadata(db, asset_id: str, variants: list[VariantResult], crop_signature: str) -> None:
    if not variants:
        return

    asset_row = _get_asset_row(db, asset_id)
    metadata = dict(asset_row.get("metadata") or {})
    variants_meta = dict(metadata.get("variants") or {})

    signature_bucket = dict(variants_meta.get(crop_signature) or {})
    for variant in variants:
        slot = dict(signature_bucket.get(variant.variant_key) or {})
        slot[variant.format] = {
            "url": variant.hosted_url,
            "width": variant.width,
            "height": variant.height,
            "bytes": variant.bytes,
        }
        signature_bucket[variant.variant_key] = slot

    variants_meta[crop_signature] = signature_bucket
    metadata["variants"] = variants_meta

    def _best_url(variant_key: str) -> str | None:
        data = signature_bucket.get(variant_key)
        if not isinstance(data, dict):
            return None
        webp = data.get("webp")
        jpg = data.get("jpg")
        if isinstance(webp, dict) and isinstance(webp.get("url"), str):
            return webp["url"]
        if isinstance(jpg, dict) and isinstance(jpg.get("url"), str):
            return jpg["url"]
        return None

    if crop_signature == "base":
        metadata["thumb_url"] = _best_url("thumb")
        metadata["display_url"] = _best_url("card")
        metadata["detail_url"] = _best_url("detail")
    else:
        metadata["crop_display_url"] = _best_url("crop_card")
        metadata["crop_detail_url"] = _best_url("crop_detail")
        metadata["active_crop_signature"] = crop_signature

    (
        db.schema("core")
        .table("media_assets")
        .update({"metadata": metadata})
        .eq("id", asset_id)
        .execute()
    )


def _existing_cast_metadata_variants(
    metadata: dict[str, Any],
    crop_signature: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    variants = metadata.get("variants")
    if not isinstance(variants, dict):
        return {}
    signature_bucket = variants.get(crop_signature)
    if not isinstance(signature_bucket, dict):
        return {}

    existing: dict[tuple[str, str], dict[str, Any]] = {}
    for variant_key, by_format in signature_bucket.items():
        if not isinstance(variant_key, str) or not isinstance(by_format, dict):
            continue
        for fmt, payload in by_format.items():
            if not isinstance(fmt, str) or not isinstance(payload, dict):
                continue
            if isinstance(payload.get("url"), str):
                existing[(variant_key, fmt)] = payload
    return existing


def _update_cast_photo_variant_metadata(
    db,
    photo_id: str,
    metadata: dict[str, Any],
    variants: list[VariantResult],
    crop_signature: str,
) -> None:
    if not variants:
        return

    variants_meta = dict(metadata.get("variants") or {})
    signature_bucket = dict(variants_meta.get(crop_signature) or {})
    for variant in variants:
        slot = dict(signature_bucket.get(variant.variant_key) or {})
        slot[variant.format] = {
            "url": variant.hosted_url,
            "width": variant.width,
            "height": variant.height,
            "bytes": variant.bytes,
        }
        signature_bucket[variant.variant_key] = slot

    variants_meta[crop_signature] = signature_bucket
    metadata["variants"] = variants_meta

    def _best_url(variant_key: str) -> str | None:
        data = signature_bucket.get(variant_key)
        if not isinstance(data, dict):
            return None
        webp = data.get("webp")
        jpg = data.get("jpg")
        if isinstance(webp, dict) and isinstance(webp.get("url"), str):
            return webp["url"]
        if isinstance(jpg, dict) and isinstance(jpg.get("url"), str):
            return jpg["url"]
        return None

    if crop_signature == "base":
        metadata["thumb_url"] = _best_url("thumb")
        metadata["display_url"] = _best_url("card")
        metadata["detail_url"] = _best_url("detail")
    else:
        metadata["crop_display_url"] = _best_url("crop_card")
        metadata["crop_detail_url"] = _best_url("crop_detail")
        metadata["active_crop_signature"] = crop_signature

    (
        db.schema("core")
        .table("cast_photos")
        .update({"metadata": metadata})
        .eq("id", photo_id)
        .execute()
    )


def generate_media_asset_variants(
    db,
    *,
    asset_id: str,
    crop: dict[str, Any] | None = None,
    force: bool = False,
) -> list[VariantResult]:
    """Generate base variants (or crop variants when crop is provided) for a media asset."""
    asset = _get_asset_row(db, asset_id)
    image_bytes = _load_image_bytes(asset)

    image = Image.open(io.BytesIO(image_bytes))
    image = ImageOps.exif_transpose(image)

    crop_spec = _normalize_crop(crop)
    crop_signature = _crop_signature(crop_spec)
    existing = _existing_variants(db, asset_id, crop_signature)

    bucket = get_s3_bucket()
    s3_client = get_s3_client()

    created: list[VariantResult] = []

    if crop_spec is None:
        variant_specs: list[tuple[str, Image.Image]] = [
            (variant_key, _resize_to_width(image, width)) for variant_key, width in _BASE_VARIANT_WIDTHS
        ]
    else:
        variant_specs = [
            (variant_key, _focus_crop(image, crop_spec, out_w, out_h))
            for variant_key, out_w, out_h in _CROP_VARIANTS
        ]

    for variant_key, variant_image in variant_specs:
        for fmt in ("webp", "jpg"):
            if not force and (variant_key, fmt) in existing:
                row = existing[(variant_key, fmt)]
                created.append(
                    VariantResult(
                        variant_key=variant_key,
                        format=fmt,
                        hosted_url=str(row.get("hosted_url") or ""),
                        width=int(row.get("width") or variant_image.width),
                        height=int(row.get("height") or variant_image.height),
                        bytes=int(row.get("bytes") or 0),
                        crop_signature=crop_signature,
                    )
                )
                continue

            encoded, content_type, ext = _encode_image(variant_image, fmt)
            hosted_key = _variant_key_for(asset_id, crop_signature, variant_key, ext)
            s3_client.put_object(
                Bucket=bucket,
                Key=hosted_key,
                Body=encoded,
                ContentType=content_type,
                CacheControl="public, max-age=31536000, immutable",
            )
            hosted_url = build_hosted_url(hosted_key)
            _upsert_variant_row(
                db,
                asset_id=asset_id,
                variant_key=variant_key,
                fmt=fmt,
                width=variant_image.width,
                height=variant_image.height,
                bytes_len=len(encoded),
                hosted_bucket=bucket,
                hosted_key=hosted_key,
                hosted_url=hosted_url,
                crop=crop_spec,
                crop_signature=crop_signature,
            )
            created.append(
                VariantResult(
                    variant_key=variant_key,
                    format=fmt,
                    hosted_url=hosted_url,
                    width=variant_image.width,
                    height=variant_image.height,
                    bytes=len(encoded),
                    crop_signature=crop_signature,
                )
            )

    _update_asset_variant_metadata(db, asset_id, created, crop_signature)
    return created


def generate_cast_photo_variants(
    db,
    *,
    photo_id: str,
    crop: dict[str, Any] | None = None,
    force: bool = False,
) -> list[VariantResult]:
    """Generate base variants (or crop variants when crop is provided) for a cast photo."""
    photo = _get_cast_photo_row(db, photo_id)
    image_bytes = _load_cast_photo_image_bytes(photo)

    image = Image.open(io.BytesIO(image_bytes))
    image = ImageOps.exif_transpose(image)

    metadata = dict(photo.get("metadata") or {})
    crop_spec = _normalize_crop(crop)
    crop_signature = _crop_signature(crop_spec)
    existing = _existing_cast_metadata_variants(metadata, crop_signature)

    bucket = get_s3_bucket()
    s3_client = get_s3_client()

    created: list[VariantResult] = []

    if crop_spec is None:
        variant_specs: list[tuple[str, Image.Image]] = [
            (variant_key, _resize_to_width(image, width)) for variant_key, width in _BASE_VARIANT_WIDTHS
        ]
    else:
        variant_specs = [
            (variant_key, _focus_crop(image, crop_spec, out_w, out_h))
            for variant_key, out_w, out_h in _CROP_VARIANTS
        ]

    for variant_key, variant_image in variant_specs:
        for fmt in ("webp", "jpg"):
            if not force and (variant_key, fmt) in existing:
                row = existing[(variant_key, fmt)]
                created.append(
                    VariantResult(
                        variant_key=variant_key,
                        format=fmt,
                        hosted_url=str(row.get("url") or ""),
                        width=int(row.get("width") or variant_image.width),
                        height=int(row.get("height") or variant_image.height),
                        bytes=int(row.get("bytes") or 0),
                        crop_signature=crop_signature,
                    )
                )
                continue

            encoded, content_type, ext = _encode_image(variant_image, fmt)
            hosted_key = _cast_variant_key_for(photo_id, crop_signature, variant_key, ext)
            s3_client.put_object(
                Bucket=bucket,
                Key=hosted_key,
                Body=encoded,
                ContentType=content_type,
                CacheControl="public, max-age=31536000, immutable",
            )
            hosted_url = build_hosted_url(hosted_key)
            created.append(
                VariantResult(
                    variant_key=variant_key,
                    format=fmt,
                    hosted_url=hosted_url,
                    width=variant_image.width,
                    height=variant_image.height,
                    bytes=len(encoded),
                    crop_signature=crop_signature,
                )
            )

    _update_cast_photo_variant_metadata(db, photo_id, metadata, created, crop_signature)
    return created
