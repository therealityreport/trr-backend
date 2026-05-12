"""Generate social-post display image variants for hosted cover assets."""

from __future__ import annotations

import hashlib
import io
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from PIL import Image, ImageOps

_BASE_VARIANT_WIDTHS: tuple[tuple[str, int], ...] = (
    ("thumb", 320),
    ("card", 720),
    ("detail", 1440),
)

_POSTER_VARIANTS: tuple[tuple[str, int, int], ...] = (
    ("poster_card", 720, 960),
    ("poster_detail", 1440, 1920),
)


@dataclass(slots=True)
class SocialDisplayVariant:
    variant_key: str
    format: str
    url: str
    content_type: str
    width: int
    height: int
    bytes: int
    storage_key: str


def _resample_filter() -> int:
    if hasattr(Image, "Resampling"):
        return Image.Resampling.LANCZOS  # type: ignore[attr-defined]
    return Image.LANCZOS  # type: ignore[attr-defined]


def _resize_to_width(image: Image.Image, width: int) -> Image.Image:
    if image.width <= width:
        return image.copy()
    ratio = width / float(image.width)
    height = max(1, int(round(image.height * ratio)))
    return image.resize((width, height), _resample_filter())


def _center_crop(image: Image.Image, out_width: int, out_height: int) -> Image.Image:
    src_w = float(image.width)
    src_h = float(image.height)
    out_aspect = out_width / float(out_height)
    if src_w / src_h >= out_aspect:
        crop_h = src_h
        crop_w = src_h * out_aspect
    else:
        crop_w = src_w
        crop_h = src_w / out_aspect
    left = max(0, int(round((src_w - crop_w) / 2.0)))
    top = max(0, int(round((src_h - crop_h) / 2.0)))
    right = min(image.width, int(round(left + crop_w)))
    bottom = min(image.height, int(round(top + crop_h)))
    return image.crop((left, top, right, bottom)).resize((out_width, out_height), _resample_filter())


def _encode_image(image: Image.Image, fmt: str) -> tuple[bytes, str, str]:
    output = io.BytesIO()
    if fmt == "jpeg":
        if image.mode not in ("RGB", "L"):
            image = image.convert("RGB")
        image.save(output, format="JPEG", quality=80, optimize=True, progressive=True)
        return output.getvalue(), "image/jpeg", "jpg"
    if fmt == "webp":
        if image.mode not in ("RGB", "RGBA", "L"):
            image = image.convert("RGB")
        image.save(output, format="WEBP", quality=78, method=6)
        return output.getvalue(), "image/webp", "webp"
    raise ValueError(f"unsupported_social_variant_format:{fmt}")


def _safe_key_part(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in str(value or "").strip())
    return cleaned.strip("-")[:160] or "unknown"


def generate_social_cover_display_variants(
    *,
    image_path: str,
    s3_client: Any,
    bucket: str,
    key_prefix: str,
    build_hosted_url: Callable[[str], str],
) -> list[SocialDisplayVariant]:
    """Create social display variants from a local cover image and upload them."""
    with open(image_path, "rb") as source_file:
        image_bytes = source_file.read()
    cover_hash = hashlib.sha1(image_bytes).hexdigest()[:16]

    image = Image.open(io.BytesIO(image_bytes))
    image = ImageOps.exif_transpose(image)

    prefix = str(key_prefix or "").strip("/")
    safe_hash = _safe_key_part(cover_hash)
    uploaded_keys: list[str] = []
    results: list[SocialDisplayVariant] = []

    try:
        variant_images: list[tuple[str, Image.Image]] = [
            (variant_key, _resize_to_width(image, width)) for variant_key, width in _BASE_VARIANT_WIDTHS
        ]
        variant_images.extend(
            (variant_key, _center_crop(image, width, height)) for variant_key, width, height in _POSTER_VARIANTS
        )

        for base_key, variant_image in variant_images:
            for fmt in ("webp", "jpeg"):
                encoded, content_type, ext = _encode_image(variant_image, fmt)
                manifest_key = f"{base_key}_{fmt}"
                storage_key = (
                    f"{prefix}/{safe_hash}/{manifest_key}.{ext}" if prefix else f"{safe_hash}/{manifest_key}.{ext}"
                )
                s3_client.put_object(
                    Bucket=bucket,
                    Key=storage_key,
                    Body=encoded,
                    ContentType=content_type,
                    CacheControl="public, max-age=31536000, immutable",
                )
                uploaded_keys.append(storage_key)
                results.append(
                    SocialDisplayVariant(
                        variant_key=manifest_key,
                        format=fmt,
                        url=build_hosted_url(storage_key),
                        content_type=content_type,
                        width=variant_image.width,
                        height=variant_image.height,
                        bytes=len(encoded),
                        storage_key=storage_key,
                    )
                )
    except Exception:
        for key in uploaded_keys:
            try:
                s3_client.delete_object(Bucket=bucket, Key=key)
            except Exception:  # noqa: BLE001
                pass
        raise

    return results
