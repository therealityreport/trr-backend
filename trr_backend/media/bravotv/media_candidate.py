from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any


def _clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _compact_dict(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if value is not None and value != "" and value != [] and value != {}
    }


def _normalize_nup_identifier(value: Any) -> str | None:
    stem = re.sub(r"\.[a-z0-9]+$", "", str(value or "").strip(), flags=re.IGNORECASE).upper()
    if not stem:
        return None
    match = re.match(r"^(NUP)_(\d+)_([0-9]+)", stem)
    if not match:
        return stem
    return f"{match.group(1)}_{match.group(2)}_{int(match.group(3))}"


def _nup_set(value: Any) -> str | None:
    normalized = _normalize_nup_identifier(value)
    parts = str(normalized or "").split("_")
    if len(parts) != 3 or parts[0] != "NUP":
        return None
    return f"{parts[0]}_{parts[1]}"


def _default_source_role(source: str) -> str:
    if source == "nbcumv":
        return "original"
    if source == "peacock":
        return "official_original"
    if source == "bravo":
        return "editorial_context"
    if source == "getty":
        return "reference_metadata"
    return "supplemental"


def _default_display_eligible(source: str, record: Mapping[str, Any]) -> bool:
    if source == "getty":
        return False
    if source == "nbcumv":
        return bool(_clean_str(record.get("source_url")) or _clean_str(record.get("hosted_url")))
    if source == "peacock":
        return bool(_clean_str(record.get("source_url")) or _clean_str(record.get("hosted_url")))
    if source == "bravo":
        return bool(_clean_str(record.get("source_url")) or _clean_str(record.get("hosted_url")))
    return False


def _build_bridge_keys(record: Mapping[str, Any]) -> dict[str, Any]:
    raw = record.get("raw") if isinstance(record.get("raw"), Mapping) else {}
    nup_filename = _clean_str(record.get("nup_filename")) or _clean_str(raw.get("lbx_filename"))
    return _compact_dict(
        {
            "nup_filename": nup_filename,
            "nup_set": _clean_str(record.get("nup_set")) or _nup_set(nup_filename),
            "getty_editorial_id": _clean_str(record.get("getty_editorial_id")),
            "lbx_id": _clean_str(raw.get("lbx_id"))
            or (_clean_str(record.get("source_id")) if record.get("source") == "nbcumv" else None),
            "bravo_gallery_item_id": _clean_str(raw.get("gallery_item_id")),
            "file_url": _clean_str(raw.get("file_url")) or _clean_str(record.get("source_url")),
            "source_page_url": _clean_str(record.get("source_page_url")),
        }
    )


@dataclass(frozen=True)
class MediaCandidate:
    source: str
    source_role: str
    source_asset_id: str | None = None
    source_url: str | None = None
    source_page_url: str | None = None
    acquisition_status: str = "candidate"
    display_eligible: bool = False
    media_type: str = "image"
    width: int | None = None
    height: int | None = None
    bytes: int | None = None
    content_type: str | None = None
    sha256: str | None = None
    caption: str | None = None
    alt_text: str | None = None
    credit: str | None = None
    photographer: str | None = None
    copyright: str | None = None
    air_date: str | None = None
    season_number: int | str | None = None
    episode_title: str | None = None
    people_names: list[str] = field(default_factory=list)
    bridge_keys: dict[str, Any] = field(default_factory=dict)
    confidence: str | None = None
    review_reasons: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def candidate_from_normalized_record(record: Mapping[str, Any]) -> MediaCandidate:
    source = str(record.get("source") or "unknown").strip().lower() or "unknown"
    raw = dict(record.get("raw")) if isinstance(record.get("raw"), Mapping) else {}
    review_reasons: list[str] = []
    if source == "getty":
        review_reasons.append("metadata_only")
    if not _clean_str(record.get("source_id")):
        review_reasons.append("missing_source_asset_id")
    if not _clean_str(record.get("source_url")):
        review_reasons.append("missing_source_url")

    return MediaCandidate(
        source=source,
        source_role=_default_source_role(source),
        source_asset_id=_clean_str(record.get("source_id")),
        source_url=_clean_str(record.get("source_url")),
        source_page_url=_clean_str(record.get("source_page_url")),
        display_eligible=_default_display_eligible(source, record),
        width=record.get("width") if isinstance(record.get("width"), int) else None,
        height=record.get("height") if isinstance(record.get("height"), int) else None,
        bytes=record.get("bytes") if isinstance(record.get("bytes"), int) else None,
        content_type=_clean_str(record.get("content_type")),
        sha256=_clean_str(record.get("sha256")),
        caption=_clean_str(record.get("caption")),
        alt_text=_clean_str(record.get("alt_text")),
        credit=_clean_str(record.get("credit")),
        photographer=_clean_str(record.get("photographer")),
        copyright=_clean_str(record.get("copyright")),
        air_date=_clean_str(record.get("air_date")),
        season_number=record.get("season_number"),
        episode_title=_clean_str(record.get("episode_title")),
        people_names=[str(value).strip() for value in (record.get("people_names") or []) if str(value).strip()],
        bridge_keys=_build_bridge_keys(record),
        confidence=_clean_str(record.get("match_confidence")) or _clean_str(record.get("confidence")),
        review_reasons=review_reasons,
        raw=raw,
    )
