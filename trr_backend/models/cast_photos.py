from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class CastPhotoUpsert:
    person_id: UUID
    source: str = "imdb"
    imdb_person_id: str | None = None  # Required for source='imdb', optional otherwise
    source_image_id: str | None = None  # Required for source='imdb', optional otherwise
    viewer_id: str | None = None
    mediaindex_url_path: str | None = None
    mediaviewer_url_path: str | None = None
    url: str | None = None
    url_path: str | None = None
    source_page_url: str | None = None
    image_url: str | None = None
    thumb_url: str | None = None
    file_name: str | None = None
    alt_text: str | None = None
    width: int | None = None
    height: int | None = None
    caption: str | None = None
    gallery_index: int | None = None
    gallery_total: int | None = None
    people_imdb_ids: list[str] | None = None
    people_names: list[str] | None = None
    title_imdb_ids: list[str] | None = None
    title_names: list[str] | None = None
    context_section: str | None = None
    context_type: str | None = None
    season: int | None = None
    position: int | None = None
    image_url_canonical: str | None = None
    fetched_at: datetime | None = None
    updated_at: datetime | None = None
    metadata: dict[str, Any] | None = None
