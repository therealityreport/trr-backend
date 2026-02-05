"""Repository helpers for media_links."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def list_person_links_by_asset_id(db: Any, media_asset_id: str) -> list[dict[str, Any]]:
    try:
        response = (
            db.schema("core")
            .table("media_links")
            .select("id, context")
            .eq("media_asset_id", media_asset_id)
            .eq("entity_type", "person")
            .eq("kind", "gallery")
            .execute()
        )
    except Exception as exc:
        logger.warning("Failed to query media_links for asset %s: %s", media_asset_id, exc)
        return []

    if hasattr(response, "error") and response.error:
        logger.warning("Failed to query media_links for asset %s: %s", media_asset_id, response.error)
        return []

    return response.data or []


def has_manual_people_tags(context: dict[str, Any] | None) -> bool:
    if not context:
        return False
    if str(context.get("people_count_source", "")).lower() == "manual":
        return True
    if context.get("people_names"):
        return True
    if context.get("people_ids"):
        return True
    return False


def has_people_count(context: dict[str, Any] | None) -> bool:
    if not context:
        return False
    return context.get("people_count") is not None


def update_person_links_context(
    db: Any,
    links: list[dict[str, Any]],
    update: dict[str, Any],
) -> None:
    if not links:
        return
    now = datetime.now(UTC).isoformat()
    for link in links:
        base_ctx = link.get("context") or {}
        merged = {**base_ctx, **update}
        payload = {"context": merged, "updated_at": now}
        try:
            response = db.schema("core").table("media_links").update(payload).eq("id", link["id"]).execute()
        except Exception as exc:
            logger.warning("Failed to update media_link %s: %s", link.get("id"), exc)
            continue
        if hasattr(response, "error") and response.error:
            logger.warning("Failed to update media_link %s: %s", link.get("id"), response.error)


def update_media_link_facebank_seed(
    db: Any,
    link_id: str,
    facebank_seed: bool,
) -> dict[str, Any]:
    payload = {"facebank_seed": bool(facebank_seed)}
    try:
        response = db.schema("core").table("media_links").update(payload).eq("id", link_id).execute()
    except Exception as exc:
        logger.warning("Failed to update media_link %s facebank_seed: %s", link_id, exc)
        raise

    if hasattr(response, "error") and response.error:
        logger.warning("Failed to update media_link %s facebank_seed: %s", link_id, response.error)
        raise RuntimeError("Database error updating facebank_seed")

    data = response.data or []
    if isinstance(data, list) and data:
        return data[0]
    raise RuntimeError("Media link update returned no data")
