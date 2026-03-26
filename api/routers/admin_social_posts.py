from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel

from api.auth import InternalAdminUser
from trr_backend.repositories import social_posts as social_posts_repo

router = APIRouter(prefix="/admin", tags=["admin-social-posts"])


class CreateSocialPostRequest(BaseModel):
    platform: str
    url: str
    trr_season_id: str | None = None
    title: str | None = None
    notes: str | None = None


class UpdateSocialPostRequest(BaseModel):
    platform: str | None = None
    url: str | None = None
    trr_season_id: str | None = None
    title: str | None = None
    notes: str | None = None


def _validate_uuid(value: str | None, field_name: str) -> None:
    import re

    if not value or not re.fullmatch(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", value, re.I):
        raise HTTPException(status_code=400, detail=f"{field_name} must be a valid UUID")


def _validate_url(value: str, *, required: bool, field_name: str = "url") -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        if required:
            raise HTTPException(status_code=400, detail=f"{field_name} is required and must be a string")
        return None
    try:
        from urllib.parse import urlparse

        parsed = urlparse(normalized)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"{field_name} must be a valid URL") from exc
    return normalized


def _actor_uid(admin: dict[str, Any], explicit_uid: str | None) -> str:
    normalized = str(explicit_uid or "").strip()
    if normalized:
        return normalized
    return str(admin.get("email") or admin.get("id") or "admin")


def _validate_season_belongs_to_show(trr_season_id: str, show_id: str, detail: str) -> None:
    season_show_id, _ = social_posts_repo.get_season_show_id(trr_season_id)
    if season_show_id != show_id:
        raise HTTPException(status_code=400, detail=detail)


@router.get("/shows/{show_id}/social-posts")
def list_social_posts_for_show(
    show_id: str,
    trr_season_id: str | None = Query(default=None),
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    _validate_uuid(show_id, "showId")
    if trr_season_id is not None:
        _validate_uuid(trr_season_id, "trr_season_id")
        _validate_season_belongs_to_show(trr_season_id, show_id, "trr_season_id must belong to the showId route")
    posts, _query_count = social_posts_repo.list_posts_for_show(show_id, trr_season_id=trr_season_id)
    return {"posts": posts}


@router.post("/shows/{show_id}/social-posts", status_code=201)
def create_social_post_for_show(
    show_id: str,
    body: CreateSocialPostRequest,
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    admin: InternalAdminUser = None,
) -> dict[str, Any]:
    _validate_uuid(show_id, "showId")
    url = _validate_url(body.url, required=True)
    if body.trr_season_id is not None:
        _validate_uuid(body.trr_season_id, "trr_season_id")
        _validate_season_belongs_to_show(body.trr_season_id, show_id, "trr_season_id must belong to the showId route")
    post, _query_count = social_posts_repo.create_post(
        trr_show_id=show_id,
        trr_season_id=body.trr_season_id,
        platform=body.platform,
        url=url or "",
        title=body.title,
        notes=body.notes,
        actor_uid=_actor_uid(admin or {}, x_trr_admin_user_uid),
    )
    return {"post": post}


@router.get("/social-posts/{post_id}")
def get_social_post(post_id: str, _: InternalAdminUser = None) -> dict[str, Any]:
    _validate_uuid(post_id, "postId")
    post, _query_count = social_posts_repo.get_post(post_id)
    if post is None:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"post": post}


@router.put("/social-posts/{post_id}")
def update_social_post(
    post_id: str,
    body: UpdateSocialPostRequest,
    _: InternalAdminUser = None,
) -> dict[str, Any]:
    _validate_uuid(post_id, "postId")
    existing_post, _query_count = social_posts_repo.get_post(post_id)
    if existing_post is None:
        raise HTTPException(status_code=404, detail="Post not found")

    if body.url is not None:
        if not isinstance(body.url, str):
            raise HTTPException(status_code=400, detail="url must be a string")
        _validate_url(body.url, required=False)

    season_value: Any = None
    if "trr_season_id" in body.model_fields_set:
        if body.trr_season_id is None:
            season_value = None
        else:
            _validate_uuid(body.trr_season_id, "trr_season_id")
            _validate_season_belongs_to_show(
                body.trr_season_id,
                str(existing_post.get("trr_show_id") or ""),
                "trr_season_id must belong to the post show",
            )
            season_value = body.trr_season_id

    post, _query_count = social_posts_repo.update_post(
        post_id=post_id,
        trr_season_id=season_value if "trr_season_id" in body.model_fields_set else None,
        platform=body.platform if "platform" in body.model_fields_set else None,
        url=body.url if "url" in body.model_fields_set else None,
        title=body.title if "title" in body.model_fields_set else None,
        notes=body.notes if "notes" in body.model_fields_set else None,
    )
    if post is None:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"post": post}


@router.delete("/social-posts/{post_id}")
def delete_social_post(post_id: str, _: InternalAdminUser = None) -> dict[str, bool]:
    _validate_uuid(post_id, "postId")
    deleted, _query_count = social_posts_repo.delete_post(post_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Post not found")
    return {"success": True}
