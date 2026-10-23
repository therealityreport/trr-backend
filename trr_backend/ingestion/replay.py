"""Offline replay parsing and comparison; source/storage clients are absent."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import TypeAdapter

from trr_backend.ingestion.canonical import normalize_post_source
from trr_backend.ingestion.comments import COMMENT_PARSER_VERSION, CommentPage, parse_comment_evidence
from trr_backend.ingestion.domain import Stage, utc_timestamp
from trr_backend.ingestion.evidence import Evidence, sanitize_payload
from trr_backend.ingestion.listing import ROOT_FIELD, is_anonymous_listing, parse_listing_page
from trr_backend.ingestion.providers import PARSER_VERSION, InvalidSourceError, ParsedSource, parse_evidence
from trr_backend.ingestion.queries import Queries
from trr_backend.socials.instagram.graphql_response import PROVENANCE_KEY, classify_graphql_response

if TYPE_CHECKING:
    from trr_backend.ingestion.media import MediaResult

_JSON = TypeAdapter(Any)
# Immutable parser contract for the source-inventory envelope retained by the
# media activity. No import of its download/storage implementation is required.
MEDIA_REPLAY_VERSION = "instagram-media-v1"
MEDIA_RESULT_REPLAY_VERSION = "instagram-media-result-v1"
MEDIA_APPLY_LIMITATION = (
    "Retained media evidence contains source inventory, not hosted-result URLs, verified content hashes "
    "or generated display-variant results. Offline apply cannot reconstruct canonical hosted results."
)


@dataclass(frozen=True)
class ListingReplay:
    observed_at: datetime
    posts: tuple[ParsedSource, ...]
    has_next_page: bool | None
    end_cursor: str | None


@dataclass(frozen=True)
class MediaReplay:
    observed_at: datetime
    inventory: dict[str, Any]
    source_unavailable: str | None = None
    result: MediaResult | None = dataclass_field(default=None, repr=False)


ReplaySource = ParsedSource | CommentPage | ListingReplay | MediaReplay


def _object(value: Any, *, required: str, optional: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidSourceError("Media result requires a typed object")
    missing = set(required.split()) - value.keys()
    extra = value.keys() - set((required + " " + optional).split())
    if missing or extra:
        raise InvalidSourceError("Media result fields differ from the retained canonical contract")
    return dict(value)


def _public_reference(value: Any, *, source: bool = False) -> str:
    if not isinstance(value, str) or not value:
        raise InvalidSourceError("Media result URL is missing")
    try:
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.fragment
            or (parts.query and not source)
            or parts.hostname in {"localhost", "metadata.google.internal"}
            or parts.hostname.endswith((".localhost", ".local", ".internal"))
        ):
            raise ValueError("Not a public retained reference")
        try:
            address = ipaddress.ip_address(parts.hostname)
        except ValueError:
            address = None
        if address is not None and not address.is_global:
            raise ValueError("Not a public address")
    except ValueError:
        raise InvalidSourceError("Media result URL is not a public retained reference") from None
    return value


def _positive(value: Any, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if type(value) is not int or value <= 0:
        raise InvalidSourceError("Media result dimensions or byte count is invalid")


def _hash(value: Any) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise InvalidSourceError("Media result verified content hash is invalid")


def _asset(value: Any, *, observed_at: datetime, variant: bool = False) -> dict[str, Any]:
    required = "url format content_type width height bytes storage_key source_role variant_key generated_at"
    item = _object(value, required=required if variant else required + " sha256", optional="sha256" if variant else "")
    _public_reference(item["url"])
    if (
        not isinstance(item["storage_key"], str)
        or not re.fullmatch(r"[A-Za-z0-9_./-]{1,1024}", item["storage_key"])
        or ".." in item["storage_key"].split("/")
    ):
        raise InvalidSourceError("Media result storage key is invalid")
    _positive(item["bytes"])
    _positive(item["width"], nullable=not variant)
    _positive(item["height"], nullable=not variant)
    if not isinstance(item["format"], str) or not item["format"] or not isinstance(item["content_type"], str):
        raise InvalidSourceError("Media result content descriptor is invalid")
    if _timestamp(item["generated_at"]) != observed_at:
        raise InvalidSourceError("Media result generated time differs from its original observation")
    if "sha256" in item:
        _hash(item["sha256"])
    return item


def serialize_media_result(result: MediaResult) -> dict[str, Any]:
    """Seal known successful mirror outputs, never transport/session objects.

    Originals and avatars retain the hashes returned by the verified immutable
    storage path. Variants/comments retain exactly the available output fields;
    this serializer never invents missing per-object hashes or a new source time.
    """
    from trr_backend.ingestion.domain import Outcome

    if result.outcome != Outcome.COMMITTED:
        raise InvalidSourceError("Only committed media results can produce offline apply evidence")
    snapshot = result.snapshot
    snapshot.validate()
    post = dict(result.post_fields)
    manifest = dict(post.pop("asset_manifest"))
    tagged = post.pop("hosted_tagged_profile_pics", {})
    manifest["display_variants"] = [dict(value) for _, value in sorted(manifest["display_variants"].items())]
    stamp = utc_timestamp(snapshot.observed_at)
    payload = {
        "shortcode": snapshot.shortcode,
        "ingestion_media": {"version": MEDIA_REPLAY_VERSION, "observed_at": stamp},
        "post_id": snapshot.post_id,
        "canonical_post_id": snapshot.canonical_post_id,
        "show_id": snapshot.show_id,
        "season_number": snapshot.season_number,
        "week_index": snapshot.week_index,
        "media_urls": list(snapshot.media_urls),
        "thumbnail_url": snapshot.thumbnail_url,
        "expected_media_count": snapshot.expected_media_count,
        "comments_complete": snapshot.comments_complete,
        "avatars_complete": snapshot.avatars_complete,
        "unavailable_avatars": list(snapshot.unavailable_avatars),
        "include_comment_avatars": snapshot.include_comment_avatars,
        "media": [{"id": role, "storage_source_key": key} for role, key in sorted(snapshot.storage_keys().items())],
        "comments": [
            {
                "comment_id": item.comment_id,
                "row_id": item.row_id,
                "media_urls": list(item.media_urls),
                "storage_source_key": item.storage_source_key,
                "observed_at": utc_timestamp(item.observed_at or snapshot.observed_at),
                **({"attachment_refresh": item.attachment_refresh.to_payload()} if item.attachment_refresh else {}),
                **({"attachment_version": item.attachment_version} if item.attachment_version else {}),
            }
            for item in snapshot.comments
        ],
        "avatars": [
            {
                "username": item.username,
                "profile_pic_url": item.source_url,
                "role": item.role,
                "comment_id": item.comment_id,
                "observed_at": utc_timestamp(item.observed_at or snapshot.observed_at),
                "hosted_url": item.cached_hosted_url,
                "content_hash": item.cached_sha256,
            }
            for item in snapshot.avatars
        ],
        "media_result": {
            "version": 1,
            "outcome": "committed",
            "post_fields": post,
            "asset_manifest": manifest,
            "comment_results": [{"comment_id": key, **value} for key, value in sorted(result.comment_fields.items())],
            "avatars": [dict(item) for item in result.avatars],
            "tagged_profiles": [{"username": key, "hosted_url": value} for key, value in sorted(tagged.items())],
        },
    }
    _decode_media_result(payload)
    payload = sanitize_payload(payload)
    _decode_media_result(payload)
    return payload


def _decode_media_result(payload: dict[str, Any]) -> MediaResult:
    from trr_backend.ingestion.media import AvatarSource, CommentMedia, MediaResult, MediaSnapshot, source_reference

    try:
        _object(
            payload,
            required=(
                "shortcode ingestion_media post_id canonical_post_id show_id season_number week_index media_urls "
                "thumbnail_url expected_media_count comments_complete avatars_complete unavailable_avatars "
                "include_comment_avatars media comments avatars media_result"
            ),
        )
        marker = _object(payload["ingestion_media"], required="version observed_at")
        if marker["version"] != MEDIA_REPLAY_VERSION:
            raise InvalidSourceError("Unsupported media inventory version")
        for name in ("media_urls", "media", "comments", "avatars", "unavailable_avatars"):
            if not isinstance(payload[name], list):
                raise InvalidSourceError("Media result inventory requires explicit lists")
        for name in ("comments_complete", "avatars_complete", "include_comment_avatars"):
            if type(payload[name]) is not bool:
                raise InvalidSourceError("Media result coverage requires explicit booleans")
        keys = {}
        for item in payload["media"]:
            _object(item, required="id storage_source_key")
            if item["id"] in keys:
                raise InvalidSourceError("Duplicate preserved media role")
            keys[item["id"]] = item["storage_source_key"]
        source = MediaSnapshot(
            observed_at=_timestamp(payload["ingestion_media"]["observed_at"]),
            post_id=payload["post_id"],
            canonical_post_id=payload["canonical_post_id"],
            shortcode=payload["shortcode"],
            show_id=payload["show_id"],
            season_number=payload["season_number"],
            week_index=payload["week_index"],
            media_urls=tuple(payload["media_urls"]),
            thumbnail_url=payload["thumbnail_url"],
            expected_media_count=payload["expected_media_count"],
            comments_complete=payload["comments_complete"],
            avatars_complete=payload["avatars_complete"],
            include_comment_avatars=payload["include_comment_avatars"],
            unavailable_avatars=tuple(payload["unavailable_avatars"]),
            preserved_storage_keys=keys,
            comments=tuple(
                CommentMedia(
                    item["comment_id"],
                    item["row_id"],
                    tuple(item["media_urls"]),
                    item["storage_source_key"],
                    _timestamp(item["observed_at"]),
                    _attachment_refresh(item.get("attachment_refresh")),
                    item.get("attachment_version"),
                )
                for item in payload["comments"]
                if _object(
                    item,
                    required="comment_id row_id media_urls storage_source_key observed_at",
                    optional="attachment_refresh attachment_version",
                )
            ),
            avatars=tuple(
                AvatarSource(
                    item["username"],
                    item["profile_pic_url"],
                    item["role"],
                    item["comment_id"],
                    _timestamp(item["observed_at"]),
                    item.get("hosted_url"),
                    item.get("content_hash"),
                )
                for item in payload["avatars"]
                if _object(
                    item,
                    required="username profile_pic_url role comment_id observed_at",
                    optional="hosted_url content_hash",
                )
            ),
        )
        source.validate()
        for url in (
            *source.media_urls,
            source.thumbnail_url,
            *(url for item in source.comments for url in item.media_urls),
            *(item.source_url for item in source.avatars),
        ):
            _public_reference(url, source=True)
        result = _object(
            payload["media_result"],
            required="version outcome post_fields asset_manifest comment_results avatars tagged_profiles",
        )
        if type(result["version"]) is not int or result["version"] != 1 or result["outcome"] != "committed":
            raise InvalidSourceError("Unsupported media result outcome or version")
        post = _object(
            result["post_fields"],
            required=(
                "hosted_thumbnail_url hosted_media_urls media_mirror_status media_mirror_error "
                "profile_pic_mirror_status profile_pic_mirror_error"
            ),
            optional="hosted_owner_profile_pic_url",
        )
        if (
            post["media_mirror_status"] != "mirrored"
            or post["media_mirror_error"] is not None
            or post["profile_pic_mirror_error"] is not None
        ):
            raise InvalidSourceError("Retained media result is incomplete")
        if post["profile_pic_mirror_status"] != (
            "unsupported" if source.unavailable_avatars and not source.avatars else "mirrored"
        ):
            raise InvalidSourceError("Media avatar coverage differs from its source inventory")
        _references(post["hosted_media_urls"])
        if len(post["hosted_media_urls"]) != source.expected_media_count:
            raise InvalidSourceError("Hosted media count differs from its source inventory")
        for url in [post["hosted_thumbnail_url"], *post["hosted_media_urls"]]:
            _public_reference(url)
        manifest = _object(
            result["asset_manifest"],
            required=(
                "selection_policy originals display_variants display_variants_status display_variants_reason "
                "updated_at ingestion_media storage_summary"
            ),
        )
        if (
            manifest["selection_policy"] != "best_per_role_with_cover_display_variants"
            or manifest["display_variants_status"] != "ready"
            or manifest["display_variants_reason"] is not None
        ):
            raise InvalidSourceError("Media display variants are not complete")
        if (
            _timestamp(manifest["updated_at"]) != source.observed_at
            or manifest["ingestion_media"] != payload["ingestion_media"]
        ):
            raise InvalidSourceError("Media manifest source observation differs")
        originals = _object(manifest["originals"], required="cover media")
        cover = _asset(originals["cover"], observed_at=source.observed_at)
        if (
            cover["source_role"] != "cover"
            or cover["variant_key"] != "cover_original"
            or cover["url"] != post["hosted_thumbnail_url"]
            or not cover["content_type"].startswith("image/")
        ):
            raise InvalidSourceError("Media cover result identity differs")
        if not isinstance(originals["media"], list) or len(originals["media"]) != source.expected_media_count:
            raise InvalidSourceError("Media original result coverage differs")
        assets = [cover]
        for index, raw in enumerate(originals["media"], 1):
            item = _asset(raw, observed_at=source.observed_at)
            if (
                item["source_role"] != "media"
                or item["variant_key"] != f"media_original_{index:02d}"
                or item["url"] != post["hosted_media_urls"][index - 1]
            ):
                raise InvalidSourceError("Media original result identity differs")
            assets.append(item)
        variants = {}
        if not isinstance(manifest["display_variants"], list):
            raise InvalidSourceError("Media display variants must be explicit entries")
        for raw in manifest["display_variants"]:
            item = _asset(raw, observed_at=source.observed_at, variant=True)
            role = item["variant_key"]
            if (
                role in variants
                or item["source_role"] != "cover"
                or role != role.rsplit("_", 1)[0] + "_" + item["format"]
            ):
                raise InvalidSourceError("Media display variant role differs")
            if item["content_type"] != "image/" + ("jpeg" if item["format"] == "jpeg" else "webp"):
                raise InvalidSourceError("Media display variant content type differs")
            variants[role] = item
        expected_roles = {
            f"{size}_{fmt}"
            for size in ("thumb", "card", "detail", "poster_card", "poster_detail")
            for fmt in ("webp", "jpeg")
        }
        if set(variants) != expected_roles:
            raise InvalidSourceError("Media display variant coverage differs")
        summary = _object(
            manifest["storage_summary"],
            required="original_bytes variant_bytes object_count variant_count cdn_url_count",
        )
        # Roles may share one immutable object. Validate shared-key metadata
        # before deduplicating; dictionary last-wins would hide conflicting proof.
        physical, hashes = {}, {}
        for item in [*assets, *variants.values()]:
            key = item["storage_key"]
            previous = physical.get(key)
            if previous is not None and any(
                previous[field] != item[field]
                for field in ("bytes", "url", "format", "content_type", "width", "height")
            ):
                raise InvalidSourceError("Media shared object metadata conflicts")
            if "sha256" in item:
                if key in hashes and hashes[key] != item["sha256"]:
                    raise InvalidSourceError("Media shared object hash conflicts")
                hashes[key] = item["sha256"]
            physical.setdefault(key, item)
        original_keys = {item["storage_key"] for item in assets}
        variant_keys = {item["storage_key"] for item in variants.values()}
        physical_summary = {
            "original_bytes": sum(physical[key]["bytes"] for key in original_keys),
            "variant_bytes": sum(physical[key]["bytes"] for key in variant_keys),
            "object_count": len(physical),
            "variant_count": len(variant_keys),
            "cdn_url_count": len({item["url"] for item in physical.values()}),
        }
        # Preserved result-v1 evidence counted roles, even when cover/media used
        # the same key. Accept that exact historical calculation, not hybrids.
        legacy_summary = {
            "original_bytes": sum(item["bytes"] for item in assets),
            "variant_bytes": sum(item["bytes"] for item in variants.values()),
            "object_count": len(assets) + len(variants),
            "variant_count": len(variants),
            "cdn_url_count": len(assets) + len(variants),
        }
        if any(type(value) is not int for value in summary.values()) or summary not in (
            physical_summary,
            legacy_summary,
        ):
            raise InvalidSourceError("Media object summary differs from retained outputs")
        for role, key in keys.items():
            observed = variants.get(role) or next((item for item in assets if item["variant_key"] == role), None)
            if observed is None or observed["storage_key"] != key:
                raise InvalidSourceError("Media result changed a preserved object identity")
        manifest["display_variants"] = variants
        post["asset_manifest"] = manifest
        comments = {}
        if not isinstance(result["comment_results"], list):
            raise InvalidSourceError("Media comment outputs must be explicit entries")
        for raw in result["comment_results"]:
            item = _object(
                raw,
                required="comment_id hosted_media_urls media_mirror_status media_mirror_error",
                optional="hosted_author_profile_pic_url",
            )
            identity = item.pop("comment_id")
            if (
                identity in comments
                or item["media_mirror_status"] != "mirrored"
                or item["media_mirror_error"] is not None
            ):
                raise InvalidSourceError("Media comment output is incomplete or duplicated")
            comments[identity] = item
        if set(comments) != {item.comment_id for item in source.comments}:
            raise InvalidSourceError("Media comment output inventory differs")
        for item in source.comments:
            _references(comments[item.comment_id]["hosted_media_urls"])
            if len(comments[item.comment_id]["hosted_media_urls"]) != len(item.media_urls):
                raise InvalidSourceError("Comment hosted media count differs")
            for url in comments[item.comment_id]["hosted_media_urls"]:
                _public_reference(url)
        if not isinstance(result["avatars"], list) or len(result["avatars"]) != len(source.avatars):
            raise InvalidSourceError("Media avatar output inventory differs")
        tagged, owners, author_urls = {}, [], {}
        for raw, avatar in zip(result["avatars"], source.avatars, strict=True):
            item = _object(raw, required="username source_url hosted_url sha256 observed_at")
            _hash(item["sha256"])
            _public_reference(item["hosted_url"])
            if (
                item["username"] != avatar.username.lower()
                or item["source_url"] != source_reference(avatar.source_url)
                or _timestamp(item["observed_at"]) != avatar.observed_at
            ):
                raise InvalidSourceError("Media avatar result identity or observation differs")
            if avatar.role == "owner":
                owners.append(item["hosted_url"])
            elif avatar.role == "comment":
                author_urls[avatar.comment_id] = item["hosted_url"]
            else:
                tagged[avatar.username] = item["hosted_url"]
        if post.get("hosted_owner_profile_pic_url") != (owners[-1] if owners else None):
            raise InvalidSourceError("Hosted owner avatar differs from verified output")
        for identity, item in comments.items():
            if item.get("hosted_author_profile_pic_url") != author_urls.get(identity):
                raise InvalidSourceError("Hosted comment avatar differs from verified output")
        if not isinstance(result["tagged_profiles"], list):
            raise InvalidSourceError("Tagged profile outputs must be explicit entries")
        retained_tagged = {}
        for item in result["tagged_profiles"]:
            item = _object(item, required="username hosted_url")
            if item["username"] in retained_tagged:
                raise InvalidSourceError("Duplicate tagged avatar output")
            retained_tagged[item["username"]] = item["hosted_url"]
        if tagged != retained_tagged:
            raise InvalidSourceError("Tagged avatars differ from verified output")
        if tagged:
            post["hosted_tagged_profile_pics"] = tagged
        return MediaResult(source, post, comments, tuple(dict(item) for item in result["avatars"]))
    except InvalidSourceError:
        raise
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        raise InvalidSourceError("Retained media result cannot reconstruct the canonical contract") from error


def _listing(evidence: Evidence, *, account: str, target_key: str, parser_version: str, pinned: bool) -> ListingReplay:
    if parser_version != PARSER_VERSION:
        raise InvalidSourceError("Requested listing replay parser version is unavailable")
    payload = evidence.read(pinned=pinned)
    data = payload.get("data")
    if not is_anonymous_listing(payload) and (not isinstance(data, dict) or ROOT_FIELD not in data):
        if (
            PROVENANCE_KEY in payload
            or payload.get("errors")
            or payload.get("error")
            or payload.get("status") not in (None, "ok")
        ):
            if not classify_graphql_response(payload).accepted:
                raise InvalidSourceError("Listing source reported an unsuccessful response")
        # Earlier retained single-post listing observations remain replayable;
        # their own shortcode, rather than a page cursor, identifies the target.
        parsed = parse_evidence(
            evidence,
            account=account,
            stage=Stage.LISTING,
            target_key=target_key,
            parser_version=parser_version,
            pinned=pinned,
        )
        return ListingReplay(evidence.captured_at, (parsed,), None, None)
    decision = classify_graphql_response(payload)
    if (
        not is_anonymous_listing(payload)
        and not decision.accepted
        and decision.reason not in {"graphql_empty_connection", "graphql_invalid_connection"}
    ):
        raise InvalidSourceError("Listing source reported an unsuccessful response")
    page = parse_listing_page(payload, account=account, target_key=target_key)
    posts = []
    seen = set()
    for node in page.posts:
        post = normalize_post_source(node, account=account)
        if not post.shortcode or not post.source_id or post.owner is None or not post.owner.username:
            raise InvalidSourceError("Listing replay post identity or owner is missing")
        if post.shortcode in seen:
            raise InvalidSourceError("Listing replay contains duplicate post identities")
        seen.add(post.shortcode)
        posts.append(ParsedSource(Stage.LISTING, post.shortcode, evidence.captured_at, node, post=post))
    return ListingReplay(evidence.captured_at, tuple(posts), page.has_next_page, page.end_cursor)


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise InvalidSourceError("Media inventory observation timestamp is invalid") from None
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise InvalidSourceError("Media inventory requires its original observation timestamp")
    return datetime.fromisoformat(utc_timestamp(value))


def _uuid(value: Any, *, field: str) -> None:
    try:
        UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        raise InvalidSourceError(f"Media inventory {field} identity is invalid") from None


def _references(value: Any) -> None:
    if not isinstance(value, list) or any(not isinstance(url, str) or not url for url in value):
        raise InvalidSourceError("Media inventory URL references are invalid")


def _media(evidence: Evidence, *, target_key: str, parser_version: str, pinned: bool) -> MediaReplay:
    payload = evidence.read(pinned=pinned)
    if payload.get("shortcode") != target_key or evidence.source_identity != f"instagram:media:{target_key}":
        raise InvalidSourceError("Media inventory identity does not match its retained target")
    unavailable = payload.get("status")
    if isinstance(unavailable, str) and unavailable in {"deleted", "restricted", "media_unavailable"}:
        if parser_version not in {MEDIA_REPLAY_VERSION, MEDIA_RESULT_REPLAY_VERSION, "source-proof-v1"}:
            raise InvalidSourceError("Requested media source-proof parser version is unavailable")
        return MediaReplay(evidence.captured_at, payload, unavailable)
    if parser_version not in {MEDIA_REPLAY_VERSION, MEDIA_RESULT_REPLAY_VERSION}:
        raise InvalidSourceError("Requested media replay parser version is unavailable")
    marker = payload.get("ingestion_media")
    if not isinstance(marker, dict) or marker.get("version") != MEDIA_REPLAY_VERSION:
        raise InvalidSourceError("Media capture is not a retained versioned source inventory")
    if _timestamp(marker.get("observed_at")) != evidence.captured_at:
        raise InvalidSourceError("Media inventory observation differs from its retained capture")
    for field in ("post_id", "canonical_post_id"):
        _uuid(payload.get(field), field=field)
    _references(payload.get("media_urls"))
    count = payload.get("expected_media_count")
    if type(count) is not int or count < 0:
        raise InvalidSourceError("Media inventory expected count is unknown")
    for field in ("comments_complete", "avatars_complete", "include_comment_avatars"):
        if type(payload.get(field)) is not bool:
            raise InvalidSourceError("Media inventory coverage policy is unknown")
    for field in ("comments", "avatars", "media"):
        if not isinstance(payload.get(field), list) or any(not isinstance(item, dict) for item in payload[field]):
            raise InvalidSourceError("Media inventory members are invalid")
    comments = set()
    for comment in payload["comments"]:
        identity = comment.get("comment_id")
        if not isinstance(identity, str) or not identity or identity in comments:
            raise InvalidSourceError("Media inventory comment identity is invalid or duplicated")
        comments.add(identity)
        _uuid(comment.get("row_id"), field="comment row")
        _references(comment.get("media_urls"))
        _timestamp(comment.get("observed_at"))
    for avatar in payload["avatars"]:
        if not isinstance(avatar.get("username"), str) or not avatar["username"]:
            raise InvalidSourceError("Media inventory avatar identity is invalid")
        _timestamp(avatar.get("observed_at"))
    result = None
    if parser_version == MEDIA_RESULT_REPLAY_VERSION and "media_result" in payload:
        result = _decode_media_result(payload)
    return MediaReplay(evidence.captured_at, payload, result=result)


def _normalized(parsed: ReplaySource) -> Any:
    if isinstance(parsed, ListingReplay):
        value = {
            "posts": [post.post.to_dict() for post in parsed.posts],
            "pagination": {"has_next_page": parsed.has_next_page, "end_cursor": parsed.end_cursor},
        }
    elif isinstance(parsed, MediaReplay):
        value = {
            "inventory": parsed.inventory,
            "source_unavailable": parsed.source_unavailable,
            "apply_supported": parsed.result is not None,
            "apply_limitation": None if parsed.result is not None else MEDIA_APPLY_LIMITATION,
            "remote_objects_reverified": False,
        }
    elif isinstance(parsed, ParsedSource) and parsed.post:
        value = parsed.post.to_dict()
    else:
        value = [comment.to_dict() for comment in parsed.comments]
    return _JSON.dump_python(value, mode="json")


def prepare_replay(
    evidence_id: str, account: str, retained: dict[str, Any], *, parser_version: str
) -> tuple[dict[str, Any], ReplaySource]:
    """Compare parser outputs from the same original capture, without effects."""
    evidence = retained["evidence"]
    stage = Stage(retained["stage"])

    def parse(version):
        if stage == Stage.LISTING:
            return _listing(
                evidence,
                account=account,
                target_key=retained["target_key"],
                parser_version=version,
                pinned=retained["pinned"],
            )
        if stage == Stage.MEDIA:
            return _media(
                evidence, target_key=retained["target_key"], parser_version=version, pinned=retained["pinned"]
            )
        if stage == Stage.COMMENTS:
            return parse_comment_evidence(evidence, pinned=retained["pinned"], parser_version=version)
        return parse_evidence(
            evidence,
            account=account,
            stage=stage,
            target_key=retained["target_key"],
            parser_version=version,
            pinned=retained["pinned"],
        )

    parsed = parse(parser_version)
    previous = _normalized(parse(evidence.parser_version))
    current = _normalized(parsed)
    return {
        "mode": "dry_run",
        "evidence_id": evidence_id,
        "run_id": retained["run_id"],
        "stage": stage.value,
        "target_key": retained["target_key"],
        "captured_at": evidence.captured_at,
        "content_hash": evidence.content_hash,
        "capture_parser_version": evidence.parser_version,
        "parser_version": parser_version,
        "schema_version": evidence.schema_version,
        "normalized": current,
        "comparison": {"previous": previous, "current": current, "changed": previous != current},
        "writes": 0,
        "source_requests": 0,
    }, parsed


class Replay:
    def __init__(self, queries: Queries):
        self.queries = queries

    def preview(self, evidence_id: str, account: str, *, parser_version: str | None = None) -> dict[str, Any]:
        retained = self.queries.evidence(evidence_id, account)
        current = {
            Stage.COMMENTS: COMMENT_PARSER_VERSION,
            Stage.MEDIA: MEDIA_RESULT_REPLAY_VERSION,
            Stage.DETAILS: PARSER_VERSION,
            Stage.LISTING: PARSER_VERSION,
        }[Stage(retained["stage"])]
        return prepare_replay(evidence_id, account, retained, parser_version=parser_version or current)[0]


def _attachment_refresh(payload):
    if payload is None:
        return None
    from trr_backend.ingestion.comment_attachment_refresh import AttachmentRefresh

    return AttachmentRefresh.from_payload(payload)
