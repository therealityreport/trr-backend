"""Versioned, bounded source evidence. Transport/session objects never enter this API.

This projection is intentionally allowlisted. Adding a parser field requires an
explicit evidence-schema review; recursively deleting suspicious keys is not a
safe boundary for a provider response containing viewer-specific data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import unquote_plus, urlsplit, urlunsplit

from trr_backend.ingestion.domain import ReplayUnavailable, fingerprint, stable_json, utc_timestamp
from trr_backend.socials.instagram.graphql_response import PROVENANCE_KEY, project_graphql_provenance

SCHEMA_VERSION = 1
MAX_PAYLOAD_BYTES = 4 * 1024 * 1024
MAX_DEPTH = 32
RETENTION = timedelta(days=30)

# Source content only: no response headers, request arguments, session snapshots,
# viewer flags, friendship state, direct messages or exception representations.
CONTENT_KEYS = frozenset(
    {
        "id",
        "pk",
        "code",
        "shortcode",
        "media_id",
        "media_type",
        "__typename",
        "taken_at",
        "taken_at_timestamp",
        "timestamp",
        "created_at",
        "created_at_utc",
        "caption",
        "text",
        "alt",
        "accessibility_caption",
        "caption_is_edited",
        "owner",
        "user",
        "username",
        "full_name",
        "is_verified",
        "profile_pic_url",
        "profile_pic_url_hd",
        "width",
        "height",
        "dimensions",
        "original_width",
        "original_height",
        "image_versions2",
        "candidates",
        "video_versions",
        "url",
        "display_url",
        "display_resources",
        "thumbnail_src",
        "video_url",
        "src",
        "config_width",
        "config_height",
        "carousel_media",
        "carousel_media_count",
        "edge_sidecar_to_children",
        "edges",
        "node",
        "items",
        "data",
        "edge_owner_to_timeline_media",
        "include_comment_avatars",
        "ingestion_media",
        "has_more_head_child_comments",
        "next_min_child_cursor",
        "cached_comments_cursor",
        "tao_cursor",
        "bifilter_token",
        "media",
        "edge_media_to_caption",
        "edge_media_preview_like",
        "edge_liked_by",
        "count",
        "like_count",
        "comment_count",
        "video_view_count",
        "video_play_count",
        "play_count",
        "view_count",
        "video_duration",
        "is_video",
        "product_type",
        "location",
        "name",
        "slug",
        "address",
        "lat",
        "lng",
        "latitude",
        "longitude",
        "usertags",
        "in",
        "position",
        "edge_media_to_tagged_user",
        "x",
        "y",
        "coauthor_producers",
        "invited_coauthor_producers",
        "tagged_users",
        "comments",
        "replies",
        "parent_comment_id",
        "parent_comment_pk",
        "child_comments",
        "preview_child_comments",
        "child_comment_count",
        "comment_like_count",
        "edge_threaded_comments",
        "edge_media_to_parent_comment",
        "edge_media_to_comment",
        "edge_media_to_preview_comment",
        "page_info",
        "has_next_page",
        "end_cursor",
        "next_max_id",
        "has_more_comments",
        "has_more_headload_comments",
        "next_min_id",
        "has_more_tail_child_comments",
        "next_max_child_cursor",
        "comments_disabled",
        "commenting_disabled_for_viewer",
        "media_repost_count",
        "reshare_count",
        "repost_count",
        "share_count",
    }
) - {"commenting_disabled_for_viewer", "invited_coauthor_producers"}

# Public source aliases consumed by the maintained native/DTO parsers. These
# remain individually listed: arbitrary music/cursor/attachment objects are not
# copied wholesale, and viewer actions (for example did_report_as_spam) stay out.
CONTENT_KEYS |= frozenset(
    """
graphql shortcode_media xdt_shortcode_media xdt_api__v1__feed__user_timeline_graphql_connection
xdt_api__v1__media__media_id__comments__connection
xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection
hd_profile_pic_url_info is_edited has_translation caption_has_translation hashtags mentions
is_paid_partnership is_advertisement has_audio audio_url music_info clips_metadata users_in_photo
coauthor_producer_users like_and_view_counts_disabled hide_like_and_view_counts like_count_hidden
likes_hidden view_count_hidden views_hidden comment_count_hidden comments_count_hidden
reply_count replies_count repliesCount reply_count_observed reply_depth comment_id user_id likes
parent_comment_external_id source_snapshot_type owner_full_name owner_profile_pic_url
owner_profile_pic_url_hd owner_is_verified owner_detail tagged_users_detail collaborators_detail
post_type date_time media_urls thumbnail_url child_posts_data collaborators profile_tags
tag_x tag_y fbid_v2 is_mentionable is_private latest_reel_media profile_pic_id
is_covered is_ranked comment_index phase status is_pinned meta_ai_comment_type liked_by_media_coauthors
isCovered isRanked commentIndex api_phase isEdited isPinned metaAiCommentType childCommentCount
likedByMediaCoauthors cursor_min_id cursor_param cursor_payload comment_filter_param
cursorMinId cursorParam cursorPayload commentFilterParam min_id max_id after before
adapter_family adapter_version request_cursor relay_doc_id relay_friendly_name sort_order page_size
relay_logged_in relay_threads_style
listing_profile_id listing_profile_username
detail_snapshot version source observed_at coverage_version media_complete caption_present
likes_present comments_present media_count
music_asset_info music_consumption_info audio_asset_id audio_cluster_id title display_artist
artist_id duration_in_ms progressive_download_url cover_artwork_uri
comment_media commentMedia comment_gif commentGif media_versions attachment attachments content preview
image images sticker stickers sticker_asset stickerAsset static_sticker staticSticker gift gifts
gift_media giftMedia gif gif_media gifMedia giphy_media_info giphyMediaInfo first_party_cdn_proxied_images
tenor_media_info tenorMediaInfo
visual_media visualMedia animated_media animatedMedia original downsized fixed_height fixedHeight
fixed_width fixedWidth clip uri displayUrl videoUrl image_url imageUrl thumbnailUrl media_url mediaUrl
animated_image_url animatedImageUrl fixed_height_url fixedHeightUrl fixed_width_url fixedWidthUrl webp
storage_source_key post_id canonical_post_id row_id show_id season_number week_index expected_media_count
comments_complete avatars_complete avatars role source_url unavailable_avatars avatar_url_explicitly_missing
hosted_url content_hash
gif_media_id is_sticker attachment_refresh attachment_version ingestion_attachment native_gif_id captured_at
baseline_sha256 evidence_sha256 source_identity kind
""".split()
)

# Versioned ordered public-profile source fragments. No document bootstrap,
# request form, viewer state or response headers enter this content projection.
CONTENT_KEYS |= frozenset(
    {"xig_user_by_username", "polaris_ordered_timeline_connection", "pinned", "is_pinned_to_profile"}
)

CONTENT_KEYS |= frozenset(
    """
media_result outcome post_fields asset_manifest originals cover display_variants selection_policy
 display_variants_status display_variants_reason updated_at storage_summary original_bytes variant_bytes
object_count variant_count cdn_url_count comment_results tagged_profiles format content_type bytes storage_key
sha256 source_role variant_key generated_at hosted_thumbnail_url hosted_media_urls media_mirror_status
media_mirror_error profile_pic_mirror_status profile_pic_mirror_error hosted_owner_profile_pic_url
hosted_author_profile_pic_url
inventory_fingerprint result_inventory_fingerprint comment_settlement basis expected_total observed_total count_gap
""".split()
)

# Public commenter-profile projection: scalar metadata/counts only. The profile
# projector excludes timeline edges, relationships and all viewer/session state.
CONTENT_KEYS |= frozenset(
    """
biography followers_count follows_count posts_count profile_projection_version profile_invalid_fields
profile_source_provenance profile_query_id profile_query_name profile_provider_flags
PolarisCASB976ProfileEnabled PolarisCannesGuardianExperienceEnabled PolarisShortDramaEnabled
PolarisProfileRingsGradientEnabled PolarisRepostsConsumptionEnabled PolarisWebSchoolsEnabled
""".split()
)

URL_KEYS = frozenset(
    {"url", "src", "display_url", "thumbnail_src", "video_url", "profile_pic_url", "profile_pic_url_hd"}
)
URL_KEYS |= frozenset(
    """
audio_url media_urls thumbnail_url owner_profile_pic_url owner_profile_pic_url_hd
progressive_download_url cover_artwork_uri uri displayUrl videoUrl image_url imageUrl thumbnailUrl
media_url mediaUrl animated_image_url animatedImageUrl fixed_height_url fixedHeightUrl
fixed_width_url fixedWidthUrl webp
source_url hosted_url hosted_thumbnail_url hosted_media_urls hosted_owner_profile_pic_url hosted_author_profile_pic_url
""".split()
)


# Reviewed public-content delivery parameters on Meta CDN origins only. These
# authorize retrieval of a particular public asset, never an account session.
# Preserve their original encoding/order: re-encoding can invalidate signatures.
CDN_DELIVERY_KEYS = frozenset(
    {
        "oh",
        "oe",
        "stp",
        "efg",
        "ccb",
        "edm",
        "_nc_cat",
        "_nc_sid",
        "_nc_ht",
        "_nc_ohc",
        "_nc_oc",
        "_nc_gid",
        "_nc_zt",
        "_nc_rid",
        "_nc_ad",
        "_nc_cid",
        "_nc_ss",
        "ig_cache_key",
        "bytestart",
        "byteend",
    }
)


def _media_reference(value: str) -> str | None:
    try:
        parts = urlsplit(value)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
            return None
        host = parts.hostname.lower()
        # Native GIF attachments use this exact delivery host. Keep only the
        # reviewed delivery keys; other fbsbx.com hosts retain no query fields.
        is_cdn = host == "cdn.fbsbx.com" or any(
            host == base or host.endswith("." + base) for base in ("cdninstagram.com", "fbcdn.net")
        )
        query = "&".join(
            segment
            for segment in parts.query.split("&")
            if is_cdn and unquote_plus(segment.split("=", 1)[0]) in CDN_DELIVERY_KEYS
        )
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, ""))
    except ValueError:
        return None


def sanitize_payload(payload: dict[str, Any], *, parser_version: str | None = None) -> dict[str, Any]:
    if parser_version in {"instagram-saved-owner-asset-input-v1", "instagram-saved-owner-asset-authorization-v1"}:
        from trr_backend.ingestion.saved_owner_asset import validate_payload

        return validate_payload(payload, parser_version=parser_version)
    if parser_version == "instagram-cached-avatar-regional-alias-v1":
        from trr_backend.ingestion.cached_avatar_alias import validate_payload

        result = validate_payload(payload)
        if len(stable_json(result).encode()) > MAX_PAYLOAD_BYTES:
            raise ValueError("Evidence exceeds 4 MiB")
        return result
    if parser_version == "instagram-public-profile-failure-v1":
        from trr_backend.ingestion.profile_failure_diagnostics import validate_payload

        return validate_payload(payload)

    def project(value: Any, depth: int = 0, key: str = "") -> Any:
        if depth > MAX_DEPTH:
            raise ValueError("Evidence nesting exceeds the supported bound")
        if isinstance(value, dict):
            return {name: project(child, depth + 1, name) for name, child in value.items() if name in CONTENT_KEYS}
        if isinstance(value, list):
            return [project(child, depth + 1, key) for child in value]
        if value is None or type(value) in (bool, int, float):
            return value
        if isinstance(value, str):
            return _media_reference(value) if key in URL_KEYS else value
        raise ValueError("Evidence must contain JSON values only")

    if not isinstance(payload, dict):
        raise ValueError("Evidence must be a source content object")
    result = project(payload)
    provenance = project_graphql_provenance(payload)
    if provenance is not None:
        result[PROVENANCE_KEY] = provenance
    if len(stable_json(result).encode()) > MAX_PAYLOAD_BYTES:
        raise ValueError("Evidence exceeds 4 MiB")
    return result


@dataclass(frozen=True)
class Evidence:
    source_identity: str
    captured_at: datetime
    parser_version: str
    payload_json: str
    content_hash: str
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def capture(
        cls, *, source_identity: str, captured_at: datetime, parser_version: str, payload: dict[str, Any]
    ) -> Evidence:
        if not source_identity or not parser_version:
            raise ValueError("Source identity and parser version are required")
        sanitized = sanitize_payload(payload, parser_version=parser_version)
        return cls(
            source_identity=source_identity,
            captured_at=datetime.fromisoformat(utc_timestamp(captured_at)),
            parser_version=parser_version,
            payload_json=stable_json(sanitized),
            content_hash=fingerprint(sanitized),
        )

    @property
    def expires_at(self) -> datetime:
        return self.captured_at + RETENTION

    def read(self, *, now: datetime | None = None, pinned: bool = False) -> dict[str, Any]:
        import json

        now = datetime.fromisoformat(utc_timestamp(now or datetime.now(UTC)))
        if now >= self.expires_at and not pinned:
            raise ReplayUnavailable("Retained source evidence has expired")
        if self.schema_version != SCHEMA_VERSION:
            raise ReplayUnavailable("Unsupported evidence schema version")
        payload = json.loads(self.payload_json)
        if fingerprint(payload) != self.content_hash:
            raise ValueError("Evidence content hash mismatch")
        if (
            stable_json(payload) != self.payload_json
            or sanitize_payload(payload, parser_version=self.parser_version) != payload
        ):
            raise ValueError("Evidence is not canonical sanitized source content")
        return payload
