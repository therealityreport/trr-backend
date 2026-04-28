# Instagram Data Contract

Status: Phase 1 documentation contract for the Instagram queryable-data plan. This file is documentation-only. It does not approve schema, runner, migration, backfill, Python, or UI changes.

Plan reference: `/Users/thomashulihan/Projects/TRR/docs/codex/plans/2026-04-28-instagram-post-queryable-data-plan.md`

## Contract Rules

- Repo-native Instagram source families are the source of truth. Apify-style names are adapter/reference aliases only.
- `latestComments`, `firstComment`, XDT embedded comment snapshots, and similar partial snippets are not persisted as comments, not stored in a child table, and not counted as comment coverage.
- Full comment and reply payloads from the comments REST lane are the only persisted/queryable Instagram comment source.
- Raw source payloads remain forensic data. Legacy raw columns are currently a transitional exposure risk; new raw/observation/profile diagnostic payloads must be private or admin/service-role only.
- Viewer-session fields are diagnostics, not stable business facts. They must not drive public/admin product state unless a later plan explicitly promotes a specific field.
- Following-list rows are in scope only for explicit `following` collection. Follower-list collection is out of scope.

## Source Families

| Source family | Repo-native evidence | Request tags and response paths |
| --- | --- | --- |
| `profile_timeline_xdt` | `trr_backend/socials/instagram/posts_scrapling/fetcher.py`, `trr_backend/socials/instagram/scraper.py`, `trr_backend/repositories/social_season_analytics.py` | `xdt_api__v1__feed__user_timeline_graphql_connection`, `PolarisProfilePostsTabContentQuery_connection`; form/runtime tags: `fb_api_caller_class`, `fb_api_req_friendly_name`, `variables`, `server_timestamps`, `doc_id`, `av`, `__d`, `__user`, `__a`, `__req`, `__comet_req`, `lsd`, `__spin_r`, `__spin_b`, `__spin_t`, `hsi`; headers: `x-fb-friendly-name`, `x-fb-lsd`, `x-asbd-id`, `x-bloks-version-id`; response paths: `data.xdt_api__v1__feed__user_timeline_graphql_connection.edges`, `.page_info`, `.count`. |
| `shortcode_graphql` | `trr_backend/socials/instagram/permalink_metadata.py` | `PolarisPostActionLoadPostQueryQuery`; response paths: `data.xdt_shortcode_media`, legacy `graphql.shortcode_media`. |
| `media_info_rest` | `trr_backend/socials/instagram/constants.py`, `trr_backend/socials/instagram/permalink_metadata.py`, `trr_backend/socials/instagram/scraper.py` | `https://www.instagram.com/api/v1/media/{media_id}/info/`; response path: `items[0]`. |
| `permalink_html_meta` | `trr_backend/socials/instagram/permalink_metadata.py` | `script[data-sjs]`, `window._sharedData`, `__additionalDataLoaded(...)`, `script[type="application/ld+json"]`, `meta[property="og:image"]`, `meta[property="og:video"]`; JSON-LD fields: `image`, `video.contentUrl`, `thumbnailUrl`. |
| `web_profile_info` | `trr_backend/socials/instagram/constants.py`, `trr_backend/socials/instagram/scraper.py`, runtime adapters | `https://www.instagram.com/api/v1/users/web_profile_info/?username={username}`; response paths: `data.user`, `data.user.edge_owner_to_timeline_media.edges`, `.page_info`, `.count`. |
| `comments_rest` | `trr_backend/socials/instagram/constants.py`, `trr_backend/socials/instagram/comments_scrapling/fetcher.py`, `trr_backend/socials/instagram/comments_scrapling/persistence.py` | `https://www.instagram.com/api/v1/media/{media_id}/comments/` and `https://www.instagram.com/api/v1/media/{media_id}/comments/{comment_id}/child_comments/`; page fields: `comments[]`, `has_more_comments`, `has_more_headload_comments`, `next_min_id`, `next_max_id`, `child_comments[]`, `has_more_tail_child_comments`, `next_min_child_cursor`. |
| `profile_following_list` | `trr_backend/repositories/social_season_analytics.py` shared job stage `instagram_profile_following` | `https://www.instagram.com/api/v1/friendships/{profile_id}/following/`; request params: `count`, `max_id`; response paths: `users[]`, `next_max_id`, `big_list`/`has_more`. Persist only when the requested mode and source row both mean `following`. |
| `apify_reference_adapter` | `trr_backend/socials/instagram/apify_scraper.py` | Adapter/reference aliases only. These names map into the source families above and must not define canonical field names. |

## Storage Classifications

| Classification | Meaning |
| --- | --- |
| `normalized_scalar_column` | A durable scalar that should be promoted into a typed table column. |
| `child_table_row` | Repeatable or joinable data that should become rows in a child/query table. |
| `indexed_jsonb_diagnostic_column` | Structured diagnostic data that may stay JSONB only with a documented query owner/index. |
| `viewer_session_diagnostic` | Authenticated-viewer-dependent data; private diagnostic only unless later promoted. |
| `raw_only_unknown_future_field` | Retained only in raw/observation payloads until the source meaning is understood. |
| `adapter_alias` | Non-canonical external/adapter name that maps to repo-native source fields. |
| `excluded_not_persisted` | Must not be normalized, persisted as queryable rows, or counted. |

## Privacy Classifications

| Privacy | Meaning |
| --- | --- |
| `public_curated` | Safe to expose through curated public/admin read shapes. |
| `admin_only` | Operator-only, usually because it includes provenance, assignment, or diagnostics. |
| `service_role_private` | Raw/normalized payloads, legacy refs, and sensitive diagnostics. |
| `transitional_public_raw_risk` | Existing legacy raw columns that are currently public by grants/policies until a later privacy cleanup. |

Current raw posture: legacy `social.instagram_posts.raw_data`, `social.instagram_comments.raw_data`, and `social.instagram_account_catalog_posts.raw_data` are a transitional public raw risk. New raw payloads, `social.social_post_observations.raw_payload`, `social.social_post_observations.normalized_payload`, planned `social.instagram_profiles.raw_data`, and planned `social.instagram_profiles.about_raw` must be service-role/private or exposed only through curated admin APIs.

## Post Field Contract

| Canonical field family | Repo-native fields | Storage target | API exposure | Notes |
| --- | --- | --- | --- | --- |
| Source identity | `shortcode`, `code`, `pk`, `id`, media id from media-info REST | `social.social_posts.source_id`; legacy refs in `social.social_post_legacy_refs`; compatibility in `social.instagram_posts.shortcode`, `media_id`, `social.instagram_account_catalog_posts.source_id` | Public curated | Use shortcode/code as the route/catalog source id where available. Preserve media id as external id/provenance. |
| Owner identity | `user.username`, `owner.username`, `user.pk`, `user.id`, `owner.id` | `social.social_posts.owner_handle`, `owner_handle_norm`, `owner_id`; compatibility bridge where current reads need it | Public curated | Apify `ownerUsername`, `ownerId`, and `ownerFullName` are aliases only. |
| Owner profile details | `owner.full_name`, `owner.profile_pic_url`, `owner.profile_pic_url_hd`, `owner.is_verified`, matching `user.*` fields | `social.social_posts` owner scalars where supported; otherwise Instagram compatibility columns or `social.social_post_entities`/diagnostic payload until canonical fields exist | Public curated for scalar owner facts; admin-only for diagnostic mismatches | Hosted owner profile pictures remain mirror metadata. |
| Caption/body | `caption.text`, `edge_media_to_caption.edges[].node.text`, string caption aliases | `social.social_posts.body`; compatibility in `instagram_posts.caption`, catalog `caption`/`text`; search bridge in `instagram_posts.search_text` | Public curated | Caption metadata such as caption id, edited, translation state needs bridge columns or normalized payload until schema exists. |
| Post URL | permalink from shortcode, explicit `permalink`, `url`, HTML canonical/meta fallbacks | `social.social_posts.canonical_url`; compatibility in catalog `permalink` | Public curated | Prefer canonical Instagram permalink. |
| Posted time | `taken_at`, `taken_at_timestamp`, `timestamp` | `social.social_posts.posted_at`; compatibility `posted_at` | Public curated | Store as timestamptz. |
| Media type/product type | `media_type`, `__typename`, `product_type`, `productType`, `is_video` | `social.social_posts.media_type`; product/post format as Instagram-specific scalar or normalized payload | Public curated | XDT integer media types map to image/video/carousel. |
| Engagement counts | `like_count`, `edge_media_preview_like.count`, `comment_count`, `edge_media_to_comment.count`, `view_count`, `play_count`, `video_view_count`, `video_play_count` | `social.social_posts.like_count`, `comment_count`, `view_count`; compatibility counts in legacy tables | Public curated | Reported `comment_count` is not saved comment coverage. Saved coverage comes from full comments rows. |
| Disabled/visibility flags | `comments_disabled`, `is_comments_disabled`, `commenting_disabled_for_viewer`, `like_and_view_counts_disabled` | Normalized scalar columns or admin diagnostic payload until schema exists | Public curated if stable source fact; admin-only if viewer-specific | `commenting_disabled_for_viewer` depends on the session and should stay diagnostic unless promoted. |
| Media variants | `image_versions2.candidates[]`, `display_resources[]`, `display_url`, `thumbnail_src`, `video_versions[]`, `video_url`, `video_dash_manifest`, JSON-LD image/video | `social.social_post_media_assets`; compatibility `media_urls`, `thumbnail_url`, hosted mirror fields | Public curated | Detailed arrays should load in post detail routes, not every list row. |
| Carousel children | `carousel_media[]`, `edge_sidecar_to_children.edges[].node`, Apify `childPosts` alias | `social.social_post_media_assets` rows and/or `child_posts_data` compatibility while migrating | Public curated in detail route | Child payload raw stays private/diagnostic. |
| Dimensions and duration | `original_width`, `original_height`, candidate width/height, `video_duration`, derived duration | `social.social_post_media_assets.width`, `height`, `duration_seconds`; compatibility columns where present | Public curated | Subsecond duration can stay in payload until a numeric field is approved. |
| Hashtags and mentions | caption extraction, `hashtags`, `mentions`, `search_hashtags`, `search_handles` | `social.social_post_entities` with `entity_type='hashtag'`/`'mention'`; compatibility arrays/search columns | Public curated | Existing `instagram_posts.search_*` columns remain bridge fields. |
| Tagged users/profile tags | `usertags.in[]`, `edge_media_to_tagged_user.edges[]`, Apify `taggedUsers` alias | Prefer `social.social_post_entities` after entity type support; otherwise Instagram-specific child table keyed by canonical post id | Public curated for user identity; admin-only for source path/raw geometry diagnostics | Tag coordinates are stable enough for detail views but not needed in list rows. |
| Collaborators/coauthors | `coauthor_producers[]`, `invited_coauthor_producers[]`, Apify `coauthorProducers` alias | `social.social_post_entities` with `entity_type='collaborator'`; existing `social.instagram_account_catalog_post_collaborators` remains compatibility | Public curated | Do not create a duplicate Instagram collaborator table unless canonical/entity reuse is formally rejected. |
| Location | repo-native location object if present; Apify `locationName`/`locationId` aliases | `social.social_post_entities` if `location` entity type is approved, or Instagram-specific extension keyed by canonical post id | Public curated for id/name; admin-only for raw object | Phase 2 must decide exact table support before code writes. |
| Music/audio | `music_info`, `audio_url`, audio/sound references | `social.social_post_entities` with `entity_type='sound'` plus media/diagnostic payload | Public curated if displayable; admin-only for raw attribution payload | Existing legacy/catalog bridge columns may continue while canonicalization lands. |
| Paid/ad/partnership flags | `is_paid_partnership`, `paid_partnership`, `isAdvertisement`, `is_advertisement` | Normalized scalar or admin diagnostic payload depending on source stability | Public curated if stable source fact | Keep aliases mapped back to repo-native or normalized DTO fields. |
| Viewer actions | `has_liked`, `has_viewer_saved`, `can_viewer_reshare`, `friendship_status`, `top_likers` | Private diagnostic payload only | Not public | These are session-dependent and must not become durable business facts without explicit approval. |
| Embedded/latest comment samples | `latestComments`, `firstComment`, XDT embedded snippets | Excluded; retain only in raw/observation payload if raw retention keeps them | Not public | Not persisted, not comment coverage, not a child table. |

## Profile Field Contract

| Canonical field family | Repo-native fields | Storage target | Privacy/session dependence | API exposure |
| --- | --- | --- | --- | --- |
| Profile identity | `data.user.id`, `pk`, `username`, `full_name` | Planned `social.instagram_profiles.profile_id`, `username`, `normalized_username`, `full_name` | Stable public fact | Public curated/admin |
| Profile URL/input URL | input handle URL, canonical profile URL | Planned profile scalar columns | Stable public fact | Public curated/admin |
| Biography | `biography` | Planned profile scalar | Stable public fact | Public curated/admin |
| Counts | follower count, following/follows count, post count, highlight/IGTV count where present | Planned profile scalar counts | Stable public fact, but snapshot-time dependent | Public curated/admin with `last_scraped_at` |
| Business/private/verified flags | `is_private`, `is_verified`, `is_business_account`, `joined_recently`, `has_channel`, `business_category_name` | Planned profile scalar columns | Stable enough for snapshot reads | Public curated/admin |
| Profile picture variants | `profile_pic_url`, `profile_pic_url_hd`, hosted variants | Planned profile scalar columns plus hosted mirror fields | Stable public fact; hosted fields are operational | Public curated/admin |
| External URL and links | `external_url`, shimmed URL, link list fields when present | Planned `social.instagram_profile_external_links` child rows plus scalar primary URL | Public if source is public; admin-only for raw shim diagnostics | Public curated/admin |
| Account-about country | `about.country`, equivalent account-about payload fields | Planned profile scalar plus `about_raw` private snapshot | Stable enough only after source inspection | Public/admin only after source meaning is verified |
| Date joined | `about.date_joined`, timestamp parse if possible | Planned profile scalar date text and timestamptz | Stable enough only after source inspection | Public/admin only after source meaning is verified |
| Date verified/history | `about.date_verified`, verification history fields | Planned profile scalar and private diagnostics | Some fields may be account/session dependent | Admin curated unless product-approved for public |
| Former usernames/count | `about.former_usernames_count`, former username/history fields | Planned scalar count plus private diagnostics | Potentially sensitive/account-about data | Admin curated by default |
| Shared followers/session diagnostics | `about.accounts_with_shared_followers`, `friendship_status`, mutual/followed-by viewer fields | `about_raw` or diagnostic-only private storage | Viewer-session dependent | Not public; admin diagnostic only if needed |
| Unknown profile/about fields | any unclassified `data.user.*` or `about.*` | Private raw/diagnostic only | Unknown | Not public until classified |

Profile backfill note: no existing `social.instagram_profiles` table is confirmed in the current migrations. Existing post/catalog raw payloads can provide partial owner/profile fields, but full profile/about/external-link coverage generally requires bounded fresh profile snapshot scrapes.

## Relationship Field Contract

Following relationships are planned as Instagram profile relationships, not post data.

| Field | Source fields | Storage target | API exposure | Notes |
| --- | --- | --- | --- | --- |
| Owner account | `username_scrape`, selected profile handle/id | Planned `social.instagram_profile_relationships.owner_profile_id`, owner username/profile id | Admin curated | Owner must be explicit; do not infer from related row alone. |
| Direction | `type` plus requested scrape mode | Planned `relationship_type='following'` | Admin curated | Persist only `Following`, `following`, or `Follows` when requested mode is following. Reject/skip `Followers`. |
| Related identity | `id`, `username`, `full_name` | Planned related id/username/full name columns | Admin curated | Related profile may not be a curated shared source. |
| Related flags/avatar | `is_private`, `is_verified`, `profile_pic_url`, hosted variant | Planned scalar columns | Admin curated | Host avatar mirror fields are operational/admin. |
| Page/cursor provenance | source page ordinal, source cursor, next cursor, page size/rank | Planned page/cursor metadata columns or job/run metadata | Admin-only | Needed to debug capped/partial collections. |
| Raw mismatched follower rows | rows where source says `Followers` | Private diagnostic only | Not public | Must not be persisted as following relationships. |

## Comment Field Contract

The current full comment path is `comments_rest` through the comments Scrapling lane and `social.instagram_comments`.

| Canonical field family | Source fields | Storage target | API exposure | Notes |
| --- | --- | --- | --- | --- |
| Comment identity | `pk`, `id` | `social.instagram_comments.comment_id` | Public/admin curated | Unique by post/comment after the composite uniqueness migration. |
| Parent/reply identity | reply endpoint parent id, nested `replies[]`, `child_comments[]` | `social.instagram_comments.parent_comment_id`, `is_reply`; optional future `parent_comment_external_id`, `root_comment_id`, `reply_depth` | Public/admin curated | Persist nested replies as separate comment rows. |
| Author identity | `user.pk`, `user.id`, `user.username`, `owner.id`, `owner.username`, `ownerUsername`, `ownerId` | `username`, `user_id` | Public/admin curated | Adapter aliases map to the same author fields. |
| Author avatar/verification | `ownerProfilePicUrl`, `ownerProfilePicUrlHd`, `owner.profile_pic_url`, `user.profile_pic_url`, `owner.is_verified`, `user.is_verified` | `author_profile_pic_url`, `hosted_author_profile_pic_url`, `author_is_verified` | Public/admin curated | Hosted avatar is mirror metadata. |
| Comment text/time | `text`, `created_at`, `timestamp` | `text`, `created_at`, `scraped_at` | Public/admin curated | Deleted/missing lifecycle fields are admin-operational. |
| Likes/reply count | `comment_like_count`, `like_count`, `likesCount`, `child_comment_count`, `repliesCount` | `likes`, `reply_count` | Public/admin curated | Counts are source-reported snapshot values. |
| Comment media | optional media nodes, `media_urls` | `media_urls`, hosted mirror fields | Public/admin curated for URLs; admin for mirror status | Only when present in full comments payloads. |
| Pagination | `has_more_comments`, `has_more_headload_comments`, `next_min_id`, `next_max_id`, `has_more_tail_child_comments`, `next_min_child_cursor` | Job/run metadata and API pagination | Admin-only for raw cursors unless needed by client | Repeated cursor/page cap/deadline failures must be classified. |

## Adapter Alias Rules

Apify/reference names are adapter aliases only:

- `shortCode` maps to repo-native `shortcode`/`code`.
- `displayUrl`, `videoUrl`, and `displayResourceUrls` map to media variant extraction.
- `ownerUsername`, `ownerFullName`, `ownerId`, and owner avatar aliases map to owner identity/detail fields.
- `locationName` and `locationId` map to the location field family, not an Apify-owned schema.
- `taggedUsers` maps to `usertags.in[]` / `edge_media_to_tagged_user.edges[]`.
- `coauthorProducers` maps to `coauthor_producers[]` / `invited_coauthor_producers[]`.
- `musicInfo`, `videoPlayCount`, `videoDuration`, `commentsCount`, and `likesCount` map to canonical music/media/engagement fields.
- `latestComments` and `firstComment` are excluded partial samples. They do not create comment rows, coverage counts, or API sample sections.

## API Contract Pointer

The Phase 5 response examples and route appendix live in `TRR-Backend/docs/social/instagram-api-contract-appendix.md`. That appendix is the consumer-facing contract gate before backend read-path/API work starts.
