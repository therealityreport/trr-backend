# Instagram API Contract Appendix

Status: Phase 5 contract appendix gate for the Instagram queryable-data plan. This file is documentation-only. It defines the response shapes that backend read-path/API work must follow later.

Plan reference: `/Users/thomashulihan/Projects/TRR/docs/codex/plans/2026-04-28-instagram-post-queryable-data-plan.md`

## Gate Rules

- TRR-Backend owns these response contracts.
- TRR-APP must consume backend routes or compatibility proxy routes. Do not add app direct SQL reads for the new Instagram tables.
- Response changes must be additive to existing routes.
- Raw payloads, raw observation snapshots, `raw_data`, `about_raw`, `normalized_payload`, and legacy refs are excluded from client responses.
- `latestComments`, `firstComment`, and embedded comment snippets are excluded from all response examples and must not be exposed as saved comments or coverage counts.

## Existing Routes To Modify

These routes already exist in `api/routers/socials.py` and should be expanded additively when Phase 5 implementation starts:

| Route | Current owner | Phase 5 role |
| --- | --- | --- |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/summary` | `get_social_account_profile_summary_route` | Keep summary bounded; do not load heavy detail arrays on initial page load. |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/dashboard` | `get_social_account_profile_dashboard_route` | Keep dashboard composition backend-owned. |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/posts` | `get_social_account_profile_posts_route` | Add bounded list-row fields only when they are useful for scanning/filtering. |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/comments` | `get_social_account_profile_comments_route` | Add author avatar, author verification, reply, and optional threaded metadata. |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/posts` | `get_social_account_catalog_posts_route` | Add curated catalog-row fields from canonical/queryable storage. |
| `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/posts/{source_id}/detail` | `get_social_account_catalog_post_detail_route` | Add full post detail arrays and saved comment/thread summaries. |

## New Routes

Add these routes only after profile and following storage exists:

| Route | Purpose |
| --- | --- |
| `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/profile` | Profile detail response with profile/about/external-link/picture/count fields. |
| `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/relationships?type=following` | Bounded following relationship list. `type` must accept only `following` in this plan. |
| `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/comments/{comment_id}/thread` | Single comment thread with parent and nested replies if the existing comments list cannot serve the modal/thread view efficiently. |

## Pagination Shape

Existing page-based routes should keep the current page envelope and may add cursor fields when a source collection needs them:

```json
{
  "pagination": {
    "page": 1,
    "page_size": 25,
    "total": 431,
    "total_pages": 18,
    "has_more": true,
    "next_cursor": "QVFD...",
    "cursor_source": "source_pagination"
  }
}
```

Rules:

- `page`, `page_size`, `total`, and `total_pages` remain required for existing list routes.
- `has_more`, `next_cursor`, and `cursor_source` are optional and only appear when the route is backed by cursor/page provenance.
- Relationship and comment collection responses must include cap/completeness metadata when a scrape stopped early.
- Raw cursor payloads are admin-only diagnostics; client routes should receive sanitized cursor tokens only when the client needs to request the next page.

## Catalog Row Example

Route: `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/posts`

```json
{
  "items": [
    {
      "platform": "instagram",
      "account_handle": "thetraitorsus",
      "id": "catalog-row-uuid",
      "source_id": "C6Example",
      "canonical_post_id": "social-post-uuid",
      "source_surface": "catalog",
      "url": "https://www.instagram.com/p/C6Example/",
      "posted_at": "2026-04-20T18:30:00Z",
      "title": null,
      "content": "Castle breakfast reaction.",
      "owner": {
        "id": "17841400000000000",
        "username": "thetraitorsus",
        "full_name": "The Traitors",
        "profile_pic_url": "https://instagram.example/profile.jpg",
        "hosted_profile_pic_url": "https://cdn.trr.example/instagram/profiles/thetraitorsus.jpg",
        "is_verified": true
      },
      "location": {
        "id": "213385402",
        "name": "Scotland"
      },
      "media": {
        "media_type": "carousel",
        "product_type": "feed",
        "thumbnail_url": "https://cdn.trr.example/post-thumb.jpg",
        "source_thumbnail_url": "https://instagram.example/post-thumb.jpg",
        "hosted_thumbnail_url": "https://cdn.trr.example/post-thumb.jpg",
        "media_variants_count": 6,
        "carousel_children_count": 3
      },
      "saved_metrics": {
        "likes_count": 1240,
        "comments_count": 86,
        "views_count": 0,
        "video_play_count": 0,
        "saved_comments": 54
      },
      "flags": {
        "comments_disabled": false,
        "like_and_view_counts_disabled": false,
        "is_paid_partnership": false,
        "is_advertisement": false
      },
      "entities": {
        "hashtags": ["thetraitors"],
        "mentions": ["peacock"],
        "collaborators": ["peacock"],
        "tagged_users_count": 2
      },
      "assignment_status": "assigned",
      "assignment_source": "admin",
      "candidate_matches": []
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 25,
    "total": 431,
    "total_pages": 18
  }
}
```

List-row limits:

- Include counts and first/best media URLs.
- Do not include full `media_assets[]`, `tagged_users_detail[]`, `collaborators_detail[]`, `child_posts_data[]`, comments, raw payloads, or embedded/latest comment samples.

## Post Detail Example

Route: `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/posts/{source_id}/detail`

```json
{
  "platform": "instagram",
  "account_handle": "thetraitorsus",
  "id": "instagram-post-row-uuid",
  "canonical_post_id": "social-post-uuid",
  "source_id": "C6Example",
  "source_surface": "materialized",
  "url": "https://www.instagram.com/p/C6Example/",
  "posted_at": "2026-04-20T18:30:00Z",
  "content": "Castle breakfast reaction.",
  "caption_metadata": {
    "caption_id": "caption-123",
    "is_edited": false,
    "has_translation": false
  },
  "owner": {
    "id": "17841400000000000",
    "username": "thetraitorsus",
    "full_name": "The Traitors",
    "profile_pic_url": "https://instagram.example/profile.jpg",
    "hosted_profile_pic_url": "https://cdn.trr.example/instagram/profiles/thetraitorsus.jpg",
    "is_verified": true
  },
  "location": {
    "id": "213385402",
    "name": "Scotland"
  },
  "media": {
    "media_type": "carousel",
    "product_type": "feed",
    "post_format": "carousel",
    "assets": [
      {
        "position": 0,
        "role": "display",
        "source_url": "https://instagram.example/image-0.jpg",
        "hosted_url": "https://cdn.trr.example/image-0.jpg",
        "thumbnail_url": "https://instagram.example/thumb-0.jpg",
        "hosted_thumbnail_url": "https://cdn.trr.example/thumb-0.jpg",
        "width": 1440,
        "height": 1800,
        "duration_seconds": null,
        "mirror_status": "mirrored"
      }
    ],
    "audio": {
      "has_audio": false,
      "audio_url": null,
      "music_info": null
    }
  },
  "entities": {
    "hashtags": ["thetraitors"],
    "mentions": ["peacock"],
    "tagged_users_detail": [
      {
        "username": "peacock",
        "user_id": "17841411111111111",
        "full_name": "Peacock",
        "is_verified": true,
        "profile_pic_url": "https://instagram.example/peacock.jpg",
        "hosted_profile_pic_url": "https://cdn.trr.example/peacock.jpg",
        "tag_x": 0.42,
        "tag_y": 0.55
      }
    ],
    "collaborators_detail": [
      {
        "username": "peacock",
        "user_id": "17841411111111111",
        "full_name": "Peacock",
        "is_verified": true,
        "profile_pic_url": "https://instagram.example/peacock.jpg",
        "hosted_profile_pic_url": "https://cdn.trr.example/peacock.jpg"
      }
    ]
  },
  "flags": {
    "comments_disabled": false,
    "commenting_disabled_for_viewer": null,
    "like_and_view_counts_disabled": false,
    "is_paid_partnership": false,
    "is_advertisement": false
  },
  "saved_metrics": {
    "likes_count": 1240,
    "comments_count": 86,
    "views_count": 0,
    "video_play_count": 0,
    "saved_comments": 54
  },
  "comment_summary": {
    "reported_comments": 86,
    "saved_comments": 54,
    "coverage_status": "partial",
    "coverage_source": "full_comments_scrape"
  },
  "admin_only": {
    "last_scrape_job_id": "scrape-job-uuid",
    "last_scrape_run_id": "scrape-run-uuid",
    "last_observed_at": "2026-04-28T12:00:00Z",
    "source_shape": "profile_timeline_xdt",
    "raw_observation_diff_url": "/api/v1/admin/socials/profiles/instagram/thetraitorsus/catalog/posts/C6Example/raw-diff"
  }
}
```

Excluded from post detail:

- `raw_data`
- `raw_payload`
- `normalized_payload`
- `social_post_legacy_refs`
- cookies/session tokens/runtime GraphQL tags
- `latestComments`, `firstComment`, and embedded comment snippets

## Profile Detail Example

Route: `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/profile`

```json
{
  "platform": "instagram",
  "account_handle": "thetraitorsus",
  "profile": {
    "id": "instagram-profile-row-uuid",
    "profile_id": "17841400000000000",
    "username": "thetraitorsus",
    "normalized_username": "thetraitorsus",
    "url": "https://www.instagram.com/thetraitorsus/",
    "input_url": "https://www.instagram.com/thetraitorsus/",
    "full_name": "The Traitors",
    "biography": "The official account for The Traitors.",
    "is_private": false,
    "is_verified": true,
    "is_business_account": true,
    "business_category_name": "TV show",
    "joined_recently": false,
    "has_channel": false
  },
  "counts": {
    "followers_count": 480000,
    "follows_count": 81,
    "posts_count": 431,
    "highlight_reel_count": 12,
    "igtv_video_count": 0
  },
  "account_about": {
    "country": "United States",
    "date_joined": "January 2023",
    "date_joined_at": "2023-01-01T00:00:00Z",
    "date_verified": null,
    "date_verified_at": null,
    "former_usernames_count": 0,
    "verification_history": []
  },
  "external_links": [
    {
      "index": 0,
      "title": "Watch on Peacock",
      "url": "https://www.peacocktv.com/stream-tv/the-traitors",
      "shimmed_url": "https://l.instagram.com/?u=https%3A%2F%2Fwww.peacocktv.com%2F...",
      "link_type": "external"
    }
  ],
  "profile_picture": {
    "profile_pic_url": "https://instagram.example/profile.jpg",
    "profile_pic_url_hd": "https://instagram.example/profile-hd.jpg",
    "hosted_profile_pic_url": "https://cdn.trr.example/instagram/profiles/thetraitorsus.jpg",
    "hosted_profile_pic_url_hd": "https://cdn.trr.example/instagram/profiles/thetraitorsus-hd.jpg"
  },
  "freshness": {
    "first_seen_at": "2026-04-28T12:00:00Z",
    "last_seen_at": "2026-04-28T12:00:00Z",
    "last_scraped_at": "2026-04-28T12:00:00Z"
  },
  "admin_only": {
    "shared_account_source_id": "shared-source-uuid",
    "last_scrape_job_id": "scrape-job-uuid",
    "last_scrape_run_id": "scrape-run-uuid",
    "source_scope": "bravo",
    "source_shape": "web_profile_info",
    "about_fields_omitted": [
      "accounts_with_shared_followers",
      "friendship_status"
    ]
  }
}
```

Profile privacy rules:

- Stable public facts can appear in curated profile responses after source meaning is verified.
- Account-about fields default to admin-curated until the raw profile payload is inspected and semantics are stable.
- Viewer-session diagnostics such as `about.accounts_with_shared_followers` remain excluded from public/client responses.

## Relationship List Example

Route: `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/relationships?type=following&page=1&page_size=50`

```json
{
  "platform": "instagram",
  "account_handle": "thetraitorsus",
  "relationship_type": "following",
  "items": [
    {
      "relationship_type": "following",
      "owner_profile_id": "17841400000000000",
      "owner_username": "thetraitorsus",
      "related_account_id": "17841411111111111",
      "related_username": "peacock",
      "related_full_name": "Peacock",
      "related_is_private": false,
      "related_is_verified": true,
      "related_profile_pic_url": "https://instagram.example/peacock.jpg",
      "related_hosted_profile_pic_url": "https://cdn.trr.example/instagram/profiles/peacock.jpg",
      "first_seen_at": "2026-04-28T12:00:00Z",
      "last_seen_at": "2026-04-28T12:00:00Z",
      "last_scrape_run_id": "scrape-run-uuid",
      "last_scrape_job_id": "scrape-job-uuid",
      "source_page_ordinal": 1,
      "source_rank": 7
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 50,
    "total": 81,
    "total_pages": 2,
    "has_more": true,
    "next_cursor": "QVFD..."
  },
  "collection_state": {
    "requested_type": "following",
    "persisted_type": "following",
    "is_complete": false,
    "cap_status": "page_cap_reached",
    "cap_reason": "max_relationship_pages",
    "source_pages_seen": 3,
    "last_cursor_seen": "QVFD...",
    "last_collected_at": "2026-04-28T12:00:00Z"
  },
  "admin_only": {
    "rejected_rows": [
      {
        "source_type": "Followers",
        "reason": "follower_rows_out_of_scope"
      }
    ]
  }
}
```

Relationship rules:

- `type=following` is the only accepted relationship type in this plan.
- If a source payload says `Followers`, `followers`, or `Follower`, the row is skipped/rejected or stored only as private diagnostics. It must not be returned as a relationship item.
- Completeness is separate from `follows_count`; a profile can have `follows_count=81` while the stored relationship scrape is partial.

## Comment Thread Example

Route options:

- Existing list: `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/comments?post_source_id={source_id}`
- New thread detail if needed: `GET /api/v1/admin/socials/profiles/instagram/{account_handle}/comments/{comment_id}/thread`

```json
{
  "platform": "instagram",
  "account_handle": "thetraitorsus",
  "post": {
    "id": "instagram-post-row-uuid",
    "source_id": "C6Example",
    "url": "https://www.instagram.com/p/C6Example/"
  },
  "thread": {
    "id": "comment-row-uuid",
    "comment_id": "18000000000000000",
    "parent_comment_id": null,
    "text": "This breakfast scene was wild.",
    "author": {
      "username": "viewer_one",
      "user_id": "17840000000000000",
      "profile_pic_url": "https://instagram.example/viewer-one.jpg",
      "hosted_profile_pic_url": "https://cdn.trr.example/instagram/comments/viewer-one.jpg",
      "is_verified": false
    },
    "created_at": "2026-04-20T19:02:00Z",
    "likes_count": 12,
    "reply_count": 1,
    "is_reply": false,
    "media_urls": [],
    "hosted_media_urls": [],
    "replies": [
      {
        "id": "reply-row-uuid",
        "comment_id": "18000000000000001",
        "parent_comment_id": "comment-row-uuid",
        "parent_comment_external_id": "18000000000000000",
        "text": "Completely agree.",
        "author": {
          "username": "viewer_two",
          "user_id": "17840000000000001",
          "profile_pic_url": "https://instagram.example/viewer-two.jpg",
          "hosted_profile_pic_url": "https://cdn.trr.example/instagram/comments/viewer-two.jpg",
          "is_verified": false
        },
        "created_at": "2026-04-20T19:04:00Z",
        "likes_count": 2,
        "reply_count": 0,
        "is_reply": true,
        "media_urls": [],
        "hosted_media_urls": []
      }
    ]
  },
  "pagination": {
    "page": 1,
    "page_size": 25,
    "total": 1,
    "total_pages": 1,
    "has_more": false,
    "next_cursor": null
  },
  "coverage": {
    "source": "full_comments_scrape",
    "reported_comments": 86,
    "saved_comments": 54,
    "is_complete": false,
    "last_comments_run_id": "scrape-run-uuid",
    "last_comments_run_status": "completed"
  },
  "admin_only": {
    "media_mirror_status": "not_required",
    "lifecycle_state": {
      "is_missing": false,
      "last_seen_at": "2026-04-28T12:00:00Z",
      "last_seen_run_id": "scrape-run-uuid"
    }
  }
}
```

Comment rules:

- Comments and replies must come from full comments scrape rows.
- Replies are either nested under `replies[]` or returned with enough `parent_comment_id`/`parent_comment_external_id` metadata for client-side threading.
- `reported_comments` comes from post metadata; `saved_comments` comes from `social.instagram_comments`.
- Embedded/latest snippets never increment `saved_comments`.

## Admin-Only Fields

These fields may appear under `admin_only` in admin routes, but should not appear in public/client response bodies:

- `last_scrape_job_id`, `last_scrape_run_id`, `last_observed_at`, `source_shape`, `source_family`
- raw diff links that require admin auth, for example `raw_observation_diff_url`
- profile/following cap state, rejected row diagnostics, source page/cursor provenance
- assignment diagnostics such as `candidate_matches` where the consumer is admin-only
- mirror diagnostics: `media_mirror_status`, `media_mirror_error`, profile/avatar mirror attempts
- comment lifecycle state: `is_missing`, `missing_at`, `last_seen_at`, `last_seen_run_id`

Admin-only still means curated diagnostics. It does not mean raw payloads are returned inline.

## Excluded Response Fields

Do not return these fields in catalog row, post detail, profile detail, relationship list, or comment thread responses:

- `raw_data`
- `raw_payload`
- `normalized_payload`
- `about_raw`
- `social_post_legacy_refs`
- complete GraphQL form data, headers, cookies, runtime tokens, or session identifiers
- raw `x-fb-lsd`, `lsd`, `__spin_r`, `__spin_b`, `__spin_t`, `hsi`, cookie values, proxy credentials, or browser session state
- `latestComments`
- `firstComment`
- embedded/XDT comment snippets
- viewer-session fields such as `has_liked`, `has_viewer_saved`, `friendship_status`, `top_likers`, and `about.accounts_with_shared_followers`

If a future admin raw diff route is added, it must be a separate admin-only route/action with explicit auth and redaction. It must not be mixed into normal read responses.

## Implementation Checklist For Phase 5

- Backend route tests cover response shape for catalog row/detail, profile detail, relationships, and comment thread/list.
- Pagination tests cover page totals and optional cursor metadata.
- Comment tests prove author avatar, author verification, likes, reply counts, timestamps, parent-child relationships, and saved coverage derive from full comment rows.
- Profile tests prove account-about fields, external links, profile picture variants, and counts are curated and raw diagnostics are omitted.
- Relationship tests prove only following rows are returned and follower-mode rows are rejected/skipped.
- Contract tests prove `latestComments`, `firstComment`, and embedded snippet counts are absent from public/admin examples.
