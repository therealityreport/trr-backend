alter table if exists social.instagram_posts
  add column if not exists fb_comment_count integer,
  add column if not exists fb_like_count integer,
  add column if not exists is_shared_to_fb boolean,
  add column if not exists crosspost_metadata jsonb not null default '{}'::jsonb,
  add column if not exists social_context jsonb not null default '{}'::jsonb,
  add column if not exists facebook_post_id text,
  add column if not exists facebook_post_url text,
  add column if not exists facebook_crosspost_observed_at timestamptz,
  add column if not exists facebook_crosspost_source text;

comment on column social.instagram_posts.fb_comment_count is
  'Facebook-side comments reported by authenticated Instagram PolarisPostRootQuery. Excluded from Instagram comment scraper completeness gaps.';

comment on column social.instagram_posts.fb_like_count is
  'Facebook-side likes reported by authenticated Instagram PolarisPostRootQuery when available.';

comment on column social.instagram_posts.is_shared_to_fb is
  'Whether Instagram reports the media as shared to Facebook.';

comment on column social.instagram_posts.crosspost_metadata is
  'Raw crosspost metadata from authenticated Instagram post-root GraphQL, excluding cookies and request secrets.';

comment on column social.instagram_posts.social_context is
  'Raw social_context from authenticated Instagram post-root GraphQL, excluding cookies and request secrets.';

comment on column social.instagram_posts.facebook_post_id is
  'Facebook post identifier if Instagram crosspost metadata exposes one; otherwise null.';

comment on column social.instagram_posts.facebook_post_url is
  'Facebook post URL if Instagram crosspost metadata exposes one; otherwise null.';

comment on column social.instagram_posts.facebook_crosspost_observed_at is
  'Timestamp when Facebook crosspost counters were last observed from authenticated Instagram metadata.';

comment on column social.instagram_posts.facebook_crosspost_source is
  'Source query used for Facebook crosspost counters, normally PolarisPostRootQuery.';
