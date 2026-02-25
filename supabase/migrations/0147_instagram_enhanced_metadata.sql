begin;

-- Instagram posts: rich user detail objects and additional metadata
alter table social.instagram_posts
  add column if not exists tagged_users_detail jsonb not null default '[]'::jsonb,
  add column if not exists collaborators_detail jsonb not null default '[]'::jsonb,
  add column if not exists owner_profile_pic_url text,
  add column if not exists owner_full_name text,
  add column if not exists owner_is_verified boolean,
  add column if not exists product_type text,
  add column if not exists video_play_count integer,
  add column if not exists alt_text text,
  add column if not exists width integer,
  add column if not exists height integer,
  add column if not exists is_comments_disabled boolean,
  add column if not exists music_info jsonb,
  add column if not exists video_duration numeric,
  add column if not exists child_posts_data jsonb not null default '[]'::jsonb;

-- Profile picture S3 mirror tracking
alter table social.instagram_posts
  add column if not exists hosted_owner_profile_pic_url text,
  add column if not exists hosted_tagged_profile_pics jsonb not null default '{}'::jsonb,
  add column if not exists profile_pic_mirror_status text,
  add column if not exists profile_pic_mirror_error text;

-- Instagram comments: author detail columns (dataclass already captures these)
alter table social.instagram_comments
  add column if not exists author_profile_pic_url text,
  add column if not exists author_is_verified boolean;

commit;
