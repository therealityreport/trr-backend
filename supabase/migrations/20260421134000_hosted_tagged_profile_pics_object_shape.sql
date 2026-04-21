begin;

comment on column social.instagram_posts.hosted_tagged_profile_pics is
  'Supports both legacy string values and object values with hosted_url, sha256, mirrored_at.';

commit;
