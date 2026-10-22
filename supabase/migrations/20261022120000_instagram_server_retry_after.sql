-- Do not infer server waits from legacy locally synthesized cooldowns.
alter table social.instagram_detail_request_lanes
  add column if not exists server_retry_after_until timestamptz;

comment on column social.instagram_detail_request_lanes.server_retry_after_until is
  'Actual explicit server Retry-After deadline only; never a locally synthesized cooldown.';

-- The maintained legacy comment fetcher uses a separate lane table when present.
alter table if exists social.ig_comment_rate_pace
  add column if not exists server_retry_after_until timestamptz;
