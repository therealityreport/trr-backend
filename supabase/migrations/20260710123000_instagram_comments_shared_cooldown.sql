begin;

alter table if exists social.ig_comment_rate_pace
  add column if not exists cooldown_until timestamptz;

-- Clear only capacity failures that were previously misclassified as Instagram
-- authentication blocks. Checkpoints and scrape-job history are untouched.
update social.account_auth_cooldown
set cooldown_until = null,
    consecutive_auth_failures = 0,
    last_error_code = null,
    blocker_kind = 'auth',
    updated_at = now()
where lower(btrim(platform)) = 'instagram'
  and lower(btrim(blocker_kind)) = 'auth'
  and lower(btrim(last_error_code)) = 'database_capacity';

commit;

