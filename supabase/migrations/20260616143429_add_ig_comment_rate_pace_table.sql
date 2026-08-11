create table if not exists social.ig_comment_rate_pace (
  rate_key text primary key,
  last_start timestamptz not null default now()
);
comment on table social.ig_comment_rate_pace is
  'IG comments lane cross-container request pacing: one row per global_rate_limit_key. last_start = most recently claimed request slot (DB clock). Updated via atomic upsert reservation in fetcher._try_advisory_lock_pace; replaces advisory-lock-with-in-lock-sleep.';