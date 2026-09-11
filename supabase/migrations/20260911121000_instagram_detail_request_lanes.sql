begin;

-- Bounded sliding request window shared by every detail worker/fallback using
-- the same authenticated identity. No credentials or response bodies are stored.
create table social.instagram_detail_request_lanes (
  identity_key text primary key,
  request_times timestamptz[] not null default '{}',
  cooldown_until timestamptz,
  blocked boolean not null default false,
  blocked_fingerprint text,
  last_error_code text,
  consecutive_failures integer not null default 0,
  failure_generation bigint not null default 0,
  probe_token text,
  probe_expires_at timestamptz
);
alter table social.instagram_detail_request_lanes enable row level security;
revoke all on social.instagram_detail_request_lanes from anon, authenticated;
grant all on social.instagram_detail_request_lanes to service_role;

commit;
