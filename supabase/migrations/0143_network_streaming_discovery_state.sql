begin;

create schema if not exists admin;

create table if not exists admin.network_streaming_discovery_state (
  entity_type text not null check (entity_type in ('network', 'streaming', 'production')),
  entity_key text not null,
  source text not null check (source in ('official', 'catalog', 'imdb', 'tmdb', 'wikimedia', 'override')),
  last_outcome text not null check (last_outcome in ('success', 'failed', 'skipped')),
  last_reason text,
  attempt_count int not null default 0,
  last_attempt_at timestamptz,
  lock_until timestamptz,
  cached_candidate_count int not null default 0,
  updated_at timestamptz not null default now(),
  primary key (entity_type, entity_key, source)
);

create index if not exists network_streaming_discovery_state_updated_idx
  on admin.network_streaming_discovery_state (updated_at desc);

grant usage on schema admin to service_role;
grant all privileges on table admin.network_streaming_discovery_state to service_role;

commit;
