begin;

create table if not exists social.instagram_profile_pagination_state (
  id uuid primary key default gen_random_uuid(),
  run_id uuid,
  job_id text,
  account_handle text not null,
  source_scope text not null default 'bravo',
  direction text not null default 'forward',
  cursor_in text,
  end_cursor text,
  page_index integer not null default 0 check (page_index >= 0),
  posts_seen integer not null default 0 check (posts_seen >= 0),
  posts_upserted integer not null default 0 check (posts_upserted >= 0),
  doc_id_used text,
  doc_ids_attempted jsonb not null default '[]'::jsonb,
  proxy_fingerprint text,
  proxy_session_key text,
  stop_reason text,
  partial boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  completed_at timestamptz
);

create unique index if not exists instagram_profile_pagination_state_active_key
  on social.instagram_profile_pagination_state (
    run_id,
    lower(account_handle),
    source_scope,
    direction
  )
  where completed_at is null;

create index if not exists instagram_profile_pagination_state_account_updated_idx
  on social.instagram_profile_pagination_state (
    lower(account_handle),
    source_scope,
    updated_at desc
  );

create index if not exists instagram_profile_pagination_state_run_updated_idx
  on social.instagram_profile_pagination_state (
    run_id,
    updated_at desc
  );

comment on table social.instagram_profile_pagination_state is
  'Resumable Instagram profile-post pagination checkpoints for Backfill Posts.';

comment on column social.instagram_profile_pagination_state.stop_reason is
  'Terminal or partial reason such as timeout_guard, cursor_expired_restart_required, pagination_doc_id_stale, or completed.';

commit;
