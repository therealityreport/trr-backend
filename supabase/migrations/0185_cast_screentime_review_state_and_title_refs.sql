create table if not exists screenalytics.cast_screentime_suggestion_decisions (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid null references core.seasons(id) on delete set null,
  episode_id uuid null references core.episodes(id) on delete set null,
  owner_scope text not null check (owner_scope in ('show', 'season', 'episode')),
  owner_entity_id uuid not null,
  video_asset_id uuid not null references screenalytics.video_assets(id) on delete cascade,
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  suggestion_key text not null,
  person_id uuid not null references core.people(id) on delete cascade,
  decision text not null check (decision in ('accept', 'reject', 'defer')),
  notes_json jsonb not null default '{}'::jsonb,
  suggestion_payload jsonb not null default '{}'::jsonb,
  decided_by text null,
  decided_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists cast_screentime_suggestion_decisions_scope_person_idx
  on screenalytics.cast_screentime_suggestion_decisions (owner_scope, owner_entity_id, person_id);

create index if not exists cast_screentime_suggestion_decisions_run_idx
  on screenalytics.cast_screentime_suggestion_decisions (run_id, decided_at desc);

create index if not exists cast_screentime_suggestion_decisions_show_idx
  on screenalytics.cast_screentime_suggestion_decisions (show_id, decided_at desc);

create table if not exists screenalytics.cast_screentime_unknown_review_state (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid null references core.seasons(id) on delete set null,
  episode_id uuid null references core.episodes(id) on delete set null,
  owner_scope text not null check (owner_scope in ('show', 'season', 'episode')),
  owner_entity_id uuid not null,
  video_asset_id uuid not null references screenalytics.video_assets(id) on delete cascade,
  run_id uuid not null references screenalytics.runs_v2(id) on delete cascade,
  queue_key text not null,
  queue_group text not null,
  candidate_person_id uuid null references core.people(id) on delete set null,
  decision text not null check (decision in ('accept', 'reject', 'defer')),
  escalation_level text not null check (escalation_level in ('episode', 'season', 'show')),
  recommended_action text not null,
  notes_json jsonb not null default '{}'::jsonb,
  queue_payload jsonb not null default '{}'::jsonb,
  decided_by text null,
  decided_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists cast_screentime_unknown_review_scope_group_idx
  on screenalytics.cast_screentime_unknown_review_state (owner_scope, owner_entity_id, queue_group);

create index if not exists cast_screentime_unknown_review_run_idx
  on screenalytics.cast_screentime_unknown_review_state (run_id, decided_at desc);

create index if not exists cast_screentime_unknown_review_show_idx
  on screenalytics.cast_screentime_unknown_review_state (show_id, decided_at desc);

do $$
begin
  if to_regprocedure('core.set_updated_at()') is not null then
    drop trigger if exists screenalytics_cast_suggestion_decisions_set_updated_at on screenalytics.cast_screentime_suggestion_decisions;
    create trigger screenalytics_cast_suggestion_decisions_set_updated_at
    before update on screenalytics.cast_screentime_suggestion_decisions
    for each row execute function core.set_updated_at();

    drop trigger if exists screenalytics_unknown_review_state_set_updated_at on screenalytics.cast_screentime_unknown_review_state;
    create trigger screenalytics_unknown_review_state_set_updated_at
    before update on screenalytics.cast_screentime_unknown_review_state
    for each row execute function core.set_updated_at();
  end if;
end $$;
