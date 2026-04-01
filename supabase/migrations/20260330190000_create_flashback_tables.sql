-- Flashback game tables — quizzes, events, sessions, and user stats.

create table if not exists public.flashback_quizzes (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  publish_date date not null,
  description text,
  is_published boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.flashback_events (
  id uuid primary key default gen_random_uuid(),
  quiz_id uuid not null references public.flashback_quizzes(id) on delete cascade,
  description text not null,
  image_url text,
  year integer not null,
  sort_order integer not null,
  point_value integer not null default 2
);

create table if not exists public.flashback_sessions (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  quiz_id uuid not null references public.flashback_quizzes(id) on delete cascade,
  current_round integer not null default 0,
  score integer not null default 0,
  placements jsonb not null default '[]'::jsonb,
  completed boolean not null default false,
  started_at timestamptz not null default now(),
  completed_at timestamptz,
  unique (user_id, quiz_id)
);

create table if not exists public.flashback_user_stats (
  user_id text primary key,
  games_played integer not null default 0,
  total_points integer not null default 0,
  perfect_scores integer not null default 0,
  current_streak integer not null default 0,
  max_streak integer not null default 0,
  updated_at timestamptz not null default now()
);

-- RLS
alter table public.flashback_quizzes enable row level security;
alter table public.flashback_events enable row level security;
alter table public.flashback_sessions enable row level security;
alter table public.flashback_user_stats enable row level security;

-- Read policies (public read for published quizzes/events)
create policy "anyone_read_published_quizzes" on public.flashback_quizzes
  for select using (is_published = true);
create policy "anyone_read_events" on public.flashback_events
  for select using (true);
create policy "own_sessions" on public.flashback_sessions
  for all using (true);
create policy "own_stats" on public.flashback_user_stats
  for all using (true);

-- Grants
grant select on public.flashback_quizzes to anon, authenticated, service_role;
grant select on public.flashback_events to anon, authenticated, service_role;
grant all on public.flashback_sessions to anon, authenticated, service_role;
grant all on public.flashback_user_stats to anon, authenticated, service_role;
