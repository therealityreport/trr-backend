begin;

-- Baseline TRR backend schema for Supabase Postgres
-- Schemas: core (shows/cast), games (interactive games), surveys (surveys + live aggregates)

create extension if not exists pgcrypto;

create schema if not exists core;
create schema if not exists games;
create schema if not exists surveys;

-- ---------------------------------------------------------------------------
-- core
-- ---------------------------------------------------------------------------

create table core.shows (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  description text,
  premiere_date date,
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table core.seasons (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  season_number integer not null check (season_number > 0),
  title text,
  premiere_date date,
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint seasons_show_id_season_number_unique unique (show_id, season_number)
);

create index seasons_show_id_idx on core.seasons (show_id);

create table core.episodes (
  id uuid primary key default gen_random_uuid(),
  season_id uuid not null references core.seasons (id) on delete cascade,
  episode_number integer not null check (episode_number > 0),
  title text,
  air_date date,
  synopsis text,
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint episodes_season_id_episode_number_unique unique (season_id, episode_number)
);

create index episodes_season_id_idx on core.episodes (season_id);

create table core.people (
  id uuid primary key default gen_random_uuid(),
  full_name text not null,
  known_for text,
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index people_full_name_idx on core.people (full_name);

create table core.cast_memberships (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  season_id uuid references core.seasons (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  role text not null default 'cast',
  billing_order integer,
  notes text,
  created_at timestamptz not null default now()
);

create index cast_memberships_show_id_idx on core.cast_memberships (show_id);
create index cast_memberships_season_id_idx on core.cast_memberships (season_id);
create index cast_memberships_person_id_idx on core.cast_memberships (person_id);

create unique index cast_memberships_show_person_role_no_season_unique_idx
on core.cast_memberships (show_id, person_id, role)
where season_id is null;

create unique index cast_memberships_show_season_person_role_unique_idx
on core.cast_memberships (show_id, season_id, person_id, role)
where season_id is not null;

create table core.episode_cast (
  id uuid primary key default gen_random_uuid(),
  episode_id uuid not null references core.episodes (id) on delete cascade,
  cast_membership_id uuid not null references core.cast_memberships (id) on delete cascade,
  appearance_type text,
  created_at timestamptz not null default now(),
  constraint episode_cast_episode_id_membership_unique unique (episode_id, cast_membership_id)
);

create index episode_cast_episode_id_idx on core.episode_cast (episode_id);
create index episode_cast_cast_membership_id_idx on core.episode_cast (cast_membership_id);

-- ---------------------------------------------------------------------------
-- games
-- ---------------------------------------------------------------------------

create table games.games (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  season_id uuid references core.seasons (id) on delete set null,
  episode_id uuid references core.episodes (id) on delete set null,
  game_type text not null check (game_type in ('quiz', 'poll', 'prediction', 'ranking')),
  title text not null,
  description text,
  status text not null default 'draft' check (status in ('draft', 'published', 'archived')),
  starts_at timestamptz,
  ends_at timestamptz,
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint games_episode_requires_season check (episode_id is null or season_id is not null),
  constraint games_ends_after_starts check (ends_at is null or starts_at is null or ends_at >= starts_at)
);

create index games_show_id_idx on games.games (show_id);
create index games_season_id_idx on games.games (season_id);
create index games_episode_id_idx on games.games (episode_id);

create table games.questions (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references games.games (id) on delete cascade,
  question_order integer not null check (question_order > 0),
  prompt text not null,
  question_type text not null check (question_type in ('single_choice', 'multiple_choice', 'ranking', 'free_text', 'numeric')),
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint game_questions_game_id_order_unique unique (game_id, question_order),
  constraint game_questions_game_id_id_unique unique (game_id, id)
);

create index game_questions_game_id_idx on games.questions (game_id);

create table games.options (
  id uuid primary key default gen_random_uuid(),
  question_id uuid not null references games.questions (id) on delete cascade,
  option_order integer not null check (option_order > 0),
  label text not null,
  value text,
  created_at timestamptz not null default now(),
  constraint game_options_question_id_order_unique unique (question_id, option_order)
);

create index game_options_question_id_idx on games.options (question_id);

create table games.answer_keys (
  id uuid primary key default gen_random_uuid(),
  question_id uuid not null references games.questions (id) on delete cascade,
  answer jsonb not null,
  explanation text,
  created_at timestamptz not null default now(),
  constraint game_answer_keys_question_id_unique unique (question_id)
);

create index game_answer_keys_question_id_idx on games.answer_keys (question_id);

create table games.sessions (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references games.games (id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users (id) on delete cascade,
  status text not null default 'in_progress' check (status in ('in_progress', 'submitted', 'abandoned')),
  started_at timestamptz not null default now(),
  submitted_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint game_sessions_submitted_after_started check (submitted_at is null or submitted_at >= started_at),
  constraint game_sessions_game_id_id_unique unique (game_id, id)
);

create index game_sessions_game_id_idx on games.sessions (game_id);
create index game_sessions_user_id_idx on games.sessions (user_id);

create table games.responses (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references games.games (id) on delete cascade,
  session_id uuid not null,
  question_id uuid not null,
  answer jsonb not null,
  created_at timestamptz not null default now(),
  constraint game_responses_session_id_question_id_unique unique (session_id, question_id),
  constraint game_responses_game_id_session_id_fkey foreign key (game_id, session_id)
    references games.sessions (game_id, id) on delete cascade,
  constraint game_responses_game_id_question_id_fkey foreign key (game_id, question_id)
    references games.questions (game_id, id) on delete cascade
);

create index game_responses_game_id_idx on games.responses (game_id);
create index game_responses_session_id_idx on games.responses (session_id);
create index game_responses_question_id_idx on games.responses (question_id);

create table games.stats (
  id uuid primary key default gen_random_uuid(),
  game_id uuid not null references games.games (id) on delete cascade,
  question_id uuid references games.questions (id) on delete cascade,
  stats jsonb not null,
  computed_at timestamptz not null default now()
);

create unique index game_stats_game_id_no_question_unique_idx
on games.stats (game_id)
where question_id is null;

create unique index game_stats_game_id_question_unique_idx
on games.stats (game_id, question_id)
where question_id is not null;

create index game_stats_game_id_idx on games.stats (game_id);
create index game_stats_question_id_idx on games.stats (question_id);

-- ---------------------------------------------------------------------------
-- surveys
-- ---------------------------------------------------------------------------

create table surveys.surveys (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows (id) on delete cascade,
  season_id uuid references core.seasons (id) on delete set null,
  episode_id uuid references core.episodes (id) on delete set null,
  title text not null,
  description text,
  status text not null default 'draft' check (status in ('draft', 'published', 'archived')),
  starts_at timestamptz,
  ends_at timestamptz,
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint surveys_episode_requires_season check (episode_id is null or season_id is not null),
  constraint surveys_ends_after_starts check (ends_at is null or starts_at is null or ends_at >= starts_at)
);

create index surveys_show_id_idx on surveys.surveys (show_id);
create index surveys_season_id_idx on surveys.surveys (season_id);
create index surveys_episode_id_idx on surveys.surveys (episode_id);

create table surveys.questions (
  id uuid primary key default gen_random_uuid(),
  survey_id uuid not null references surveys.surveys (id) on delete cascade,
  question_order integer not null check (question_order > 0),
  prompt text not null,
  question_type text not null check (question_type in ('single_choice', 'multiple_choice', 'free_text', 'numeric')),
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint survey_questions_survey_id_order_unique unique (survey_id, question_order),
  constraint survey_questions_survey_id_id_unique unique (survey_id, id)
);

create index survey_questions_survey_id_idx on surveys.questions (survey_id);

create table surveys.options (
  id uuid primary key default gen_random_uuid(),
  question_id uuid not null references surveys.questions (id) on delete cascade,
  option_order integer not null check (option_order > 0),
  label text not null,
  value text,
  created_at timestamptz not null default now(),
  constraint survey_options_question_id_order_unique unique (question_id, option_order)
);

create index survey_options_question_id_idx on surveys.options (question_id);

create table surveys.responses (
  id uuid primary key default gen_random_uuid(),
  survey_id uuid not null references surveys.surveys (id) on delete cascade,
  user_id uuid not null default auth.uid() references auth.users (id) on delete cascade,
  submitted_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint survey_responses_survey_id_id_unique unique (survey_id, id)
);

create index survey_responses_survey_id_idx on surveys.responses (survey_id);
create index survey_responses_user_id_idx on surveys.responses (user_id);

create table surveys.answers (
  id uuid primary key default gen_random_uuid(),
  survey_id uuid not null references surveys.surveys (id) on delete cascade,
  response_id uuid not null,
  question_id uuid not null,
  answer jsonb not null,
  created_at timestamptz not null default now(),
  constraint survey_answers_response_id_question_id_unique unique (response_id, question_id),
  constraint survey_answers_survey_id_response_id_fkey foreign key (survey_id, response_id)
    references surveys.responses (survey_id, id) on delete cascade,
  constraint survey_answers_survey_id_question_id_fkey foreign key (survey_id, question_id)
    references surveys.questions (survey_id, id) on delete cascade
);

create index survey_answers_survey_id_idx on surveys.answers (survey_id);
create index survey_answers_response_id_idx on surveys.answers (response_id);
create index survey_answers_question_id_idx on surveys.answers (question_id);

create table surveys.aggregates (
  id uuid primary key default gen_random_uuid(),
  survey_id uuid not null references surveys.surveys (id) on delete cascade,
  question_id uuid not null references surveys.questions (id) on delete cascade,
  aggregate jsonb not null,
  updated_at timestamptz not null default now(),
  constraint survey_aggregates_survey_id_question_id_unique unique (survey_id, question_id)
);

create index survey_aggregates_survey_id_idx on surveys.aggregates (survey_id);
create index survey_aggregates_question_id_idx on surveys.aggregates (question_id);
create index survey_aggregates_survey_id_question_id_idx on surveys.aggregates (survey_id, question_id);

-- ---------------------------------------------------------------------------
-- Grants (privileges) for Supabase API roles
-- ---------------------------------------------------------------------------

grant usage on schema core to anon, authenticated, service_role;
grant usage on schema games to anon, authenticated, service_role;
grant usage on schema surveys to anon, authenticated, service_role;

-- Public read
grant select on table
  core.shows,
  core.seasons,
  core.episodes,
  core.people,
  core.cast_memberships,
  core.episode_cast,
  games.games,
  games.questions,
  games.options,
  surveys.surveys,
  surveys.questions,
  surveys.options,
  games.stats,
  surveys.aggregates
to anon, authenticated;

-- User-scoped write
grant select, insert, update, delete on table
  games.sessions,
  games.responses,
  surveys.responses,
  surveys.answers
to authenticated;

-- Service role can manage everything (RLS bypass)
grant all privileges on all tables in schema core to service_role;
grant all privileges on all tables in schema games to service_role;
grant all privileges on all tables in schema surveys to service_role;

-- ---------------------------------------------------------------------------
-- Row Level Security (RLS)
-- ---------------------------------------------------------------------------

alter table core.shows enable row level security;
alter table core.seasons enable row level security;
alter table core.episodes enable row level security;
alter table core.people enable row level security;
alter table core.cast_memberships enable row level security;
alter table core.episode_cast enable row level security;

alter table games.games enable row level security;
alter table games.questions enable row level security;
alter table games.options enable row level security;
alter table games.answer_keys enable row level security;
alter table games.sessions enable row level security;
alter table games.responses enable row level security;
alter table games.stats enable row level security;

alter table surveys.surveys enable row level security;
alter table surveys.questions enable row level security;
alter table surveys.options enable row level security;
alter table surveys.responses enable row level security;
alter table surveys.answers enable row level security;
alter table surveys.aggregates enable row level security;

-- Public read policies
create policy core_shows_public_read on core.shows
for select to anon, authenticated
using (true);

create policy core_seasons_public_read on core.seasons
for select to anon, authenticated
using (true);

create policy core_episodes_public_read on core.episodes
for select to anon, authenticated
using (true);

create policy core_people_public_read on core.people
for select to anon, authenticated
using (true);

create policy core_cast_memberships_public_read on core.cast_memberships
for select to anon, authenticated
using (true);

create policy core_episode_cast_public_read on core.episode_cast
for select to anon, authenticated
using (true);

create policy games_games_public_read on games.games
for select to anon, authenticated
using (true);

create policy games_questions_public_read on games.questions
for select to anon, authenticated
using (true);

create policy games_options_public_read on games.options
for select to anon, authenticated
using (true);

create policy surveys_surveys_public_read on surveys.surveys
for select to anon, authenticated
using (true);

create policy surveys_questions_public_read on surveys.questions
for select to anon, authenticated
using (true);

create policy surveys_options_public_read on surveys.options
for select to anon, authenticated
using (true);

-- Read-only computed tables
create policy games_stats_public_read on games.stats
for select to anon, authenticated
using (true);

create policy surveys_aggregates_public_read on surveys.aggregates
for select to anon, authenticated
using (true);

-- User-scoped sessions/responses
create policy game_sessions_select_own on games.sessions
for select to authenticated
using (user_id = auth.uid());

create policy game_sessions_insert_own on games.sessions
for insert to authenticated
with check (user_id = auth.uid());

create policy game_sessions_update_own on games.sessions
for update to authenticated
using (user_id = auth.uid())
with check (user_id = auth.uid());

create policy game_sessions_delete_own on games.sessions
for delete to authenticated
using (user_id = auth.uid());

create policy game_responses_select_own on games.responses
for select to authenticated
using (
  exists (
    select 1
    from games.sessions s
    where s.id = games.responses.session_id
      and s.user_id = auth.uid()
  )
);

create policy game_responses_insert_own on games.responses
for insert to authenticated
with check (
  exists (
    select 1
    from games.sessions s
    where s.id = games.responses.session_id
      and s.user_id = auth.uid()
  )
);

create policy game_responses_update_own on games.responses
for update to authenticated
using (
  exists (
    select 1
    from games.sessions s
    where s.id = games.responses.session_id
      and s.user_id = auth.uid()
  )
)
with check (
  exists (
    select 1
    from games.sessions s
    where s.id = games.responses.session_id
      and s.user_id = auth.uid()
  )
);

create policy game_responses_delete_own on games.responses
for delete to authenticated
using (
  exists (
    select 1
    from games.sessions s
    where s.id = games.responses.session_id
      and s.user_id = auth.uid()
  )
);

create policy survey_responses_select_own on surveys.responses
for select to authenticated
using (user_id = auth.uid());

create policy survey_responses_insert_own on surveys.responses
for insert to authenticated
with check (user_id = auth.uid());

create policy survey_responses_update_own on surveys.responses
for update to authenticated
using (user_id = auth.uid())
with check (user_id = auth.uid());

create policy survey_responses_delete_own on surveys.responses
for delete to authenticated
using (user_id = auth.uid());

create policy survey_answers_select_own on surveys.answers
for select to authenticated
using (
  exists (
    select 1
    from surveys.responses r
    where r.id = surveys.answers.response_id
      and r.user_id = auth.uid()
  )
);

create policy survey_answers_insert_own on surveys.answers
for insert to authenticated
with check (
  exists (
    select 1
    from surveys.responses r
    where r.id = surveys.answers.response_id
      and r.user_id = auth.uid()
  )
);

create policy survey_answers_update_own on surveys.answers
for update to authenticated
using (
  exists (
    select 1
    from surveys.responses r
    where r.id = surveys.answers.response_id
      and r.user_id = auth.uid()
  )
)
with check (
  exists (
    select 1
    from surveys.responses r
    where r.id = surveys.answers.response_id
      and r.user_id = auth.uid()
  )
);

create policy survey_answers_delete_own on surveys.answers
for delete to authenticated
using (
  exists (
    select 1
    from surveys.responses r
    where r.id = surveys.answers.response_id
      and r.user_id = auth.uid()
  )
);

commit;
