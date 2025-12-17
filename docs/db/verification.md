# Database verification (local Supabase)

This doc validates that `supabase/migrations/0001_init.sql`, `supabase/seed.sql`, and RLS behave correctly on a fresh local reset.

## Prereqs

- Docker engine running
- Supabase CLI installed (`supabase --version`)
- `psql` available

## Task A: Reset/apply migrations + seed

Start only the database container (faster, and avoids pulling the full local stack):

```bash
supabase start --exclude gotrue,realtime,storage-api,imgproxy,kong,mailpit,postgrest,postgres-meta,studio,edge-runtime,logflare,vector,supavisor
```

Reset the DB and apply migrations + seed:

```bash
supabase db reset --yes
```

Get the DB connection string:

```bash
supabase status --output env
```

Expected: output includes applying `0001_init.sql` and seeding `supabase/seed.sql` without errors.

## Expected seed row counts

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 -c "
select 'core.shows' as table_name, count(*) as rows from core.shows union all
select 'core.seasons', count(*) from core.seasons union all
select 'core.episodes', count(*) from core.episodes union all
select 'core.people', count(*) from core.people union all
select 'core.cast_memberships', count(*) from core.cast_memberships union all
select 'core.episode_cast', count(*) from core.episode_cast union all
select 'surveys.surveys', count(*) from surveys.surveys union all
select 'surveys.questions', count(*) from surveys.questions union all
select 'surveys.options', count(*) from surveys.options union all
select 'surveys.aggregates', count(*) from surveys.aggregates union all
select 'games.games', count(*) from games.games union all
select 'games.questions', count(*) from games.questions union all
select 'games.options', count(*) from games.options union all
select 'games.answer_keys', count(*) from games.answer_keys union all
select 'games.stats', count(*) from games.stats;"
```

Expected:

```
 core.shows            | 1
 core.seasons          | 1
 core.episodes         | 2
 core.people           | 5
 core.cast_memberships | 5
 core.episode_cast     | 10
 surveys.surveys       | 1
 surveys.questions     | 3
 surveys.options       | 8
 surveys.aggregates    | 3
 games.games           | 1
 games.questions       | 2
 games.options         | 8
 games.answer_keys     | 2
 games.stats           | 3
```

## Task B: RLS behavior

Notes:

- `anon`, `authenticated`, and `service_role` are database roles that do not have login. For verification, connect as `postgres` and use `set role ...`.
- When using `auth.uid()` in psql, prefer `set_config(..., false)` so the value persists across statements in the session.

### Public read works (anon)

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 -c "
set role anon;
select 'core.shows' as table_name, count(*) as rows from core.shows union all
select 'games.games', count(*) from games.games union all
select 'games.questions', count(*) from games.questions union all
select 'games.options', count(*) from games.options union all
select 'games.stats', count(*) from games.stats union all
select 'surveys.surveys', count(*) from surveys.surveys union all
select 'surveys.questions', count(*) from surveys.questions union all
select 'surveys.options', count(*) from surveys.options union all
select 'surveys.aggregates', count(*) from surveys.aggregates;"
```

Expected: non-zero counts matching the seed (above).

### User-scoped read/write works (authenticated)

Create two test users in `auth.users` (needed for FKs on user-owned tables):

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 -c "
insert into auth.users (id) values
  ('c60f22e1-c949-4e9a-b0be-944d410b1069'),
  ('2eb7469b-7aab-4160-abbd-7d5ee5ea5839')
on conflict (id) do nothing;"
```

As user 1, create a survey response + answers (same `survey_id` enforced by composite FKs):

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 <<'SQL'
set role authenticated;
select set_config('request.jwt.claim.sub','c60f22e1-c949-4e9a-b0be-944d410b1069', false);

insert into surveys.responses (id, survey_id, submitted_at)
values (
  '4c43af78-7de9-43f7-bde8-22d00d1c57ee',
  '8a24c95d-93bc-4297-9c84-7946b753eb2d',
  now()
);

insert into surveys.answers (survey_id, response_id, question_id, answer)
values
  (
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    '4c43af78-7de9-43f7-bde8-22d00d1c57ee',
    'c5106823-7875-43d9-9172-4fbaa076a2b9',
    '{"selected_option_id":"e45aa52e-5d8f-467e-a3fd-7bad3df8f2e3"}'::jsonb
  ),
  (
    '8a24c95d-93bc-4297-9c84-7946b753eb2d',
    '4c43af78-7de9-43f7-bde8-22d00d1c57ee',
    'bfc0b968-7d5a-4826-a57d-82c9ac872226',
    '{"text":"Great episode"}'::jsonb
  );
SQL
```

As user 2, confirm they cannot see user 1 data:

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 -c "
set role authenticated;
select set_config('request.jwt.claim.sub','2eb7469b-7aab-4160-abbd-7d5ee5ea5839', false);
select count(*) as responses_visible from surveys.responses;
select count(*) as answers_visible from surveys.answers;"
```

Expected:

```
responses_visible | 0
answers_visible   | 0
```

Games (sessions + responses) work the same way:

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 <<'SQL'
set role authenticated;
select set_config('request.jwt.claim.sub','c60f22e1-c949-4e9a-b0be-944d410b1069', false);

insert into games.sessions (id, game_id)
values (
  '5938115f-347a-41db-b0ec-b9438592a782',
  '44c17a72-6264-496c-ad0f-24e374e2ba39'
);

insert into games.responses (game_id, session_id, question_id, answer)
values (
  '44c17a72-6264-496c-ad0f-24e374e2ba39',
  '5938115f-347a-41db-b0ec-b9438592a782',
  '5925bde7-82c9-4b63-acfa-2c7757b2182c',
  '{"selected_option_id":"65eeaefb-9c3c-4c56-9f9d-a1481f3fc8f4"}'::jsonb
);
SQL
```

### Aggregates/stats are read-only to clients

Authenticated users cannot write `surveys.aggregates` or `games.stats`:

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 <<'SQL'
set role authenticated;
select set_config('request.jwt.claim.sub','c60f22e1-c949-4e9a-b0be-944d410b1069', false);

insert into surveys.aggregates (survey_id, question_id, aggregate)
values ('8a24c95d-93bc-4297-9c84-7946b753eb2d','c5106823-7875-43d9-9172-4fbaa076a2b9','{"total":999}'::jsonb);
SQL
```

Expected: `ERROR: permission denied for table aggregates`

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 <<'SQL'
set role authenticated;
select set_config('request.jwt.claim.sub','c60f22e1-c949-4e9a-b0be-944d410b1069', false);

insert into games.stats (game_id, question_id, stats)
values ('44c17a72-6264-496c-ad0f-24e374e2ba39', null, '{"total":999}'::jsonb);
SQL
```

Expected: `ERROR: permission denied for table stats`

### Service role bypasses RLS

```bash
PGPASSWORD=postgres psql "postgresql://postgres@127.0.0.1:55432/postgres" -v ON_ERROR_STOP=1 -c "
set role service_role;
select count(*) as survey_responses_all from surveys.responses;
select count(*) as survey_answers_all from surveys.answers;
select count(*) as game_sessions_all from games.sessions;
select count(*) as game_responses_all from games.responses;"
```

Expected: counts include rows written by authenticated users (>= 1).

## Cleanup

```bash
supabase stop --no-backup
```
