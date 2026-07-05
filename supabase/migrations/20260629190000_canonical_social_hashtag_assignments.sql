create schema if not exists social;

create table if not exists social.hashtag_assignments (
  id uuid primary key default gen_random_uuid(),
  normalized_hashtag text not null,
  display_hashtag text not null,
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists hashtag_assignments_unique_idx
  on social.hashtag_assignments (
    normalized_hashtag,
    show_id,
    coalesce(season_id, '00000000-0000-0000-0000-000000000000'::uuid)
  );

create index if not exists hashtag_assignments_lookup_idx
  on social.hashtag_assignments (normalized_hashtag);

create index if not exists hashtag_assignments_show_idx
  on social.hashtag_assignments (show_id, season_id);

create table if not exists social.hashtag_assignment_backfill_conflicts (
  id uuid primary key default gen_random_uuid(),
  normalized_hashtag text not null,
  display_hashtag text not null,
  distinct_show_count integer not null,
  legacy_assignments jsonb not null,
  resolution_action text not null default 'merged_global_show_set',
  resolved_at timestamptz not null default now()
);

create unique index if not exists hashtag_assignment_backfill_conflicts_unique_idx
  on social.hashtag_assignment_backfill_conflicts (normalized_hashtag);

insert into social.hashtag_assignments (
  normalized_hashtag,
  display_hashtag,
  show_id,
  season_id,
  updated_by,
  created_at,
  updated_at
)
select
  a.normalized_hashtag,
  coalesce(nullif(min(a.display_hashtag), ''), '#' || a.normalized_hashtag) as display_hashtag,
  a.show_id,
  a.season_id,
  coalesce(nullif(max(a.updated_by), ''), 'migration:legacy-hashtag-global') as updated_by,
  min(a.created_at) as created_at,
  max(a.updated_at) as updated_at
from social.account_hashtag_assignments a
group by a.normalized_hashtag, a.show_id, a.season_id
having not exists (
  select 1
  from social.hashtag_assignments existing
  where existing.normalized_hashtag = a.normalized_hashtag
    and existing.show_id = a.show_id
    and coalesce(existing.season_id, '00000000-0000-0000-0000-000000000000'::uuid)
      = coalesce(a.season_id, '00000000-0000-0000-0000-000000000000'::uuid)
);

insert into social.hashtag_assignment_backfill_conflicts (
  normalized_hashtag,
  display_hashtag,
  distinct_show_count,
  legacy_assignments,
  resolution_action
)
select
  conflicts.normalized_hashtag,
  conflicts.display_hashtag,
  conflicts.distinct_show_count,
  conflicts.legacy_assignments,
  'merged_global_show_set'::text as resolution_action
from (
  select
    a.normalized_hashtag,
    coalesce(nullif(min(a.display_hashtag), ''), '#' || a.normalized_hashtag) as display_hashtag,
    count(distinct a.show_id) as distinct_show_count,
    jsonb_agg(
      distinct jsonb_build_object(
        'platform', a.platform,
        'account_handle', a.account_handle,
        'show_id', a.show_id::text,
        'season_id', a.season_id::text,
        'display_hashtag', a.display_hashtag
      )
    ) as legacy_assignments
  from social.account_hashtag_assignments a
  group by a.normalized_hashtag
  having count(distinct a.show_id) > 1
) conflicts
where not exists (
  select 1
  from social.hashtag_assignment_backfill_conflicts existing
  where existing.normalized_hashtag = conflicts.normalized_hashtag
);

delete from social.hashtag_assignments a
using social.hashtag_assignments newer
where a.id <> newer.id
  and a.normalized_hashtag = newer.normalized_hashtag
  and a.show_id = newer.show_id
  and coalesce(a.season_id, '00000000-0000-0000-0000-000000000000'::uuid)
    = coalesce(newer.season_id, '00000000-0000-0000-0000-000000000000'::uuid)
  and a.created_at < newer.created_at;

alter table social.hashtag_assignments enable row level security;
alter table social.hashtag_assignment_backfill_conflicts enable row level security;

grant all privileges on table social.hashtag_assignments to service_role;
grant all privileges on table social.hashtag_assignment_backfill_conflicts to service_role;
revoke all on table social.hashtag_assignments from anon, authenticated;
revoke all on table social.hashtag_assignment_backfill_conflicts from anon, authenticated;

drop policy if exists hashtag_assignments_service_role_all
  on social.hashtag_assignments;
create policy hashtag_assignments_service_role_all
on social.hashtag_assignments
for all
to service_role
using (true)
with check (true);

drop policy if exists hashtag_assignment_backfill_conflicts_service_role_all
  on social.hashtag_assignment_backfill_conflicts;
create policy hashtag_assignment_backfill_conflicts_service_role_all
on social.hashtag_assignment_backfill_conflicts
for all
to service_role
using (true)
with check (true);
