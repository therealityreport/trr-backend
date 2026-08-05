begin;

-- Stable public identity aliases for show and person URLs. Seasons intentionally
-- derive identity from a canonical show alias plus core.seasons.season_number.

create table if not exists core.show_slug_aliases (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  slug text not null,
  is_canonical boolean not null default false,
  source text not null default 'manual',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint core_show_slug_aliases_slug_format_check check (
    slug = lower(btrim(slug))
    and length(slug) between 1 and 160
    and slug ~ '^[a-z0-9]+(-+[a-z0-9]+)*$'
  )
);

create table if not exists core.person_slug_aliases (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people(id) on delete cascade,
  slug text not null,
  is_canonical boolean not null default false,
  source text not null default 'manual',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint core_person_slug_aliases_slug_format_check check (
    slug = lower(btrim(slug))
    and length(slug) between 1 and 160
    and slug ~ '^[a-z0-9]+(-+[a-z0-9]+)*$'
  )
);

create unique index if not exists core_show_slug_aliases_entity_slug_uidx
  on core.show_slug_aliases (show_id, slug);
create index if not exists core_show_slug_aliases_slug_idx
  on core.show_slug_aliases (slug, show_id);
create unique index if not exists core_show_slug_aliases_one_canonical_uidx
  on core.show_slug_aliases (show_id)
  where is_canonical = true;
create unique index if not exists core_show_slug_aliases_global_canonical_uidx
  on core.show_slug_aliases (slug)
  where is_canonical = true;

create unique index if not exists core_person_slug_aliases_entity_slug_uidx
  on core.person_slug_aliases (person_id, slug);
create index if not exists core_person_slug_aliases_slug_idx
  on core.person_slug_aliases (slug, person_id);
create unique index if not exists core_person_slug_aliases_one_canonical_uidx
  on core.person_slug_aliases (person_id)
  where is_canonical = true;
create unique index if not exists core_person_slug_aliases_global_canonical_uidx
  on core.person_slug_aliases (slug)
  where is_canonical = true;

drop trigger if exists core_show_slug_aliases_set_updated_at on core.show_slug_aliases;
create trigger core_show_slug_aliases_set_updated_at
before update on core.show_slug_aliases
for each row execute function core.set_updated_at();

drop trigger if exists core_person_slug_aliases_set_updated_at on core.person_slug_aliases;
create trigger core_person_slug_aliases_set_updated_at
before update on core.person_slug_aliases
for each row execute function core.set_updated_at();

-- One normalizer owns the exact slug shape used by backfill and future-write
-- triggers. The 120-character base ceiling leaves room for `--` plus the full
-- 32-character UUID suffix while keeping every stored slug at 154 chars or less.
create or replace function core.normalize_public_identity_slug(raw_value text)
returns text
language sql
immutable
parallel safe
set search_path = pg_catalog
as $normalize_public_identity_slug$
  select nullif(
    lower(
      rtrim(
        left(
          trim(
            both '-' from regexp_replace(
              regexp_replace(coalesce(raw_value, ''), '&', ' and ', 'gi'),
              '[^a-z0-9]+',
              '-',
              'gi'
            )
          ),
          120
        ),
        '-'
      )
    ),
    ''
  );
$normalize_public_identity_slug$;

-- Seed one deterministic canonical show alias per existing show. For a shared
-- base, the lowest UUID keeps the base and every remaining show receives a full
-- UUID suffix. The base and legacy eight-character suffix remain direct aliases.
with show_bases as (
  select
    s.id as show_id,
    coalesce(
      core.normalize_public_identity_slug(s.slug),
      core.normalize_public_identity_slug(s.name),
      'show-' || replace(s.id::text, '-', '')
    ) as base_slug
  from core.shows s
),
ranked_show_bases as (
  select
    show_id,
    base_slug,
    row_number() over (partition by base_slug order by show_id) as base_rank,
    count(*) over (partition by base_slug) as collision_count
  from show_bases
)
insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
select
  show_id,
  case
    when base_rank = 1 then base_slug
    else base_slug || '--' || replace(show_id::text, '-', '')
  end,
  true,
  'migration:canonical-v2'
from ranked_show_bases
on conflict do nothing;

-- Keep explicit slugs, names, and alternative names as direct aliases.
with show_alias_values as (
  select s.id as show_id, raw_alias.value as raw_value, raw_alias.source
  from core.shows s
  cross join lateral (
    values
      (s.slug, 'legacy:slug'),
      (s.name, 'legacy:name')
  ) as raw_alias(value, source)
  union all
  select s.id, alt.name, 'legacy:alternative-name'
  from core.shows s
  cross join lateral unnest(coalesce(s.alternative_names, array[]::text[])) as alt(name)
),
normalized_show_aliases as (
  select
    show_id,
    core.normalize_public_identity_slug(raw_value) as slug,
    source
  from show_alias_values
)
insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
select show_id, slug, false, source
from normalized_show_aliases
where slug is not null
on conflict (show_id, slug) do nothing;

-- Preserve the legacy show resolver's symmetric leading-article fallback.
-- Every direct alias can therefore be requested with or without `the-` while
-- canonical-match priority and explicit 409 ambiguity remain authoritative.
with show_article_aliases as (
  select
    a.show_id,
    case
      when a.slug like 'the-%' then substring(a.slug from 5)
      else 'the-' || a.slug
    end as slug
  from core.show_slug_aliases a
)
insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
select show_id, slug, false, 'legacy:article-variant'
from show_article_aliases
where slug is not null
  and slug <> ''
  and char_length(slug) <= 160
on conflict (show_id, slug) do nothing;

with ranked_show_bases as (
  select
    s.id as show_id,
    coalesce(
      core.normalize_public_identity_slug(s.slug),
      core.normalize_public_identity_slug(s.name),
      'show-' || replace(s.id::text, '-', '')
    ) as base_slug,
    count(*) over (
      partition by coalesce(
        core.normalize_public_identity_slug(s.slug),
        core.normalize_public_identity_slug(s.name),
        'show-' || replace(s.id::text, '-', '')
      )
    ) as collision_count
  from core.shows s
)
insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
select
  show_id,
  base_slug || '--' || lower(left(show_id::text, 8)),
  false,
  'legacy:collision-prefix'
from ranked_show_bases
where collision_count > 1
on conflict (show_id, slug) do nothing;

-- Person aliases follow the same collision contract. A show context supplied
-- to the public resolver narrows colliding person aliases through show cast.
with person_bases as (
  select
    p.id as person_id,
    coalesce(
      core.normalize_public_identity_slug(p.full_name),
      'person-' || replace(p.id::text, '-', '')
    ) as base_slug
  from core.people p
),
ranked_person_bases as (
  select
    person_id,
    base_slug,
    row_number() over (partition by base_slug order by person_id) as base_rank,
    count(*) over (partition by base_slug) as collision_count
  from person_bases
)
insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
select
  person_id,
  case
    when base_rank = 1 then base_slug
    else base_slug || '--' || replace(person_id::text, '-', '')
  end,
  true,
  'migration:canonical-v2'
from ranked_person_bases
on conflict do nothing;

with person_name_aliases as (
  select
    p.id as person_id,
    core.normalize_public_identity_slug(p.full_name) as slug
  from core.people p
)
insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
select person_id, slug, false, 'legacy:full-name'
from person_name_aliases
where slug is not null
on conflict (person_id, slug) do nothing;

with ranked_person_bases as (
  select
    p.id as person_id,
    coalesce(
      core.normalize_public_identity_slug(p.full_name),
      'person-' || replace(p.id::text, '-', '')
    ) as base_slug,
    count(*) over (
      partition by coalesce(
        core.normalize_public_identity_slug(p.full_name),
        'person-' || replace(p.id::text, '-', '')
      )
    ) as collision_count
  from core.people p
)
insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
select
  person_id,
  base_slug || '--' || lower(left(person_id::text, 8)),
  false,
  'legacy:collision-prefix'
from ranked_person_bases
where collision_count > 1
on conflict (person_id, slug) do nothing;

-- Future show inserts and renames are maintained centrally. The advisory lock
-- serializes canonical allocation for one normalized base. Existing canonical
-- URLs are never rewritten; updates only add newly observed direct aliases.
create or replace function core.sync_show_public_identity_aliases()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog
as $sync_show_public_identity_aliases$
declare
  base_slug text;
  canonical_slug text;
  normalized_alias text;
  article_alias text;
  raw_alias text;
  canonical_was_created boolean := false;
begin
  base_slug := coalesce(
    core.normalize_public_identity_slug(new.slug),
    core.normalize_public_identity_slug(new.name),
    'show-' || replace(new.id::text, '-', '')
  );

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('core.show_slug_aliases:' || base_slug, 0)
  );

  select a.slug
  into canonical_slug
  from core.show_slug_aliases a
  where a.show_id = new.id
    and a.is_canonical = true;

  if canonical_slug is null then
    canonical_slug := base_slug;
    if exists (
      select 1
      from core.show_slug_aliases a
      where a.slug = canonical_slug
        and a.is_canonical = true
        and a.show_id <> new.id
    ) then
      canonical_slug := base_slug || '--' || replace(new.id::text, '-', '');
    end if;

    insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
    values (new.id, canonical_slug, true, 'trigger:canonical-v2')
    on conflict (show_id, slug) do update
      set is_canonical = true,
          updated_at = now();
    canonical_was_created := true;
  end if;

  foreach raw_alias in array array[new.slug, new.name]
  loop
    normalized_alias := core.normalize_public_identity_slug(raw_alias);
    if normalized_alias is not null then
      insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
      values (new.id, normalized_alias, false, 'trigger:show-field')
      on conflict (show_id, slug) do nothing;

      article_alias := case
        when normalized_alias like 'the-%' then substring(normalized_alias from 5)
        else 'the-' || normalized_alias
      end;
      if length(article_alias) between 1 and 160 then
        insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
        values (new.id, article_alias, false, 'trigger:article-variant')
        on conflict (show_id, slug) do nothing;
      end if;
    end if;
  end loop;

  foreach raw_alias in array coalesce(new.alternative_names, array[]::text[])
  loop
    normalized_alias := core.normalize_public_identity_slug(raw_alias);
    if normalized_alias is not null then
      insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
      values (new.id, normalized_alias, false, 'trigger:alternative-name')
      on conflict (show_id, slug) do nothing;

      article_alias := case
        when normalized_alias like 'the-%' then substring(normalized_alias from 5)
        else 'the-' || normalized_alias
      end;
      if length(article_alias) between 1 and 160 then
        insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
        values (new.id, article_alias, false, 'trigger:article-variant')
        on conflict (show_id, slug) do nothing;
      end if;
    end if;
  end loop;

  if canonical_was_created and canonical_slug <> base_slug then
    insert into core.show_slug_aliases (show_id, slug, is_canonical, source)
    values (
      new.id,
      base_slug || '--' || lower(left(new.id::text, 8)),
      false,
      'legacy:collision-prefix'
    )
    on conflict (show_id, slug) do nothing;
  end if;

  return new;
end
$sync_show_public_identity_aliases$;

create or replace function core.sync_person_public_identity_aliases()
returns trigger
language plpgsql
security definer
set search_path = pg_catalog
as $sync_person_public_identity_aliases$
declare
  base_slug text;
  canonical_slug text;
  normalized_alias text;
  canonical_was_created boolean := false;
begin
  base_slug := coalesce(
    core.normalize_public_identity_slug(new.full_name),
    'person-' || replace(new.id::text, '-', '')
  );

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('core.person_slug_aliases:' || base_slug, 0)
  );

  select a.slug
  into canonical_slug
  from core.person_slug_aliases a
  where a.person_id = new.id
    and a.is_canonical = true;

  if canonical_slug is null then
    canonical_slug := base_slug;
    if exists (
      select 1
      from core.person_slug_aliases a
      where a.slug = canonical_slug
        and a.is_canonical = true
        and a.person_id <> new.id
    ) then
      canonical_slug := base_slug || '--' || replace(new.id::text, '-', '');
    end if;

    insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
    values (new.id, canonical_slug, true, 'trigger:canonical-v2')
    on conflict (person_id, slug) do update
      set is_canonical = true,
          updated_at = now();
    canonical_was_created := true;
  end if;

  normalized_alias := core.normalize_public_identity_slug(new.full_name);
  if normalized_alias is not null then
    insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
    values (new.id, normalized_alias, false, 'trigger:full-name')
    on conflict (person_id, slug) do nothing;
  end if;

  if canonical_was_created and canonical_slug <> base_slug then
    insert into core.person_slug_aliases (person_id, slug, is_canonical, source)
    values (
      new.id,
      base_slug || '--' || lower(left(new.id::text, 8)),
      false,
      'legacy:collision-prefix'
    )
    on conflict (person_id, slug) do nothing;
  end if;

  return new;
end
$sync_person_public_identity_aliases$;

drop trigger if exists core_shows_sync_public_identity_aliases on core.shows;
create trigger core_shows_sync_public_identity_aliases
after insert or update of slug, name, alternative_names on core.shows
for each row execute function core.sync_show_public_identity_aliases();

drop trigger if exists core_people_sync_public_identity_aliases on core.people;
create trigger core_people_sync_public_identity_aliases
after insert or update of full_name on core.people
for each row execute function core.sync_person_public_identity_aliases();

-- Never leave a partially seeded identity surface. Partial unique indexes
-- enforce at most one canonical alias; these checks enforce exactly one for
-- every row present when the migration is applied. Future inserts are covered
-- by the parent-table triggers above.
do $validate_show_canonical_aliases$
begin
  if exists (
    select 1
    from core.shows s
    left join core.show_slug_aliases a
      on a.show_id = s.id
     and a.is_canonical = true
    group by s.id
    having count(a.id) <> 1
  ) then
    raise exception 'Every core.shows row must have exactly one canonical show slug alias';
  end if;
end
$validate_show_canonical_aliases$;

do $validate_person_canonical_aliases$
begin
  if exists (
    select 1
    from core.people p
    left join core.person_slug_aliases a
      on a.person_id = p.id
     and a.is_canonical = true
    group by p.id
    having count(a.id) <> 1
  ) then
    raise exception 'Every core.people row must have exactly one canonical person slug alias';
  end if;
end
$validate_person_canonical_aliases$;

grant select on table core.show_slug_aliases, core.person_slug_aliases to anon, authenticated;
grant all privileges on table core.show_slug_aliases, core.person_slug_aliases to service_role;
revoke insert, update, delete on table core.show_slug_aliases, core.person_slug_aliases from anon, authenticated;

alter table core.show_slug_aliases enable row level security;
alter table core.person_slug_aliases enable row level security;

drop policy if exists core_show_slug_aliases_public_read on core.show_slug_aliases;
create policy core_show_slug_aliases_public_read
  on core.show_slug_aliases
  for select to anon, authenticated
  using (true);

drop policy if exists core_show_slug_aliases_service_role_all on core.show_slug_aliases;
create policy core_show_slug_aliases_service_role_all
  on core.show_slug_aliases
  for all to service_role
  using (true)
  with check (true);

drop policy if exists core_person_slug_aliases_public_read on core.person_slug_aliases;
create policy core_person_slug_aliases_public_read
  on core.person_slug_aliases
  for select to anon, authenticated
  using (true);

drop policy if exists core_person_slug_aliases_service_role_all on core.person_slug_aliases;
create policy core_person_slug_aliases_service_role_all
  on core.person_slug_aliases
  for all to service_role
  using (true)
  with check (true);

revoke all on function core.normalize_public_identity_slug(text) from public, anon, authenticated;
revoke all on function core.sync_show_public_identity_aliases() from public, anon, authenticated;
revoke all on function core.sync_person_public_identity_aliases() from public, anon, authenticated;
grant execute on function core.normalize_public_identity_slug(text) to service_role;
grant execute on function core.sync_show_public_identity_aliases() to service_role;
grant execute on function core.sync_person_public_identity_aliases() to service_role;

comment on table core.show_slug_aliases is
  'Direct and canonical public URL aliases for core.shows. Canonical aliases are globally unique and stable.';
comment on table core.person_slug_aliases is
  'Direct and canonical public URL aliases for core.people. Person collisions may be narrowed by show context.';
comment on function core.normalize_public_identity_slug(text) is
  'Normalizes public identity aliases to a maximum 120-character base, leaving room for a full UUID suffix.';

commit;
