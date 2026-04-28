begin;

create table if not exists social.instagram_account_catalog_post_collaborators (
  catalog_post_id uuid not null references social.instagram_account_catalog_posts(id) on delete cascade,
  source_id text not null,
  source_account text not null,
  collaborator_handle text not null,
  collaborator_source text not null check (collaborator_source in ('collaborators', 'collaborators_detail')),
  posted_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (catalog_post_id, collaborator_handle)
);

create index if not exists instagram_catalog_post_collaborators_handle_posted_idx
  on social.instagram_account_catalog_post_collaborators
  (collaborator_handle, posted_at desc nulls last, catalog_post_id);

create index if not exists instagram_catalog_post_collaborators_source_account_idx
  on social.instagram_account_catalog_post_collaborators
  (lower(source_account), posted_at desc nulls last);

drop trigger if exists set_instagram_catalog_post_collaborators_updated_at
  on social.instagram_account_catalog_post_collaborators;

create trigger set_instagram_catalog_post_collaborators_updated_at
before update on social.instagram_account_catalog_post_collaborators
for each row
execute function public.set_current_timestamp_updated_at();

create temp table instagram_catalog_post_collaborator_backfill (
  catalog_post_id uuid not null,
  source_id text not null,
  source_account text not null,
  collaborator_handle text not null,
  collaborator_source text not null,
  source_rank int not null,
  posted_at timestamptz
) on commit drop;

insert into instagram_catalog_post_collaborator_backfill (
  catalog_post_id,
  source_id,
  source_account,
  collaborator_handle,
  collaborator_source,
  source_rank,
  posted_at
)
select
  p.id,
  p.source_id,
  lower(p.source_account),
  normalized.handle,
  'collaborators',
  1,
  p.posted_at
from social.instagram_account_catalog_posts p
cross join lateral jsonb_array_elements_text(
  case
    when jsonb_typeof(coalesce(p.collaborators, '[]'::jsonb)) = 'array'
      then coalesce(p.collaborators, '[]'::jsonb)
    else '[]'::jsonb
  end
) as collaborator(value)
cross join lateral (
  select nullif(
    regexp_replace(
      lower(regexp_replace(coalesce(collaborator.value, ''), '^@+', '')),
      '[^a-z0-9._-]+',
      '',
      'g'
    ),
    ''
  ) as handle
) normalized
where normalized.handle is not null;

insert into instagram_catalog_post_collaborator_backfill (
  catalog_post_id,
  source_id,
  source_account,
  collaborator_handle,
  collaborator_source,
  source_rank,
  posted_at
)
select
  p.id,
  p.source_id,
  lower(p.source_account),
  normalized.handle,
  'collaborators_detail',
  2,
  p.posted_at
from social.instagram_account_catalog_posts p
cross join lateral jsonb_array_elements(
  case
    when jsonb_typeof(coalesce(p.raw_data -> 'collaborators_detail', '[]'::jsonb)) = 'array'
      then coalesce(p.raw_data -> 'collaborators_detail', '[]'::jsonb)
    else '[]'::jsonb
  end
) as collaborator_detail(value)
cross join lateral (
  select nullif(
    regexp_replace(
      lower(regexp_replace(coalesce(collaborator_detail.value ->> 'username', ''), '^@+', '')),
      '[^a-z0-9._-]+',
      '',
      'g'
    ),
    ''
  ) as handle
) normalized
where normalized.handle is not null;

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'social'
      and table_name = 'instagram_account_catalog_posts'
      and column_name = 'collaborators_detail'
  ) then
    execute $sql$
      insert into instagram_catalog_post_collaborator_backfill (
        catalog_post_id,
        source_id,
        source_account,
        collaborator_handle,
        collaborator_source,
        source_rank,
        posted_at
      )
      select
        p.id,
        p.source_id,
        lower(p.source_account),
        normalized.handle,
        'collaborators_detail',
        3,
        p.posted_at
      from social.instagram_account_catalog_posts p
      cross join lateral jsonb_array_elements(
        case
          when jsonb_typeof(coalesce(to_jsonb(p.collaborators_detail), '[]'::jsonb)) = 'array'
            then coalesce(to_jsonb(p.collaborators_detail), '[]'::jsonb)
          else '[]'::jsonb
        end
      ) as collaborator_detail(value)
      cross join lateral (
        select nullif(
          regexp_replace(
            lower(regexp_replace(coalesce(collaborator_detail.value ->> 'username', ''), '^@+', '')),
            '[^a-z0-9._-]+',
            '',
            'g'
          ),
          ''
        ) as handle
      ) normalized
      where normalized.handle is not null
    $sql$;
  end if;
end $$;

delete from social.instagram_account_catalog_post_collaborators existing
where not exists (
  select 1
  from instagram_catalog_post_collaborator_backfill backfill
  where backfill.catalog_post_id = existing.catalog_post_id
    and backfill.collaborator_handle = existing.collaborator_handle
);

insert into social.instagram_account_catalog_post_collaborators (
  catalog_post_id,
  source_id,
  source_account,
  collaborator_handle,
  collaborator_source,
  posted_at
)
select distinct on (catalog_post_id, collaborator_handle)
  catalog_post_id,
  source_id,
  source_account,
  collaborator_handle,
  collaborator_source,
  posted_at
from instagram_catalog_post_collaborator_backfill
order by catalog_post_id, collaborator_handle, source_rank desc
on conflict (catalog_post_id, collaborator_handle) do update set
  source_id = excluded.source_id,
  source_account = excluded.source_account,
  collaborator_source = excluded.collaborator_source,
  posted_at = excluded.posted_at,
  updated_at = now();

grant select on table social.instagram_account_catalog_post_collaborators to anon, authenticated;
grant all privileges on table social.instagram_account_catalog_post_collaborators to service_role;

alter table social.instagram_account_catalog_post_collaborators enable row level security;

drop policy if exists instagram_account_catalog_post_collaborators_public_read
  on social.instagram_account_catalog_post_collaborators;

create policy instagram_account_catalog_post_collaborators_public_read
on social.instagram_account_catalog_post_collaborators
for select to anon, authenticated using (true);

commit;
