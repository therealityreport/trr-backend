begin;

-- ---------------------------------------------------------------------------
-- core.person_images - Primary person images from various sources
-- ---------------------------------------------------------------------------
--
-- This table stores primary person images from different sources (IMDb GraphQL, TMDb, etc.).
-- Unlike core.cast_photos (which stores full gallery images), this focuses on the
-- single primary/profile image used for cast selection and display.
--
-- Rationale:
-- - Cast selection now filters by photo presence (episodeCount <= 6 requires primaryImage)
-- - We need to persist these images for rendering and audit/debug purposes
-- - Separate from cast_photos to keep the primary image concept distinct from galleries
-- ---------------------------------------------------------------------------

create table if not exists core.person_images (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people (id) on delete cascade,
  source text not null,
  url text not null,
  width integer null,
  height integer null,
  caption text null,
  is_primary boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  -- Idempotent upsert by (person_id, source, url)
  constraint person_images_person_source_url_unique unique (person_id, source, url)
);

-- Indexes for efficient lookups
create index if not exists person_images_person_id_idx
  on core.person_images (person_id);

create index if not exists person_images_person_id_is_primary_idx
  on core.person_images (person_id, is_primary)
  where is_primary = true;

-- Maintain updated_at automatically on updates
create trigger core_person_images_set_updated_at
before update on core.person_images
for each row
execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- Upsert helper function
-- ---------------------------------------------------------------------------

create or replace function core.upsert_person_images(rows jsonb)
returns setof core.person_images
language plpgsql
security definer
as $$
declare
  rec jsonb;
  row_data core.person_images%rowtype;
  inserted core.person_images%rowtype;
begin
  if rows is null then
    return;
  end if;

  for rec in select * from jsonb_array_elements(rows)
  loop
    row_data := jsonb_populate_record(null::core.person_images, rec);

    insert into core.person_images (
      person_id,
      source,
      url,
      width,
      height,
      caption,
      is_primary,
      created_at,
      updated_at
    ) values (
      row_data.person_id,
      row_data.source,
      row_data.url,
      row_data.width,
      row_data.height,
      row_data.caption,
      coalesce(row_data.is_primary, true),
      coalesce(row_data.created_at, now()),
      coalesce(row_data.updated_at, now())
    )
    on conflict on constraint person_images_person_source_url_unique
    do update set
      -- Update dimensions if incoming has better data
      width = case
        when excluded.width is not null and excluded.width > coalesce(core.person_images.width, 0)
          then excluded.width
        else core.person_images.width
      end,
      height = case
        when excluded.height is not null and excluded.height > coalesce(core.person_images.height, 0)
          then excluded.height
        else core.person_images.height
      end,
      caption = coalesce(excluded.caption, core.person_images.caption),
      is_primary = coalesce(excluded.is_primary, core.person_images.is_primary),
      updated_at = now()
    returning * into inserted;

    return next inserted;
  end loop;

  return;
end $$;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------

grant select on table core.person_images to anon, authenticated;
grant all privileges on table core.person_images to service_role;
grant execute on function core.upsert_person_images(jsonb) to service_role;

-- ---------------------------------------------------------------------------
-- RLS policies
-- ---------------------------------------------------------------------------

alter table core.person_images enable row level security;

drop policy if exists core_person_images_public_read on core.person_images;
create policy core_person_images_public_read on core.person_images
for select to anon, authenticated
using (true);

commit;
