begin;

-- Backend-owned home for the schema DDL previously executed by the TRR-APP
-- typography repository during request handling.
create or replace function public.set_site_typography_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.site_typography_sets (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  name text not null,
  area text not null check (area in ('user-frontend', 'surveys', 'admin')),
  seed_source text not null,
  roles jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.site_typography_assignments (
  id uuid primary key default gen_random_uuid(),
  area text not null check (area in ('user-frontend', 'surveys', 'admin')),
  page_key text,
  instance_key text,
  set_id uuid not null references public.site_typography_sets(id) on delete restrict,
  source_path text not null,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_site_typography_assignments_scope
  on public.site_typography_assignments (
    area,
    coalesce(page_key, ''),
    coalesce(instance_key, '')
  );

create index if not exists idx_site_typography_assignments_set_id
  on public.site_typography_assignments (set_id);

drop trigger if exists trg_site_typography_sets_updated_at
  on public.site_typography_sets;

create trigger trg_site_typography_sets_updated_at
before update on public.site_typography_sets
for each row
execute function public.set_site_typography_updated_at();

drop trigger if exists trg_site_typography_assignments_updated_at
  on public.site_typography_assignments;

create trigger trg_site_typography_assignments_updated_at
before update on public.site_typography_assignments
for each row
execute function public.set_site_typography_updated_at();

commit;
