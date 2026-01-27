begin;

create table if not exists core.show_alternative_names (
  id bigserial primary key,
  show_id uuid not null references core.shows(id) on delete cascade,
  name text not null,
  language text,
  country text,
  source text not null default 'tmdb',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists show_alternative_names_unique
  on core.show_alternative_names(show_id, name, language, country, source);

create index if not exists show_alternative_names_show_id_idx
  on core.show_alternative_names(show_id);

-- Trigger guard (idempotent)
do $$
begin
  if not exists (select 1 from pg_trigger where tgname = 'show_alternative_names_set_updated_at') then
    create trigger show_alternative_names_set_updated_at
    before update on core.show_alternative_names
    for each row execute function core.set_updated_at();
  end if;
end $$;

-- Grants / RLS (adjust to your access model)
grant select on table core.show_alternative_names to anon, authenticated;
grant all privileges on table core.show_alternative_names to service_role;

alter table core.show_alternative_names enable row level security;
do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname = 'core'
      and tablename = 'show_alternative_names'
      and policyname = 'core_show_alternative_names_public_read'
  ) then
    create policy core_show_alternative_names_public_read
      on core.show_alternative_names for select to anon, authenticated
      using (true);
  end if;
end $$;

commit;
