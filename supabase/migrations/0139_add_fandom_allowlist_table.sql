begin;

create table if not exists core.fandom_community_allowlist (
  domain text primary key,
  is_active boolean not null default true,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists core_fandom_community_allowlist_active_idx
  on core.fandom_community_allowlist (is_active)
  where is_active = true;

grant select on table core.fandom_community_allowlist to anon, authenticated;
grant all privileges on table core.fandom_community_allowlist to service_role;

commit;
