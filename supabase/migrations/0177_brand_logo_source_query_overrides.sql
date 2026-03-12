create table if not exists admin.brand_logo_source_queries (
  id uuid primary key default gen_random_uuid(),
  target_type text not null,
  target_key text not null,
  logo_role text not null,
  source_provider text not null,
  query_value text not null,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  constraint brand_logo_source_queries_key_unique unique (target_type, target_key, logo_role, source_provider)
);

create index if not exists brand_logo_source_queries_target_idx
  on admin.brand_logo_source_queries (target_type, target_key, logo_role);

grant all privileges on table admin.brand_logo_source_queries to service_role;
