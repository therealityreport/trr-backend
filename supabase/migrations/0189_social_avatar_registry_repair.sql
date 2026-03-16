begin;

create table if not exists social.avatar_registry (
  id uuid primary key default gen_random_uuid(),
  platform text not null,
  account_handle text not null,
  source_url text,
  content_hash text,
  hosted_url text,
  status text not null default 'pending'
    check (status in ('pending', 'mirrored', 'failed', 'unsupported')),
  failure_reason text,
  last_checked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (platform, account_handle, source_url)
);

create index if not exists idx_avatar_registry_platform_account
  on social.avatar_registry (platform, account_handle, updated_at desc);

create index if not exists idx_avatar_registry_status
  on social.avatar_registry (status, updated_at desc);

grant all privileges on table social.avatar_registry to service_role;

commit;
