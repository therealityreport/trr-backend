create table if not exists social.account_hashtag_assignments (
  id uuid primary key default gen_random_uuid(),
  platform text not null,
  account_handle text not null,
  normalized_hashtag text not null,
  display_hashtag text not null,
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid references core.seasons(id) on delete cascade,
  updated_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint social_account_hashtag_assignments_platform_check
    check (platform in ('instagram', 'tiktok', 'twitter', 'youtube', 'facebook', 'threads'))
);

create unique index if not exists social_account_hashtag_assignments_unique_idx
  on social.account_hashtag_assignments (
    platform,
    account_handle,
    normalized_hashtag,
    show_id,
    coalesce(season_id, '00000000-0000-0000-0000-000000000000'::uuid)
  );

create index if not exists social_account_hashtag_assignments_lookup_idx
  on social.account_hashtag_assignments (platform, account_handle, normalized_hashtag);

create index if not exists social_account_hashtag_assignments_show_idx
  on social.account_hashtag_assignments (show_id, season_id);
