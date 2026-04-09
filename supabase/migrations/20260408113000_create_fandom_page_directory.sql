create table if not exists core.fandom_page_directory (
  id uuid primary key default gen_random_uuid(),
  community_domain text not null,
  page_title text not null,
  page_slug text not null,
  page_url text not null,
  source_kind text not null default 'allpages_html',
  is_active boolean not null default true,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists fandom_page_directory_domain_url_idx
  on core.fandom_page_directory (community_domain, page_url);

create index if not exists fandom_page_directory_domain_slug_active_idx
  on core.fandom_page_directory (community_domain, page_slug)
  where is_active = true;

create index if not exists fandom_page_directory_domain_title_active_idx
  on core.fandom_page_directory (community_domain, page_title)
  where is_active = true;

create index if not exists fandom_page_directory_domain_active_last_seen_idx
  on core.fandom_page_directory (community_domain, is_active, last_seen_at desc);

create trigger set_fandom_page_directory_updated_at
before update on core.fandom_page_directory
for each row
execute function public.set_current_timestamp_updated_at();
