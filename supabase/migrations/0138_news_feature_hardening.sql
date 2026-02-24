begin;

create table if not exists core.news_topic_taxonomy (
    topic_key text primary key,
    keywords text[] not null default '{}',
    enabled boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

insert into core.news_topic_taxonomy (topic_key, keywords, enabled)
values
    ('casting', array['cast', 'housewife', 'friend of', 'joins', 'joined', 'returning', 'returns'], true),
    ('reunion', array['reunion', 'part 1', 'part 2', 'part 3', 'sit-down'], true),
    ('relationship', array['dating', 'married', 'divorce', 'split', 'engaged', 'boyfriend', 'girlfriend'], true),
    ('legal', array['lawsuit', 'sued', 'arrested', 'charges', 'legal', 'court'], true),
    ('drama', array['feud', 'fight', 'drama', 'clash', 'shade'], true),
    ('premiere', array['premiere', 'first look', 'trailer', 'teaser'], true),
    ('finale', array['finale', 'wrap-up'], true)
on conflict (topic_key) do update
set keywords = excluded.keywords,
    enabled = excluded.enabled,
    updated_at = now();

create table if not exists core.google_news_sync_jobs (
    id uuid primary key default gen_random_uuid(),
    show_id uuid not null references core.shows(id) on delete cascade,
    source_id text not null default 'google_news',
    status text not null check (status in ('queued', 'running', 'completed', 'failed')),
    requested_async boolean not null default false,
    force boolean not null default false,
    requested_by text,
    result jsonb not null default '{}'::jsonb,
    error text,
    created_at timestamptz not null default now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz not null default now()
);

create index if not exists idx_google_news_sync_jobs_show_created
    on core.google_news_sync_jobs (show_id, created_at desc);

create index if not exists idx_google_news_sync_jobs_status
    on core.google_news_sync_jobs (status, updated_at desc);

commit;
