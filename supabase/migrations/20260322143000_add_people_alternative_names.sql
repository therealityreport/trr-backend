alter table core.people
add column if not exists alternative_names jsonb not null default '{}'::jsonb;
