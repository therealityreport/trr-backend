begin;

-- Episode discussion threads, posts, and reactions (Reddit-style)
-- Schema: social

create schema if not exists social;

-- ---------------------------------------------------------------------------
-- social.threads - Discussion threads tied to episodes
-- ---------------------------------------------------------------------------

create table social.threads (
  id uuid primary key default gen_random_uuid(),
  episode_id uuid not null references core.episodes (id) on delete cascade,
  title text not null,
  type text not null check (type in ('episode_live', 'post_episode', 'spoilers', 'general')),
  created_by uuid references auth.users (id) on delete set null,
  is_locked boolean not null default false,
  created_at timestamptz not null default now()
);

create index threads_episode_id_created_at_idx on social.threads (episode_id, created_at desc);
create index threads_created_by_idx on social.threads (created_by);

-- ---------------------------------------------------------------------------
-- social.posts - Posts/comments within threads (supports nesting)
-- ---------------------------------------------------------------------------

create table social.posts (
  id uuid primary key default gen_random_uuid(),
  thread_id uuid not null references social.threads (id) on delete cascade,
  parent_post_id uuid references social.posts (id) on delete cascade,
  user_id uuid references auth.users (id) on delete set null,
  body text not null,
  created_at timestamptz not null default now(),
  edited_at timestamptz
);

create index posts_thread_id_created_at_idx on social.posts (thread_id, created_at asc);
create index posts_parent_post_id_created_at_idx on social.posts (parent_post_id, created_at asc);
create index posts_user_id_idx on social.posts (user_id);

-- ---------------------------------------------------------------------------
-- social.reactions - Reactions to posts (upvote, downvote, lol, shade, etc.)
-- ---------------------------------------------------------------------------

create table social.reactions (
  post_id uuid not null references social.posts (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  reaction text not null check (reaction in ('upvote', 'downvote', 'lol', 'shade', 'fire', 'heart')),
  created_at timestamptz not null default now(),
  primary key (post_id, user_id, reaction)
);

create index reactions_post_id_reaction_idx on social.reactions (post_id, reaction);

-- ---------------------------------------------------------------------------
-- Grants (privileges) for Supabase API roles
-- ---------------------------------------------------------------------------

grant usage on schema social to anon, authenticated, service_role;

-- Public read on all social tables
grant select on table
  social.threads,
  social.posts,
  social.reactions
to anon, authenticated;

-- Authenticated users can write their own content
grant insert, update, delete on table
  social.threads,
  social.posts,
  social.reactions
to authenticated;

-- Service role can manage everything (RLS bypass)
grant all privileges on all tables in schema social to service_role;

-- ---------------------------------------------------------------------------
-- Row Level Security (RLS)
-- ---------------------------------------------------------------------------

alter table social.threads enable row level security;
alter table social.posts enable row level security;
alter table social.reactions enable row level security;

-- Public read policies
create policy threads_public_read on social.threads
for select to anon, authenticated
using (true);

create policy posts_public_read on social.posts
for select to anon, authenticated
using (true);

create policy reactions_public_read on social.reactions
for select to anon, authenticated
using (true);

-- Thread creation: authenticated users only, must be their own
create policy threads_insert_own on social.threads
for insert to authenticated
with check (created_by = auth.uid());

-- Thread update: only creator can update (e.g., lock their thread)
create policy threads_update_own on social.threads
for update to authenticated
using (created_by = auth.uid())
with check (created_by = auth.uid());

-- Post creation: authenticated users only, must be their own, thread not locked
create policy posts_insert_own on social.posts
for insert to authenticated
with check (
  user_id = auth.uid()
  and not exists (
    select 1 from social.threads t
    where t.id = social.posts.thread_id
      and t.is_locked = true
  )
);

-- Post update: only author can update their own posts
create policy posts_update_own on social.posts
for update to authenticated
using (user_id = auth.uid())
with check (user_id = auth.uid());

-- Post delete: only author can delete their own posts
create policy posts_delete_own on social.posts
for delete to authenticated
using (user_id = auth.uid());

-- Reaction insert: authenticated users only, must be their own
create policy reactions_insert_own on social.reactions
for insert to authenticated
with check (user_id = auth.uid());

-- Reaction delete: only the user who created it can remove it
create policy reactions_delete_own on social.reactions
for delete to authenticated
using (user_id = auth.uid());

commit;
