-- Direct Messages v1
-- Adds 1:1 DM support with conversations, messages, and read receipts

-- =============================================================================
-- TABLES
-- =============================================================================

-- DM Conversations
-- is_group=false for 1:1 DMs, direct_key ensures uniqueness
create table if not exists social.dm_conversations (
    id uuid primary key default gen_random_uuid(),
    is_group boolean not null default false,
    direct_key text unique,  -- 'user1:user2' for 1:1 DMs (sorted by UUID)
    created_at timestamptz not null default now(),
    last_message_at timestamptz
);

comment on table social.dm_conversations is 'DM conversations (1:1 or group)';
comment on column social.dm_conversations.direct_key is 'Unique key for 1:1 DMs: concat(least(user1,user2), '':'', greatest(user1,user2))';

-- DM Members (who is in each conversation)
create table if not exists social.dm_members (
    conversation_id uuid not null references social.dm_conversations(id) on delete cascade,
    user_id uuid not null references auth.users(id) on delete cascade,
    role text not null default 'member',
    joined_at timestamptz not null default now(),
    primary key (conversation_id, user_id)
);

comment on table social.dm_members is 'Members of each DM conversation';

create index if not exists idx_dm_members_user_joined
    on social.dm_members(user_id, joined_at desc);

-- DM Messages
create table if not exists social.dm_messages (
    id uuid primary key default gen_random_uuid(),
    conversation_id uuid not null references social.dm_conversations(id) on delete cascade,
    sender_id uuid references auth.users(id) on delete set null,
    body text not null,
    created_at timestamptz not null default now()
);

comment on table social.dm_messages is 'Messages in DM conversations';

create index if not exists idx_dm_messages_conversation_created
    on social.dm_messages(conversation_id, created_at desc);

-- DM Read Receipts (per-user, per-conversation)
create table if not exists social.dm_read_receipts (
    conversation_id uuid not null references social.dm_conversations(id) on delete cascade,
    user_id uuid not null references auth.users(id) on delete cascade,
    last_read_message_id uuid references social.dm_messages(id) on delete set null,
    last_read_at timestamptz not null default now(),
    primary key (conversation_id, user_id)
);

comment on table social.dm_read_receipts is 'Read receipts tracking last read message per user per conversation';

-- =============================================================================
-- RLS POLICIES
-- =============================================================================

alter table social.dm_conversations enable row level security;
alter table social.dm_members enable row level security;
alter table social.dm_messages enable row level security;
alter table social.dm_read_receipts enable row level security;

-- Conversations: can only see if you're a member
create policy "Members can view their conversations"
    on social.dm_conversations for select
    using (
        exists (
            select 1 from social.dm_members
            where dm_members.conversation_id = dm_conversations.id
            and dm_members.user_id = auth.uid()
        )
    );

-- Conversations: members can update last_message_at
create policy "Members can update conversation timestamp"
    on social.dm_conversations for update
    using (
        exists (
            select 1 from social.dm_members
            where dm_members.conversation_id = dm_conversations.id
            and dm_members.user_id = auth.uid()
        )
    )
    with check (
        exists (
            select 1 from social.dm_members
            where dm_members.conversation_id = dm_conversations.id
            and dm_members.user_id = auth.uid()
        )
    );

-- Members: can only see memberships of conversations you're in
create policy "Members can view conversation memberships"
    on social.dm_members for select
    using (
        exists (
            select 1 from social.dm_members my_membership
            where my_membership.conversation_id = dm_members.conversation_id
            and my_membership.user_id = auth.uid()
        )
    );

-- Messages: can only see messages in conversations you're a member of
create policy "Members can view conversation messages"
    on social.dm_messages for select
    using (
        exists (
            select 1 from social.dm_members
            where dm_members.conversation_id = dm_messages.conversation_id
            and dm_members.user_id = auth.uid()
        )
    );

-- Messages: can only insert if you're a member AND you're the sender
create policy "Members can send messages"
    on social.dm_messages for insert
    with check (
        sender_id = auth.uid()
        and exists (
            select 1 from social.dm_members
            where dm_members.conversation_id = dm_messages.conversation_id
            and dm_members.user_id = auth.uid()
        )
    );

-- Read receipts: can only see your own
create policy "Users can view their read receipts"
    on social.dm_read_receipts for select
    using (user_id = auth.uid());

-- Read receipts: can only update your own
create policy "Users can update their read receipts"
    on social.dm_read_receipts for update
    using (user_id = auth.uid())
    with check (user_id = auth.uid());

-- Read receipts: can only insert your own
create policy "Users can insert their read receipts"
    on social.dm_read_receipts for insert
    with check (user_id = auth.uid());

-- =============================================================================
-- RPC FUNCTION: get_or_create_direct_conversation
-- =============================================================================
-- This function is required because with strict RLS, a user cannot add
-- another user to dm_members directly. This security definer function
-- handles the atomic creation of conversation + both memberships.

create or replace function social.get_or_create_direct_conversation(other_user_id uuid)
returns uuid
language plpgsql
security definer
set search_path = social, auth, public
as $$
declare
    caller_id uuid;
    v_direct_key text;
    v_conversation_id uuid;
begin
    -- Get the authenticated user
    caller_id := auth.uid();
    if caller_id is null then
        raise exception 'Authentication required';
    end if;

    -- Cannot DM yourself
    if caller_id = other_user_id then
        raise exception 'Cannot create DM with yourself';
    end if;

    -- Generate direct_key (sorted by UUID to ensure uniqueness)
    v_direct_key := concat(least(caller_id::text, other_user_id::text), ':', greatest(caller_id::text, other_user_id::text));

    -- Try to find existing conversation
    select id into v_conversation_id
    from social.dm_conversations
    where direct_key = v_direct_key;

    -- If not found, create new conversation with members
    if v_conversation_id is null then
        -- Create conversation
        insert into social.dm_conversations (is_group, direct_key)
        values (false, v_direct_key)
        returning id into v_conversation_id;

        -- Add both members
        insert into social.dm_members (conversation_id, user_id)
        values
            (v_conversation_id, caller_id),
            (v_conversation_id, other_user_id);

        -- Initialize read receipts for both users
        insert into social.dm_read_receipts (conversation_id, user_id)
        values
            (v_conversation_id, caller_id),
            (v_conversation_id, other_user_id);
    end if;

    return v_conversation_id;
end;
$$;

comment on function social.get_or_create_direct_conversation(uuid) is
    'Creates or retrieves a 1:1 DM conversation between the caller and another user. Returns the conversation ID.';

-- Grant execute to authenticated users
grant execute on function social.get_or_create_direct_conversation(uuid) to authenticated;
