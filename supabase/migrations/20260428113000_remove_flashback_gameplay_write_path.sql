begin;

-- Flashback gameplay is intentionally disabled for now. Keep the quiz/event
-- setup tables, but remove the empty session/stat write path and RPC helpers
-- so future environments match the live cleanup.
drop function if exists public.flashback_get_or_create_session(text, uuid);
drop function if exists public.flashback_save_placement(uuid, jsonb, integer, integer, boolean);
drop function if exists public.flashback_update_user_stats(text, integer, boolean);

drop table if exists public.flashback_user_stats;
drop table if exists public.flashback_sessions;

commit;
