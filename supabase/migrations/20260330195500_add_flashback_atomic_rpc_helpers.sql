create or replace function public.flashback_get_or_create_session(
  p_user_id text,
  p_quiz_id uuid
)
returns public.flashback_sessions
language plpgsql
security invoker
set search_path = public
as $$
declare
  v_session public.flashback_sessions;
begin
  insert into public.flashback_sessions (
    user_id,
    quiz_id,
    current_round,
    score,
    placements,
    completed
  )
  values (
    p_user_id,
    p_quiz_id,
    0,
    0,
    '[]'::jsonb,
    false
  )
  on conflict (user_id, quiz_id)
  do update
    set user_id = excluded.user_id
  returning * into v_session;

  return v_session;
end;
$$;

create or replace function public.flashback_save_placement(
  p_session_id uuid,
  p_placement jsonb,
  p_new_score integer,
  p_new_round integer,
  p_completed boolean
)
returns public.flashback_sessions
language plpgsql
security invoker
set search_path = public
as $$
declare
  v_session public.flashback_sessions;
begin
  update public.flashback_sessions
     set placements = coalesce(placements, '[]'::jsonb) || jsonb_build_array(p_placement),
         score = p_new_score,
         current_round = p_new_round,
         completed = p_completed,
         completed_at = case
           when p_completed then coalesce(completed_at, now())
           else null
         end
   where id = p_session_id
  returning * into v_session;

  if v_session.id is null then
    raise exception 'Flashback session not found: %', p_session_id;
  end if;

  return v_session;
end;
$$;

create or replace function public.flashback_update_user_stats(
  p_user_id text,
  p_points_earned integer,
  p_is_perfect boolean
)
returns public.flashback_user_stats
language plpgsql
security invoker
set search_path = public
as $$
declare
  v_stats public.flashback_user_stats;
begin
  insert into public.flashback_user_stats (
    user_id,
    games_played,
    total_points,
    perfect_scores,
    current_streak,
    max_streak,
    updated_at
  )
  values (
    p_user_id,
    1,
    p_points_earned,
    case when p_is_perfect then 1 else 0 end,
    1,
    1,
    now()
  )
  on conflict (user_id)
  do update
    set games_played = public.flashback_user_stats.games_played + 1,
        total_points = public.flashback_user_stats.total_points + excluded.total_points,
        perfect_scores = public.flashback_user_stats.perfect_scores + excluded.perfect_scores,
        current_streak = public.flashback_user_stats.current_streak + 1,
        max_streak = greatest(public.flashback_user_stats.max_streak, public.flashback_user_stats.current_streak + 1),
        updated_at = now()
  returning * into v_stats;

  return v_stats;
end;
$$;

grant execute on function public.flashback_get_or_create_session(text, uuid) to anon, authenticated, service_role;
grant execute on function public.flashback_save_placement(uuid, jsonb, integer, integer, boolean) to anon, authenticated, service_role;
grant execute on function public.flashback_update_user_stats(text, integer, boolean) to anon, authenticated, service_role;
