begin;

-- Promote the live unique business keys to primary keys on the source snapshot
-- and sync tables. These are the real write-time conflict targets today.
alter table core.show_source_latest
  drop constraint if exists show_source_latest_show_id_source_id_variant_key,
  add constraint show_source_latest_pkey primary key (show_id, source_id, variant);

alter table core.season_source_latest
  drop constraint if exists season_source_latest_season_id_source_id_variant_key,
  add constraint season_source_latest_pkey primary key (season_id, source_id, variant);

alter table core.episode_source_latest
  drop constraint if exists episode_source_latest_episode_id_source_id_variant_key,
  add constraint episode_source_latest_pkey primary key (episode_id, source_id, variant);

alter table core.person_source_latest
  drop constraint if exists person_source_latest_person_id_source_id_variant_key,
  add constraint person_source_latest_pkey primary key (person_id, source_id, variant);

alter table core.sync_state
  drop constraint if exists sync_state_table_name_show_id_key,
  add constraint sync_state_pkey primary key (table_name, show_id);

-- Consolidate the only live duplicate permissive-policy pairs and make the
-- user-role expressions initplan-friendly at the same time.
drop policy if exists "Service role full access to sessions" on public.flashback_sessions;
drop policy if exists "Users manage own sessions" on public.flashback_sessions;
create policy "Sessions access"
on public.flashback_sessions
as permissive
for all
to public
using (((select auth.role()) = 'service_role'::text) or (((select auth.uid())::text) = user_id))
with check (((select auth.role()) = 'service_role'::text) or (((select auth.uid())::text) = user_id));

drop policy if exists "Service role full access to user stats" on public.flashback_user_stats;
drop policy if exists "Users manage own stats" on public.flashback_user_stats;
create policy "User stats access"
on public.flashback_user_stats
as permissive
for all
to public
using (((select auth.role()) = 'service_role'::text) or (((select auth.uid())::text) = user_id))
with check (((select auth.role()) = 'service_role'::text) or (((select auth.uid())::text) = user_id));

-- Rewrite remaining auth/current_setting policy calls into SELECT-wrapped
-- initplan form without changing which policies exist.
do $$
declare
  rec record;
  new_qual text;
  new_with_check text;
begin
  for rec in
    select schemaname, tablename, policyname, coalesce(qual, '') as qual, coalesce(with_check, '') as with_check
    from pg_policies
    where schemaname in ('public', 'social', 'surveys', 'firebase_surveys')
      and not (schemaname = 'public' and tablename in ('flashback_sessions', 'flashback_user_stats'))
      and (
        coalesce(qual, '') like '%auth.uid()%'
        or coalesce(with_check, '') like '%auth.uid()%'
        or coalesce(qual, '') like '%auth.jwt()%'
        or coalesce(with_check, '') like '%auth.jwt()%'
        or coalesce(qual, '') like '%current_setting(%'
        or coalesce(with_check, '') like '%current_setting(%'
      )
  loop
    new_qual := replace(rec.qual, 'auth.uid()', '(select auth.uid())');
    new_qual := replace(new_qual, 'auth.jwt()', '(select auth.jwt())');
    new_qual := replace(
      new_qual,
      'current_setting(''app.is_admin''::text, true)',
      '(select current_setting(''app.is_admin''::text, true))'
    );
    new_qual := replace(
      new_qual,
      'current_setting(''app.firebase_uid''::text, true)',
      '(select current_setting(''app.firebase_uid''::text, true))'
    );

    new_with_check := replace(rec.with_check, 'auth.uid()', '(select auth.uid())');
    new_with_check := replace(new_with_check, 'auth.jwt()', '(select auth.jwt())');
    new_with_check := replace(
      new_with_check,
      'current_setting(''app.is_admin''::text, true)',
      '(select current_setting(''app.is_admin''::text, true))'
    );
    new_with_check := replace(
      new_with_check,
      'current_setting(''app.firebase_uid''::text, true)',
      '(select current_setting(''app.firebase_uid''::text, true))'
    );

    if new_qual <> rec.qual then
      execute format(
        'alter policy %I on %I.%I using (%s)',
        rec.policyname,
        rec.schemaname,
        rec.tablename,
        new_qual
      );
    end if;

    if new_with_check <> rec.with_check and new_with_check <> '' then
      execute format(
        'alter policy %I on %I.%I with check (%s)',
        rec.policyname,
        rec.schemaname,
        rec.tablename,
        new_with_check
      );
    end if;
  end loop;
end;
$$;

-- Drop live duplicate indexes while preserving the unique or higher-usage copy.
drop index if exists admin.idx_covered_shows_trr_show;
drop index if exists core.idx_admin_operation_events_operation_seq;
drop index if exists core.idx_cast_tmdb_person_id;
drop index if exists core.idx_cast_tmdb_tmdb_id;
drop index if exists core.core_episodes_show_season_idx;
drop index if exists core.media_assets_sha256_idx;
drop index if exists core.idx_people_overrides_person_id;
drop index if exists core.core_seasons_show_id_season_number_idx;
drop index if exists firebase_surveys.idx_firebase_surveys_slug;
drop index if exists public.idx_flashback_quizzes_publish;
drop index if exists public.idx_sgpr_app_user_id;
drop index if exists public.idx_survey_shows_key;
drop index if exists public.idx_survey_x_app_user_id;
drop index if exists public.idx_surveys_key;
drop index if exists social.idx_social_scrape_runs_season_created_at;
drop index if exists social.shared_account_run_frontiers_lookup_idx;
drop index if exists social.idx_social_ingest_checkpoints_platform_creator;
drop index if exists social.idx_youtube_channel_sync_state_season_scope_account;
drop index if exists surveys.survey_aggregates_survey_id_question_id_idx;

commit;
