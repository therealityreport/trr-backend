-- Rollback for 20260428111000_advisor_rls_policy_cleanup.sql.
--
-- Restores the Phase 0 policy names and expressions captured in
-- docs/workspace/supabase-advisor-performance-phase0-evidence-2026-04-28.md.
-- This intentionally uses explicit drop/create statements because policies do
-- not have safe CREATE OR REPLACE semantics for this restoration.

drop policy if exists core_networks_service_role_insert on core.networks;
drop policy if exists core_networks_service_role_update on core.networks;
drop policy if exists core_networks_service_role_delete on core.networks;
drop policy if exists core_tmdb_networks_service_role on core.networks;
create policy core_tmdb_networks_service_role on core.networks
as permissive
for all
to public
using (auth.role() = 'service_role'::text)
with check (auth.role() = 'service_role'::text);

drop policy if exists core_production_companies_service_role_insert on core.production_companies;
drop policy if exists core_production_companies_service_role_update on core.production_companies;
drop policy if exists core_production_companies_service_role_delete on core.production_companies;
drop policy if exists core_tmdb_production_companies_service_role on core.production_companies;
create policy core_tmdb_production_companies_service_role on core.production_companies
as permissive
for all
to public
using (auth.role() = 'service_role'::text)
with check (auth.role() = 'service_role'::text);

drop policy if exists core_show_watch_providers_service_role_insert on core.show_watch_providers;
drop policy if exists core_show_watch_providers_service_role_update on core.show_watch_providers;
drop policy if exists core_show_watch_providers_service_role_delete on core.show_watch_providers;
drop policy if exists core_show_watch_providers_service_role on core.show_watch_providers;
create policy core_show_watch_providers_service_role on core.show_watch_providers
as permissive
for all
to public
using (auth.role() = 'service_role'::text)
with check (auth.role() = 'service_role'::text);

drop policy if exists core_watch_providers_service_role_insert on core.watch_providers;
drop policy if exists core_watch_providers_service_role_update on core.watch_providers;
drop policy if exists core_watch_providers_service_role_delete on core.watch_providers;
drop policy if exists core_tmdb_watch_providers_service_role on core.watch_providers;
create policy core_tmdb_watch_providers_service_role on core.watch_providers
as permissive
for all
to public
using (auth.role() = 'service_role'::text)
with check (auth.role() = 'service_role'::text);

drop policy if exists show_icons_service_role_insert on public.show_icons;
drop policy if exists show_icons_service_role_update on public.show_icons;
drop policy if exists show_icons_service_role_delete on public.show_icons;
drop policy if exists "Allow service role all on show_icons" on public.show_icons;
create policy "Allow service role all on show_icons" on public.show_icons
as permissive
for all
to public
using (auth.role() = 'service_role'::text)
with check (auth.role() = 'service_role'::text);

drop policy if exists flashback_quizzes_service_role_insert on public.flashback_quizzes;
drop policy if exists flashback_quizzes_service_role_update on public.flashback_quizzes;
drop policy if exists flashback_quizzes_service_role_delete on public.flashback_quizzes;
drop policy if exists "Service role full access to quizzes" on public.flashback_quizzes;
create policy "Service role full access to quizzes" on public.flashback_quizzes
as permissive
for all
to public
using (auth.role() = 'service_role'::text);

drop policy if exists flashback_events_service_role_insert on public.flashback_events;
drop policy if exists flashback_events_service_role_update on public.flashback_events;
drop policy if exists flashback_events_service_role_delete on public.flashback_events;
drop policy if exists "Service role full access to events" on public.flashback_events;
create policy "Service role full access to events" on public.flashback_events
as permissive
for all
to public
using (auth.role() = 'service_role'::text);

drop policy if exists responses_select_access on firebase_surveys.responses;
drop policy if exists responses_insert_access on firebase_surveys.responses;
drop policy if exists responses_update_access on firebase_surveys.responses;
drop policy if exists responses_delete_access on firebase_surveys.responses;
drop policy if exists responses_admin_all on firebase_surveys.responses;
drop policy if exists responses_select_own on firebase_surveys.responses;
drop policy if exists responses_insert_own on firebase_surveys.responses;
drop policy if exists responses_update_own on firebase_surveys.responses;

create policy responses_admin_all on firebase_surveys.responses
as permissive
for all
to public
using ((select current_setting('app.is_admin'::text, true)) = 'true'::text);

create policy responses_insert_own on firebase_surveys.responses
as permissive
for insert
to public
with check (user_id = (select current_setting('app.firebase_uid'::text, true)));

create policy responses_select_own on firebase_surveys.responses
as permissive
for select
to public
using (user_id = (select current_setting('app.firebase_uid'::text, true)));

create policy responses_update_own on firebase_surveys.responses
as permissive
for update
to public
using (user_id = (select current_setting('app.firebase_uid'::text, true)));

drop policy if exists answers_select_access on firebase_surveys.answers;
drop policy if exists answers_insert_access on firebase_surveys.answers;
drop policy if exists answers_update_access on firebase_surveys.answers;
drop policy if exists answers_delete_access on firebase_surveys.answers;
drop policy if exists answers_admin_all on firebase_surveys.answers;
drop policy if exists answers_select_own on firebase_surveys.answers;
drop policy if exists answers_insert_own on firebase_surveys.answers;
drop policy if exists answers_update_own on firebase_surveys.answers;

create policy answers_admin_all on firebase_surveys.answers
as permissive
for all
to public
using ((select current_setting('app.is_admin'::text, true)) = 'true'::text);

create policy answers_insert_own on firebase_surveys.answers
as permissive
for insert
to public
with check (
  exists (
    select 1
    from firebase_surveys.responses r
    where r.id = answers.response_id
      and r.user_id = (select current_setting('app.firebase_uid'::text, true))
  )
);

create policy answers_select_own on firebase_surveys.answers
as permissive
for select
to public
using (
  exists (
    select 1
    from firebase_surveys.responses r
    where r.id = answers.response_id
      and r.user_id = (select current_setting('app.firebase_uid'::text, true))
  )
);

create policy answers_update_own on firebase_surveys.answers
as permissive
for update
to public
using (
  exists (
    select 1
    from firebase_surveys.responses r
    where r.id = answers.response_id
      and r.user_id = (select current_setting('app.firebase_uid'::text, true))
  )
);
