-- Performance Advisor RLS cleanup.
--
-- Goals:
-- - keep public SELECT behavior where it exists;
-- - replace broad service-role FOR ALL policies with command-specific writes;
-- - wrap stable auth/session helper calls in SELECT form for initplan reuse;
-- - disable legacy firebase_surveys app-role collection policies because
--   survey collection is moving through the Supabase-auth surveys.* path.
--
-- Note: legacy Firebase-authenticated survey tables are retained for migration
-- and audit access only. New survey submissions should use surveys.submit_response
-- and Supabase Auth semantics, not Firebase UID session-variable policies.

drop policy if exists core_tmdb_networks_service_role on core.networks;
create policy core_networks_service_role_insert on core.networks
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy core_networks_service_role_update on core.networks
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy core_networks_service_role_delete on core.networks
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists core_tmdb_production_companies_service_role on core.production_companies;
create policy core_production_companies_service_role_insert on core.production_companies
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy core_production_companies_service_role_update on core.production_companies
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy core_production_companies_service_role_delete on core.production_companies
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists core_show_watch_providers_service_role on core.show_watch_providers;
create policy core_show_watch_providers_service_role_insert on core.show_watch_providers
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy core_show_watch_providers_service_role_update on core.show_watch_providers
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy core_show_watch_providers_service_role_delete on core.show_watch_providers
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists core_tmdb_watch_providers_service_role on core.watch_providers;
create policy core_watch_providers_service_role_insert on core.watch_providers
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy core_watch_providers_service_role_update on core.watch_providers
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy core_watch_providers_service_role_delete on core.watch_providers
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists "Allow service role all on show_icons" on public.show_icons;
create policy show_icons_service_role_insert on public.show_icons
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy show_icons_service_role_update on public.show_icons
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy show_icons_service_role_delete on public.show_icons
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists "Service role full access to quizzes" on public.flashback_quizzes;
create policy flashback_quizzes_service_role_insert on public.flashback_quizzes
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy flashback_quizzes_service_role_update on public.flashback_quizzes
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy flashback_quizzes_service_role_delete on public.flashback_quizzes
for delete
to service_role
using ((select auth.role()) = 'service_role');

drop policy if exists "Service role full access to events" on public.flashback_events;
create policy flashback_events_service_role_insert on public.flashback_events
for insert
to service_role
with check ((select auth.role()) = 'service_role');
create policy flashback_events_service_role_update on public.flashback_events
for update
to service_role
using ((select auth.role()) = 'service_role')
with check ((select auth.role()) = 'service_role');
create policy flashback_events_service_role_delete on public.flashback_events
for delete
to service_role
using ((select auth.role()) = 'service_role');

-- Legacy Firebase survey collection is disabled at the RLS policy layer. The
-- old public policies are removed and no replacement app policies are created.
-- This clears the duplicate-permissive-policy advisor finding without carrying
-- forward Firebase UID behavior that should no longer collect responses.
drop policy if exists responses_admin_all on firebase_surveys.responses;
drop policy if exists responses_select_own on firebase_surveys.responses;
drop policy if exists responses_insert_own on firebase_surveys.responses;
drop policy if exists responses_update_own on firebase_surveys.responses;
drop policy if exists responses_select_access on firebase_surveys.responses;
drop policy if exists responses_insert_access on firebase_surveys.responses;
drop policy if exists responses_update_access on firebase_surveys.responses;
drop policy if exists responses_delete_access on firebase_surveys.responses;

drop policy if exists answers_admin_all on firebase_surveys.answers;
drop policy if exists answers_select_own on firebase_surveys.answers;
drop policy if exists answers_insert_own on firebase_surveys.answers;
drop policy if exists answers_update_own on firebase_surveys.answers;
drop policy if exists answers_select_access on firebase_surveys.answers;
drop policy if exists answers_insert_access on firebase_surveys.answers;
drop policy if exists answers_update_access on firebase_surveys.answers;
drop policy if exists answers_delete_access on firebase_surveys.answers;
