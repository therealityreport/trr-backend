\i scripts/db/guard_core_schema.sql

\echo 'Wave 2 surveys representative query'
explain (analyze, buffers)
with target_survey as (
  select id
  from surveys.surveys
  order by updated_at desc nulls last, created_at desc
  limit 1
),
response_totals as (
  select count(*)::int as total_responses
  from surveys.responses
  where survey_id = (select id from target_survey)
),
expanded_answers as (
  select
    a.question_id::text as question_id,
    case
      when jsonb_typeof(a.answer->'value') = 'array' then array_item.value
      else a.answer->>'value'
    end as answer_value
  from surveys.answers as a
  left join lateral jsonb_array_elements_text(a.answer->'value') as array_item(value)
    on jsonb_typeof(a.answer->'value') = 'array'
  where a.survey_id = (select id from target_survey)
)
select
  rt.total_responses,
  count(*)::int as answer_rows
from expanded_answers ea
cross join response_totals rt
where ea.answer_value is not null
group by rt.total_responses;

\echo 'Wave 2 ml representative query'
explain (analyze, buffers)
select
  r.id,
  va.show_id,
  va.season_id,
  va.episode_id
from ml.screentime_runs r
join ml.analysis_media_assets va on va.id = r.video_asset_id
order by r.completed_at desc nulls last, r.created_at desc
limit 50;

\echo 'Wave 2 pipeline representative query'
explain (analyze, buffers)
with target as (
  select
    platform,
    account_handle,
    person_id
  from pipeline.socialblade_growth_data
  where account_handle is not null
  order by updated_at desc nulls last, created_at desc
  limit 1
)
select
  gd.id,
  gd.platform,
  gd.account_handle,
  gd.person_id,
  gd.updated_at
from pipeline.socialblade_growth_data gd
join target t on t.platform = gd.platform and t.account_handle = gd.account_handle
where gd.person_id = t.person_id
   or gd.person_id is null
order by
  case when gd.person_id = t.person_id then 0 else 1 end,
  gd.updated_at desc nulls last,
  gd.created_at desc
limit 50;

-- ============================================================================
-- Wave 2 deferred candidates (firebase_surveys + screenalytics)
-- Authored as PREPARE/EXECUTE pairs so the FK column is exercised as the $1
-- bind parameter (matching the task-1.1 template) while remaining executable
-- under the parse check (`scripts/db/run_sql.sh`).
-- ============================================================================

-- ============================================================================
-- firebase_surveys: answers.question_id -> firebase_surveys.questions
-- drives firebase_surveys_answers_question_id_idx
-- reader path: FK referential integrity only
--   (no greppable runtime reader for firebase_surveys.* outside migrations and
--    docs; index value is parent DELETE/UPDATE scan support on
--    firebase_surveys.questions)
-- ============================================================================
\echo 'Wave 2 firebase_surveys.answers.question_id deferred query-check'
prepare wave2_fs_answers_question_id (uuid) as
select a.id, a.question_id
  from firebase_surveys.answers a
  join firebase_surveys.questions q on q.id = a.question_id
 where a.question_id = $1;
explain (analyze, buffers)
execute wave2_fs_answers_question_id (
  (select id from firebase_surveys.questions order by id limit 1)
);
deallocate wave2_fs_answers_question_id;

-- ============================================================================
-- firebase_surveys: answers.option_id -> firebase_surveys.options
-- drives firebase_surveys_answers_option_id_idx
-- reader path: FK referential integrity only
--   (option_id is nullable; index value is parent DELETE/UPDATE scan support on
--    firebase_surveys.options when an option row is removed)
-- ============================================================================
\echo 'Wave 2 firebase_surveys.answers.option_id deferred query-check'
prepare wave2_fs_answers_option_id (uuid) as
select a.id, a.option_id
  from firebase_surveys.answers a
  join firebase_surveys.options o on o.id = a.option_id
 where a.option_id = $1;
explain (analyze, buffers)
execute wave2_fs_answers_option_id (
  (select id from firebase_surveys.options order by id limit 1)
);
deallocate wave2_fs_answers_option_id;

-- ============================================================================
-- screenalytics: face_bank_images.media_asset_id -> core.media_assets
-- drives screenalytics_face_bank_images_media_asset_id_idx
-- reader path: FK referential integrity only
--   (face_bank_images is donor/bridge input per
--    docs/ai/local-status/screenalytics-decommission-ledger.md:45-46;
--    bridge backfill at
--    supabase/migrations/20260403021500_face_reference_identity_reset_phase2.sql:82
--    reads via fbi.image_id, not media_asset_id; index value is parent
--    DELETE/UPDATE scan support on core.media_assets)
-- ============================================================================
\echo 'Wave 2 screenalytics.face_bank_images.media_asset_id deferred query-check'
prepare wave2_sa_fbi_media_asset_id (uuid) as
select fbi.image_id, fbi.media_asset_id
  from screenalytics.face_bank_images fbi
  join core.media_assets ma on ma.id = fbi.media_asset_id
 where fbi.media_asset_id = $1;
explain (analyze, buffers)
execute wave2_sa_fbi_media_asset_id (
  (select id from core.media_assets order by id limit 1)
);
deallocate wave2_sa_fbi_media_asset_id;

-- ============================================================================
-- screenalytics: identity_locks.run_id -> screenalytics.runs
-- drives screenalytics_identity_locks_run_id_idx
-- reader path: FK referential integrity only
--   (identity_locks has no Python reader in trr_backend/ or api/; index value
--    is parent DELETE/UPDATE scan support on screenalytics.runs when a legacy
--    run is purged)
-- ============================================================================
\echo 'Wave 2 screenalytics.identity_locks.run_id deferred query-check'
prepare wave2_sa_identity_locks_run_id (uuid) as
select il.id, il.run_id
  from screenalytics.identity_locks il
  join screenalytics.runs r on r.id = il.run_id
 where il.run_id = $1;
explain (analyze, buffers)
execute wave2_sa_identity_locks_run_id (
  (select id from screenalytics.runs order by id limit 1)
);
deallocate wave2_sa_identity_locks_run_id;

-- ============================================================================
-- screenalytics: media_upload_sessions.episode_id -> core.episodes
-- drives screenalytics_media_upload_sessions_episode_id_idx
-- reader path: FK referential integrity only
--   (media_upload_sessions listed as definite legacy table per
--    docs/ai/local-status/screenalytics-decommission-ledger.md:105;
--    index value is parent DELETE/UPDATE scan support on core.episodes)
-- ============================================================================
\echo 'Wave 2 screenalytics.media_upload_sessions.episode_id deferred query-check'
prepare wave2_sa_mus_episode_id (uuid) as
select mus.id, mus.episode_id
  from screenalytics.media_upload_sessions mus
  join core.episodes e on e.id = mus.episode_id
 where mus.episode_id = $1;
explain (analyze, buffers)
execute wave2_sa_mus_episode_id (
  (select id from core.episodes order by id limit 1)
);
deallocate wave2_sa_mus_episode_id;

-- ============================================================================
-- screenalytics: media_upload_sessions.promoted_video_asset_id
--   -> screenalytics.video_assets
-- drives screenalytics_media_upload_sessions_promoted_video_asset_id_idx
-- reader path: FK referential integrity only
--   (promoted_video_asset_id is nullable bridge column declared at
--    supabase/migrations/0181_cast_screentime_control_plane.sql:67;
--    media_upload_sessions is legacy per decommission ledger; index value is
--    parent DELETE/UPDATE scan support on screenalytics.video_assets)
-- ============================================================================
\echo 'Wave 2 screenalytics.media_upload_sessions.promoted_video_asset_id deferred query-check'
prepare wave2_sa_mus_promoted_va_id (uuid) as
select mus.id, mus.promoted_video_asset_id
  from screenalytics.media_upload_sessions mus
  join screenalytics.video_assets va on va.id = mus.promoted_video_asset_id
 where mus.promoted_video_asset_id = $1;
explain (analyze, buffers)
execute wave2_sa_mus_promoted_va_id (
  (select id from screenalytics.video_assets order by id limit 1)
);
deallocate wave2_sa_mus_promoted_va_id;

-- ============================================================================
-- screenalytics: media_upload_sessions.season_id -> core.seasons
-- drives screenalytics_media_upload_sessions_season_id_idx
-- reader path: FK referential integrity only
--   (season_id is nullable; media_upload_sessions is legacy per
--    docs/ai/local-status/screenalytics-decommission-ledger.md:105;
--    index value is parent DELETE/UPDATE scan support on core.seasons)
-- ============================================================================
\echo 'Wave 2 screenalytics.media_upload_sessions.season_id deferred query-check'
prepare wave2_sa_mus_season_id (uuid) as
select mus.id, mus.season_id
  from screenalytics.media_upload_sessions mus
  join core.seasons s on s.id = mus.season_id
 where mus.season_id = $1;
explain (analyze, buffers)
execute wave2_sa_mus_season_id (
  (select id from core.seasons order by id limit 1)
);
deallocate wave2_sa_mus_season_id;

-- ============================================================================
-- screenalytics: suggestion_applies.suggestion_id -> screenalytics.suggestions
-- drives screenalytics_suggestion_applies_suggestion_id_idx
-- reader path: FK referential integrity only
--   (suggestion_id is nullable; no greppable runtime reader; index value is
--    parent DELETE/UPDATE scan support on screenalytics.suggestions)
-- ============================================================================
\echo 'Wave 2 screenalytics.suggestion_applies.suggestion_id deferred query-check'
prepare wave2_sa_sa_suggestion_id (uuid) as
select sa.id, sa.suggestion_id
  from screenalytics.suggestion_applies sa
  join screenalytics.suggestions s on s.id = sa.suggestion_id
 where sa.suggestion_id = $1;
explain (analyze, buffers)
execute wave2_sa_sa_suggestion_id (
  (select id from screenalytics.suggestions order by id limit 1)
);
deallocate wave2_sa_sa_suggestion_id;

-- ============================================================================
-- screenalytics: suggestion_batches.run_id -> screenalytics.runs
-- drives screenalytics_suggestion_batches_run_id_idx
-- reader path: FK referential integrity only
--   (no greppable runtime reader for suggestion_batches; index value is parent
--    DELETE/UPDATE scan support on screenalytics.runs)
-- ============================================================================
\echo 'Wave 2 screenalytics.suggestion_batches.run_id deferred query-check'
prepare wave2_sa_sb_run_id (uuid) as
select sb.id, sb.run_id
  from screenalytics.suggestion_batches sb
  join screenalytics.runs r on r.id = sb.run_id
 where sb.run_id = $1;
explain (analyze, buffers)
execute wave2_sa_sb_run_id (
  (select id from screenalytics.runs order by id limit 1)
);
deallocate wave2_sa_sb_run_id;

-- ============================================================================
-- screenalytics: unknown_clusters.assigned_person_id -> core.people
-- drives screenalytics_unknown_clusters_assigned_person_id_idx
-- reader path: FK referential integrity only
--   (unknown_clusters listed as legacy per
--    docs/ai/local-status/screenalytics-decommission-ledger.md:119;
--    assigned_person_id is nullable; index value is parent DELETE/UPDATE scan
--    support on core.people)
-- ============================================================================
\echo 'Wave 2 screenalytics.unknown_clusters.assigned_person_id deferred query-check'
prepare wave2_sa_uc_assigned_person_id (uuid) as
select uc.id, uc.assigned_person_id
  from screenalytics.unknown_clusters uc
  join core.people p on p.id = uc.assigned_person_id
 where uc.assigned_person_id = $1;
explain (analyze, buffers)
execute wave2_sa_uc_assigned_person_id (
  (select id from core.people order by id limit 1)
);
deallocate wave2_sa_uc_assigned_person_id;

-- ============================================================================
-- screenalytics: video_assets.media_asset_id -> core.media_assets
-- drives screenalytics_video_assets_media_asset_id_idx
--   (partial: media_asset_id is not null)
-- reader path: bridge backfill of legacy screenalytics.video_assets rows into
--   ml.analysis_media_assets at
--   supabase/migrations/20260402233000_cast_screentime_phase1_asset_contract_freeze.sql:42
--   and supabase/migrations/20260403222500_cast_screentime_phase1_followup_repairs.sql:50
--   (video_assets is legacy bridge input only per
--    docs/ai/local-status/screenalytics-decommission-ledger.md:37-38).
-- ============================================================================
\echo 'Wave 2 screenalytics.video_assets.media_asset_id deferred query-check'
prepare wave2_sa_va_media_asset_id (uuid) as
select va.id, va.media_asset_id
  from screenalytics.video_assets va
  join core.media_assets ma on ma.id = va.media_asset_id
 where va.media_asset_id = $1
   and va.media_asset_id is not null;
explain (analyze, buffers)
execute wave2_sa_va_media_asset_id (
  (select id from core.media_assets order by id limit 1)
);
deallocate wave2_sa_va_media_asset_id;
