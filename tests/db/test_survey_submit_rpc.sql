-- Test survey submission RPC
-- Run with: psql "postgresql://supabase_admin:postgres@127.0.0.1:55432/postgres" -f tests/db/test_survey_submit_rpc.sql
--
-- NOTE: Must run as supabase_admin to be able to:
--   1. Create test user in auth.users
--   2. Mock auth.uid() function
--
-- The tests validate:
--   1. Survey not found error
--   2. Survey not published error
--   3. Successful submission creates response
--   4. Response record has correct user_id and submitted_at
--   5. Answer record is created
--   6. Duplicate submission prevention

BEGIN;

-- ============================================================================
-- PART 1: Test validation logic (no INSERT needed)
-- ============================================================================

DO $$
DECLARE
  v_fake_survey_id uuid := gen_random_uuid();
  v_draft_survey_id uuid;
  v_response_id uuid;
BEGIN
  -- Create a draft survey for testing
  INSERT INTO surveys.surveys (id, show_id, season_id, episode_id, title, status, starts_at)
  VALUES (
    gen_random_uuid(),
    'd1fdacc4-ccb0-4d52-8096-89889db83282',  -- Seed show
    '2ea88321-cb37-4527-892f-0441030b6e68',  -- Seed season
    '3d037712-54b6-4037-8109-1c69ab00448a',  -- Seed episode
    'Draft Test Survey',
    'draft',
    now()
  )
  RETURNING id INTO v_draft_survey_id;

  -- Test 1: Survey not found
  BEGIN
    v_response_id := surveys.submit_response(
      v_fake_survey_id,
      '[]'::jsonb
    );
    RAISE EXCEPTION 'TEST 1 FAILED: Expected "Survey not found" error';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLERRM LIKE 'Survey not found%' THEN
        RAISE NOTICE 'Test 1 PASSED: Survey not found error raised correctly';
      ELSE
        RAISE EXCEPTION 'TEST 1 FAILED: Unexpected error: %', SQLERRM;
      END IF;
  END;

  -- Test 2: Survey not published
  BEGIN
    v_response_id := surveys.submit_response(
      v_draft_survey_id,
      '[]'::jsonb
    );
    RAISE EXCEPTION 'TEST 2 FAILED: Expected "Survey is not accepting responses" error';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLERRM LIKE 'Survey is not accepting responses%' THEN
        RAISE NOTICE 'Test 2 PASSED: Draft survey rejection works correctly';
      ELSE
        RAISE EXCEPTION 'TEST 2 FAILED: Unexpected error: %', SQLERRM;
      END IF;
  END;

  RAISE NOTICE 'Part 1 complete: Validation tests passed';
END $$;


-- ============================================================================
-- PART 2: Test successful submission (requires mock auth.uid)
-- ============================================================================

-- Create the test user in auth.users (required for FK constraint)
INSERT INTO auth.users (id, instance_id, email, aud, role, encrypted_password, raw_app_meta_data, raw_user_meta_data, created_at, updated_at)
VALUES (
  'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
  '00000000-0000-0000-0000-000000000000',
  'test-survey-submit@example.com',
  'authenticated',
  'authenticated',
  '',
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{}'::jsonb,
  now(),
  now()
)
ON CONFLICT (id) DO NOTHING;

-- Create a mock auth.uid() function that returns a test user
CREATE OR REPLACE FUNCTION auth.uid()
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
  SELECT 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid;
$$;

DO $$
DECLARE
  v_survey_id uuid := '8a24c95d-93bc-4297-9c84-7946b753eb2d';  -- Seed survey
  v_question_id uuid := 'c5106823-7875-43d9-9172-4fbaa076a2b9'; -- First question (MVP vote)
  v_option_value text := 'ava';
  v_response_id uuid;
  v_answer_count int;
  v_submitted_at timestamptz;
  v_stored_user_id uuid;
BEGIN
  -- Test 3: Submit response with valid survey
  v_response_id := surveys.submit_response(
    v_survey_id,
    jsonb_build_array(
      jsonb_build_object(
        'question_id', v_question_id,
        'answer', jsonb_build_object('selected_option', v_option_value)
      )
    )
  );

  IF v_response_id IS NULL THEN
    RAISE EXCEPTION 'TEST 3 FAILED: submit_response returned NULL';
  END IF;

  RAISE NOTICE 'Test 3 PASSED: Response created with id %', v_response_id;

  -- Test 4: Verify response record
  SELECT submitted_at, user_id INTO v_submitted_at, v_stored_user_id
  FROM surveys.responses
  WHERE id = v_response_id;

  IF v_submitted_at IS NULL THEN
    RAISE EXCEPTION 'TEST 4 FAILED: submitted_at was not set';
  END IF;

  IF v_stored_user_id != 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid THEN
    RAISE EXCEPTION 'TEST 4 FAILED: user_id mismatch. Expected test user, got %', v_stored_user_id;
  END IF;

  RAISE NOTICE 'Test 4 PASSED: Response record verified (submitted_at set, user_id correct)';

  -- Test 5: Verify answer was created
  SELECT COUNT(*) INTO v_answer_count FROM surveys.answers WHERE response_id = v_response_id;

  IF v_answer_count != 1 THEN
    RAISE EXCEPTION 'TEST 5 FAILED: Expected 1 answer, got %', v_answer_count;
  END IF;

  RAISE NOTICE 'Test 5 PASSED: Answer record created';

  -- Test 6: Duplicate submission prevention
  BEGIN
    v_response_id := surveys.submit_response(
      v_survey_id,
      jsonb_build_array(
        jsonb_build_object(
          'question_id', v_question_id,
          'answer', jsonb_build_object('selected_option', 'ben')
        )
      )
    );
    RAISE EXCEPTION 'TEST 6 FAILED: Expected duplicate submission error';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLERRM LIKE '%already submitted%' THEN
        RAISE NOTICE 'Test 6 PASSED: Duplicate submission prevented';
      ELSE
        RAISE EXCEPTION 'TEST 6 FAILED: Unexpected error: %', SQLERRM;
      END IF;
  END;

  RAISE NOTICE 'Part 2 complete: Submission tests passed';
  RAISE NOTICE '';
  RAISE NOTICE '===========================================';
  RAISE NOTICE 'All tests passed!';
  RAISE NOTICE '===========================================';
END $$;

ROLLBACK; -- Don't persist test data (also restores original auth.uid)
