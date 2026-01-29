BEGIN;

-- Transactional RPC for submitting a survey response with all answers
-- Uses auth.uid() internally - does NOT accept user_id from client
-- Returns the response_id on success

CREATE OR REPLACE FUNCTION surveys.submit_response(
  p_survey_id uuid,
  p_answers jsonb
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = surveys, auth, public
AS $$
DECLARE
  v_user_id uuid;
  v_response_id uuid;
  v_survey_status text;
  v_answer record;
BEGIN
  -- Get authenticated user (can be NULL for anonymous)
  v_user_id := auth.uid();

  -- Verify survey exists and is published
  SELECT status INTO v_survey_status
  FROM surveys.surveys
  WHERE id = p_survey_id;

  IF v_survey_status IS NULL THEN
    RAISE EXCEPTION 'Survey not found: %', p_survey_id;
  END IF;

  IF v_survey_status != 'published' THEN
    RAISE EXCEPTION 'Survey is not accepting responses (status: %)', v_survey_status;
  END IF;

  -- Check if user already submitted (only for authenticated users)
  IF v_user_id IS NOT NULL THEN
    IF EXISTS (
      SELECT 1 FROM surveys.responses
      WHERE survey_id = p_survey_id AND user_id = v_user_id
    ) THEN
      RAISE EXCEPTION 'You have already submitted a response to this survey';
    END IF;
  END IF;

  -- Create response record
  INSERT INTO surveys.responses (survey_id, user_id, submitted_at)
  VALUES (p_survey_id, v_user_id, now())
  RETURNING id INTO v_response_id;

  -- Insert all answers from the JSONB array
  -- Expected format: [{"question_id": "uuid", "answer": {...}}, ...]
  FOR v_answer IN SELECT * FROM jsonb_array_elements(p_answers)
  LOOP
    INSERT INTO surveys.answers (survey_id, response_id, question_id, answer)
    VALUES (
      p_survey_id,
      v_response_id,
      (v_answer.value->>'question_id')::uuid,
      v_answer.value->'answer'
    );
  END LOOP;

  RETURN v_response_id;
END;
$$;

-- Grant to authenticated users (anonymous can also call since we handle NULL user_id)
GRANT EXECUTE ON FUNCTION surveys.submit_response(uuid, jsonb) TO authenticated;
GRANT EXECUTE ON FUNCTION surveys.submit_response(uuid, jsonb) TO anon;

COMMENT ON FUNCTION surveys.submit_response(uuid, jsonb) IS
  'Atomically submits a survey response with all answers. Uses auth.uid() for user identification. Prevents duplicate submissions for authenticated users.';

COMMIT;
