BEGIN;

-- Enforce one response per user per survey
-- Users who are not authenticated (user_id = NULL via anonymous) are NOT constrained
-- Only authenticated users (user_id NOT NULL) get the uniqueness check

CREATE UNIQUE INDEX IF NOT EXISTS survey_responses_one_per_user_per_survey
  ON surveys.responses (survey_id, user_id)
  WHERE user_id IS NOT NULL;

COMMENT ON INDEX surveys.survey_responses_one_per_user_per_survey IS
  'Ensures authenticated users can only submit one response per survey. Anonymous submissions (user_id NULL) are not constrained.';

COMMIT;
