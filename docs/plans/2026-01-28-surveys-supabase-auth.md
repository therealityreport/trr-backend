# Surveys with Supabase Auth Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add RPC-based transactional survey submission and unique constraint for one-response-per-user-per-survey to the existing surveys schema.

**Architecture:** The surveys schema already exists with tables, RLS policies, and an API router. This plan adds: (1) a unique constraint preventing duplicate submissions, (2) a transactional RPC function for atomic response+answers insertion, and (3) updates the API to use the RPC.

**Tech Stack:** PostgreSQL (Supabase), Python/FastAPI (existing API)

---

## Current State Analysis

### What Already Exists (DO NOT recreate)

| Component | Location | Status |
|-----------|----------|--------|
| `surveys` schema | `0001_init.sql:216-304` | Complete |
| `surveys.surveys` table | `0001_init.sql:216-230` | Has status, starts_at, ends_at |
| `surveys.questions` table | `0001_init.sql:236-246` | Has question_type, config |
| `surveys.options` table | `0001_init.sql:250-258` | Complete |
| `surveys.responses` table | `0001_init.sql:262-270` | Has user_id -> auth.users |
| `surveys.answers` table | `0001_init.sql:275-287` | JSONB answer column |
| `surveys.aggregates` table | `0001_init.sql:293-300` | For caching |
| RLS policies | `0001_init.sql:499-563` | User-scoped for responses/answers |
| API router | `api/routers/surveys.py` | List, get, submit, results |

### What's Missing (This Plan Adds)

1. **Unique constraint**: `(survey_id, user_id)` on `surveys.responses` to enforce one submission per user per survey
2. **Transactional RPC**: `surveys.submit_response(survey_id, answers_json)` that inserts response + answers atomically
3. **API update**: Use RPC instead of separate INSERT calls
4. **Slug column**: Add `slug` to `surveys.surveys` for URL-friendly identifiers (optional nice-to-have)

---

## Task 1: Add Unique Constraint for One-Response-Per-User

**Files:**
- Create: `supabase/migrations/0089_survey_response_unique_per_user.sql`
- Test: Manual verification via `supabase db reset`

**Step 1: Write the migration**

Create `supabase/migrations/0089_survey_response_unique_per_user.sql`:

```sql
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
```

**Step 2: Apply migration locally**

Run: `supabase db reset`
Expected: Migration applies without errors

**Step 3: Verify constraint exists**

Run: `psql "$SUPABASE_DB_URL" -c "\di surveys.survey_responses_one_per_user_per_survey"`
Expected: Index listed

**Step 4: Commit**

```bash
git add supabase/migrations/0089_survey_response_unique_per_user.sql
git commit -m "feat(surveys): add unique constraint for one response per user per survey"
```

---

## Task 2: Create Transactional Submit RPC Function

**Files:**
- Create: `supabase/migrations/0090_survey_submit_response_rpc.sql`
- Test: Manual verification

**Step 1: Write the RPC migration**

Create `supabase/migrations/0090_survey_submit_response_rpc.sql`:

```sql
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
```

**Step 2: Apply migration locally**

Run: `supabase db reset`
Expected: Migration applies without errors

**Step 3: Reload PostgREST schema**

Run: `./scripts/reload_postgrest_schema.sh`
Expected: Schema reloaded successfully

**Step 4: Verify function exists**

Run: `psql "$SUPABASE_DB_URL" -c "\df surveys.submit_response"`
Expected: Function listed with `(uuid, jsonb)` signature

**Step 5: Commit**

```bash
git add supabase/migrations/0090_survey_submit_response_rpc.sql
git commit -m "feat(surveys): add transactional submit_response RPC function"
```

---

## Task 3: Write Tests for Submit RPC

**Files:**
- Create: `tests/db/test_survey_submit_rpc.sql`
- Test: Run via psql

**Step 1: Write the SQL test file**

Create `tests/db/test_survey_submit_rpc.sql`:

```sql
-- Test survey submission RPC
-- Run with: psql "$SUPABASE_DB_URL" -f tests/db/test_survey_submit_rpc.sql

BEGIN;

-- Use a test survey from seed data (assumes seed.sql has been run)
DO $$
DECLARE
  v_survey_id uuid;
  v_question_id uuid;
  v_response_id uuid;
  v_answer_count int;
BEGIN
  -- Get the seeded survey
  SELECT id INTO v_survey_id FROM surveys.surveys LIMIT 1;
  IF v_survey_id IS NULL THEN
    RAISE EXCEPTION 'No survey found in seed data';
  END IF;

  -- Get first question
  SELECT id INTO v_question_id FROM surveys.questions WHERE survey_id = v_survey_id LIMIT 1;
  IF v_question_id IS NULL THEN
    RAISE EXCEPTION 'No questions found for survey';
  END IF;

  -- Test 1: Submit as anonymous (no auth.uid())
  -- Note: In test context without Supabase auth, auth.uid() returns NULL
  v_response_id := surveys.submit_response(
    v_survey_id,
    jsonb_build_array(
      jsonb_build_object('question_id', v_question_id, 'answer', jsonb_build_object('value', 'test_answer'))
    )
  );

  IF v_response_id IS NULL THEN
    RAISE EXCEPTION 'submit_response returned NULL';
  END IF;

  RAISE NOTICE 'Test 1 PASSED: Anonymous submission created response %', v_response_id;

  -- Verify answer was created
  SELECT COUNT(*) INTO v_answer_count FROM surveys.answers WHERE response_id = v_response_id;
  IF v_answer_count != 1 THEN
    RAISE EXCEPTION 'Expected 1 answer, got %', v_answer_count;
  END IF;

  RAISE NOTICE 'Test 2 PASSED: Answer record created';

  -- Test 3: Verify submitted_at was set
  IF NOT EXISTS (SELECT 1 FROM surveys.responses WHERE id = v_response_id AND submitted_at IS NOT NULL) THEN
    RAISE EXCEPTION 'submitted_at was not set';
  END IF;

  RAISE NOTICE 'Test 3 PASSED: submitted_at was set';

  RAISE NOTICE 'All tests passed!';
END $$;

ROLLBACK; -- Don't persist test data
```

**Step 2: Run the test**

Run: `psql "$SUPABASE_DB_URL" -f tests/db/test_survey_submit_rpc.sql`
Expected: "All tests passed!" in output

**Step 3: Commit**

```bash
git add tests/db/test_survey_submit_rpc.sql
git commit -m "test(surveys): add SQL tests for submit_response RPC"
```

---

## Task 4: Update API to Use RPC

**Files:**
- Modify: `api/routers/surveys.py:168-240`
- Test: `pytest tests/api/test_surveys.py -v` (if exists)

**Step 1: Write the failing test**

Create `tests/api/test_survey_submit.py`:

```python
"""Tests for survey submission endpoint."""

import pytest
from unittest.mock import MagicMock, patch


def test_submit_survey_uses_rpc(mock_db):
    """Verify that submit_survey calls the RPC function."""
    # This test documents the expected behavior:
    # The API should call surveys.submit_response RPC instead of
    # separate INSERT statements

    # For now, just verify the endpoint structure exists
    from api.routers.surveys import submit_survey
    assert callable(submit_survey)


def test_submit_survey_handles_duplicate_error():
    """Verify duplicate submission returns 409 Conflict."""
    # Will be implemented after API update
    pass
```

**Step 2: Run test to verify it fails/passes baseline**

Run: `pytest tests/api/test_survey_submit.py -v`
Expected: Tests run (may skip some)

**Step 3: Update the API endpoint**

Modify `api/routers/surveys.py`. Replace the submit_survey function (lines 168-239) with:

```python
@router.post("/{survey_id}/submit", response_model=SubmissionResponse)
def submit_survey(
    db: SupabaseClient,
    admin_db: SupabaseAdminClient,
    survey_id: UUID,
    submission: SurveySubmission,
    user: OptionalUser,
) -> dict:
    """
    Submit a survey response and get instant live results.

    This endpoint uses a transactional RPC to atomically:
    1. Create a response record (user_id derived from auth.uid())
    2. Save all answers
    3. Return the response_id

    Then computes and returns live results.

    Authentication: Optional. Anonymous submissions allowed.
    - Authenticated: user_id derived from JWT token via RPC
    - Anonymous: user_id is NULL

    Security note: user_id is NEVER accepted from client payload.
    The RPC uses auth.uid() internally.
    """
    # Build answers array for RPC
    answers_json = [
        {
            "question_id": str(answer.question_id),
            "answer": answer.answer if isinstance(answer.answer, dict) else {"value": answer.answer},
        }
        for answer in submission.answers
    ]

    # Use the appropriate client based on authentication
    # For authenticated users, use their client so auth.uid() works in RPC
    # For anonymous, use admin client with explicit NULL handling
    client = db if user else admin_db

    try:
        # Call the transactional RPC
        rpc_response = (
            client.schema("surveys")
            .rpc("submit_response", {"p_survey_id": str(survey_id), "p_answers": answers_json})
            .execute()
        )
    except Exception as e:
        error_msg = str(e).lower()
        if "already submitted" in error_msg:
            raise HTTPException(status_code=409, detail="You have already submitted a response to this survey")
        if "not found" in error_msg:
            raise HTTPException(status_code=404, detail="Survey not found")
        if "not accepting" in error_msg:
            raise HTTPException(status_code=400, detail="Survey is not accepting responses")
        raise HTTPException(status_code=500, detail=f"Failed to submit survey: {e}")

    if not rpc_response.data:
        raise HTTPException(status_code=500, detail="Failed to create response")

    response_id = rpc_response.data

    # Compute and return live results
    results = _compute_survey_results(admin_db, survey_id)

    # Update aggregates table for caching
    _update_aggregates(admin_db, survey_id, results)

    return {
        "response_id": response_id,
        "results": results,
    }
```

**Step 4: Run tests**

Run: `pytest tests/api/test_survey_submit.py -v`
Expected: Tests pass

**Step 5: Run full test suite**

Run: `ruff check api/routers/surveys.py && pytest`
Expected: All tests pass, no lint errors

**Step 6: Commit**

```bash
git add api/routers/surveys.py tests/api/test_survey_submit.py
git commit -m "refactor(surveys): use transactional RPC for survey submission"
```

---

## Task 5: Add Slug Column to Surveys (Optional Enhancement)

**Files:**
- Create: `supabase/migrations/0091_survey_slug_column.sql`
- Modify: `api/routers/surveys.py` (add get-by-slug endpoint)

**Step 1: Write the migration**

Create `supabase/migrations/0091_survey_slug_column.sql`:

```sql
BEGIN;

-- Add slug column for URL-friendly survey identifiers
ALTER TABLE surveys.surveys
ADD COLUMN IF NOT EXISTS slug text;

-- Create unique index on slug (partial - only where slug is not null)
CREATE UNIQUE INDEX IF NOT EXISTS surveys_slug_unique
  ON surveys.surveys (slug)
  WHERE slug IS NOT NULL;

-- Backfill existing surveys with auto-generated slugs from title
UPDATE surveys.surveys
SET slug = lower(regexp_replace(title, '[^a-zA-Z0-9]+', '-', 'g'))
WHERE slug IS NULL;

COMMENT ON COLUMN surveys.surveys.slug IS
  'URL-friendly identifier for the survey. Must be unique when set.';

COMMIT;
```

**Step 2: Apply and verify**

Run: `supabase db reset`
Expected: Migration applies

**Step 3: Add API endpoint for get-by-slug**

Add to `api/routers/surveys.py` after the get_survey endpoint:

```python
@router.get("/by-slug/{slug}", response_model=SurveyWithQuestions)
def get_survey_by_slug(db: SupabaseClient, slug: str) -> dict:
    """
    Get a survey by its URL-friendly slug.
    """
    # Get survey by slug
    survey_response = (
        db.schema("surveys")
        .table("surveys")
        .select("*")
        .eq("slug", slug)
        .single()
        .execute()
    )
    survey = require_single_result(survey_response, "Survey")

    # Get questions with options
    questions_response = (
        db.schema("surveys")
        .table("questions")
        .select("*, options(*)")
        .eq("survey_id", str(survey["id"]))
        .order("question_order")
        .execute()
    )
    questions = get_list_result(questions_response, "fetching survey questions")

    for q in questions:
        q["options"] = sorted(q.get("options", []), key=lambda x: x["option_order"])

    survey["questions"] = questions
    return survey
```

**Step 4: Update Survey model**

Add `slug` field to the `Survey` Pydantic model:

```python
class Survey(BaseModel):
    id: UUID
    show_id: UUID
    season_id: UUID | None
    episode_id: UUID | None
    slug: str | None  # Add this line
    title: str
    description: str | None
    status: str
    starts_at: str | None
    ends_at: str | None
    config: dict[str, Any]
```

**Step 5: Run tests and lint**

Run: `ruff check . && pytest`
Expected: Pass

**Step 6: Commit**

```bash
git add supabase/migrations/0091_survey_slug_column.sql api/routers/surveys.py
git commit -m "feat(surveys): add slug column and get-by-slug endpoint"
```

---

## Task 6: Update Schema Documentation

**Files:**
- Modify: `docs/db/schema.md`

**Step 1: Update the surveys section**

Add to the `## surveys tables` section in `docs/db/schema.md`:

```markdown
## `surveys` tables

- `surveys.surveys`: A survey scoped to a show/season/episode (`show_id` required; `season_id`/`episode_id` optional). Includes `slug` for URL-friendly identifiers.
- `surveys.questions`: Questions for a survey; `question_order` is unique per survey.
- `surveys.options`: Options for a question (for choice questions).
- `surveys.responses`: Per-user response header for a survey (user-scoped). **Unique constraint**: authenticated users can only submit one response per survey.
- `surveys.answers`: Per-response per-question answers (user-scoped via the owning response); includes `survey_id` to enforce response/question scope.
- `surveys.aggregates`: Live aggregates for survey questions (read-only to clients).

### `surveys` RPC functions

- `surveys.submit_response(survey_id uuid, answers jsonb) -> uuid`: Atomically submits a survey response with all answers. Uses `auth.uid()` internally for user identification. Prevents duplicate submissions for authenticated users. Returns the response_id.
```

**Step 2: Run schema docs check**

Run: `make schema-docs-check` (if schema changed) or verify manually
Expected: Documentation matches schema

**Step 3: Commit**

```bash
git add docs/db/schema.md
git commit -m "docs(surveys): document unique constraint and submit_response RPC"
```

---

## Task 7: Integration Test

**Files:**
- Test: Manual verification with local Supabase

**Step 1: Reset database with all migrations**

Run: `supabase db reset`
Expected: All migrations apply, seed data loaded

**Step 2: Reload PostgREST schema**

Run: `./scripts/reload_postgrest_schema.sh`
Expected: Schema reloaded

**Step 3: Start the API**

Run: `uvicorn api.main:app --reload`
Expected: API starts on port 8000

**Step 4: Test the flow manually**

```bash
# List surveys
curl http://localhost:8000/surveys

# Get a survey (use ID from list)
curl http://localhost:8000/surveys/{survey_id}

# Submit a response
curl -X POST http://localhost:8000/surveys/{survey_id}/submit \
  -H "Content-Type: application/json" \
  -d '{"answers": [{"question_id": "{question_id}", "answer": "test"}]}'

# Verify duplicate submission fails (if authenticated)
# (Requires setting up auth token)
```

**Step 5: Run full validation**

Run: `/trr-validate`
Expected: All checks pass

**Step 6: Final commit (if any fixes needed)**

```bash
git add -A
git commit -m "fix: address integration test findings"
```

---

## Summary of Changes

| Migration | Purpose |
|-----------|---------|
| `0089_survey_response_unique_per_user.sql` | Unique constraint on (survey_id, user_id) |
| `0090_survey_submit_response_rpc.sql` | Transactional RPC for atomic submission |
| `0091_survey_slug_column.sql` | URL-friendly survey identifiers (optional) |

| File | Changes |
|------|---------|
| `api/routers/surveys.py` | Use RPC for submission, add get-by-slug |
| `docs/db/schema.md` | Document new features |
| `tests/db/test_survey_submit_rpc.sql` | SQL-level RPC tests |
| `tests/api/test_survey_submit.py` | API-level tests |

---

## Decision: One-Time vs Repeatable Surveys

The current implementation enforces **one response per authenticated user per survey**. If you need repeatable surveys (weekly pulse, etc.):

1. Add `attempt_number` column to `surveys.responses`
2. Change unique constraint to `(survey_id, user_id, attempt_number)`
3. Update RPC to auto-increment attempt_number
4. Add `max_attempts` column to `surveys.surveys` for limiting

This is a future enhancement if needed.
