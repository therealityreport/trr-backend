"""
Survey endpoints with instant live results.

The key feature is that after submitting a survey response, the user
immediately gets the current aggregate results across all respondents.
"""

from __future__ import annotations

import time
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import OptionalUser
from api.deps import (
    SupabaseAdminClient,
    SupabaseClient,
    get_list_result,
    require_single_result,
)
from trr_backend.db import pg
from trr_backend.read_path_diagnostics import log_read_path

router = APIRouter(prefix="/surveys", tags=["surveys"])

SURVEY_SELECT_FIELDS = "id,show_id,season_id,episode_id,slug,title,description,status,starts_at,ends_at,config"
QUESTION_SELECT_FIELDS = "id,survey_id,question_order,prompt,question_type,config"
OPTION_SELECT_FIELDS = "id,question_id,option_order,label,value"
SURVEY_RESULTS_SQL = """
WITH response_totals AS (
  SELECT count(*)::int AS total_responses
  FROM surveys.responses
  WHERE survey_id = %s::uuid
),
expanded_answers AS (
  SELECT
    a.question_id::text AS question_id,
    CASE
      WHEN jsonb_typeof(a.answer->'value') = 'array' THEN array_item.value
      ELSE a.answer->>'value'
    END AS answer_value
  FROM surveys.answers AS a
  LEFT JOIN LATERAL jsonb_array_elements_text(a.answer->'value') AS array_item(value)
    ON jsonb_typeof(a.answer->'value') = 'array'
  WHERE a.survey_id = %s::uuid
),
distribution AS (
  SELECT
    question_id,
    answer_value,
    count(*)::int AS answer_count
  FROM expanded_answers
  WHERE answer_value IS NOT NULL
  GROUP BY question_id, answer_value
),
question_totals AS (
  SELECT
    question_id,
    count(*)::int AS total_responses
  FROM expanded_answers
  WHERE answer_value IS NOT NULL
  GROUP BY question_id
)
SELECT
  qt.question_id,
  qt.total_responses,
  COALESCE(
    jsonb_object_agg(d.answer_value, d.answer_count ORDER BY d.answer_value),
    '{}'::jsonb
  ) AS distribution,
  rt.total_responses AS survey_total_responses
FROM question_totals AS qt
LEFT JOIN distribution AS d
  ON d.question_id = qt.question_id
CROSS JOIN response_totals AS rt
GROUP BY qt.question_id, qt.total_responses, rt.total_responses
ORDER BY qt.question_id
"""


# --- Pydantic models ---


class SurveyOption(BaseModel):
    id: UUID
    question_id: UUID
    option_order: int
    label: str
    value: str | None


class SurveyQuestion(BaseModel):
    id: UUID
    survey_id: UUID
    question_order: int
    prompt: str
    question_type: str
    config: dict[str, Any]
    options: list[SurveyOption] = []


class Survey(BaseModel):
    id: UUID
    show_id: UUID
    season_id: UUID | None
    episode_id: UUID | None
    slug: str | None
    title: str
    description: str | None
    status: str
    starts_at: str | None
    ends_at: str | None
    config: dict[str, Any]


class SurveyWithQuestions(Survey):
    questions: list[SurveyQuestion] = []


class AnswerSubmission(BaseModel):
    question_id: UUID
    answer: Any  # Can be string, list, or number depending on question type


class SurveySubmission(BaseModel):
    """
    Survey submission payload.

    Note: user_id is intentionally NOT accepted from client.
    The server derives user identity from authentication (when implemented)
    or uses NULL for anonymous submissions.
    """

    answers: list[AnswerSubmission]


class QuestionAggregate(BaseModel):
    question_id: UUID
    total_responses: int
    distribution: dict[str, int]  # option_id/value -> count
    percentages: dict[str, float]  # option_id/value -> percentage


class SurveyResults(BaseModel):
    survey_id: UUID
    total_responses: int
    questions: list[QuestionAggregate]


class SubmissionResponse(BaseModel):
    response_id: UUID
    results: SurveyResults


def _load_survey_questions(db: SupabaseClient, survey_id: str) -> list[dict[str, Any]]:
    questions_response = (
        db.schema("surveys")
        .table("questions")
        .select(f"{QUESTION_SELECT_FIELDS}, options({OPTION_SELECT_FIELDS})")
        .eq("survey_id", survey_id)
        .order("question_order")
        .execute()
    )
    questions = get_list_result(questions_response, "fetching survey questions")
    for question in questions:
        question["options"] = sorted(question.get("options", []), key=lambda option: option["option_order"])
    return questions


def _message_indicates_missing_survey(error: object) -> bool:
    error_msg = str(error).lower()
    return any(marker in error_msg for marker in ("survey not found", "not found", "pgrst116", "0 rows", "no rows"))


def _survey_exists(admin_db: SupabaseAdminClient, survey_id: UUID) -> bool:
    try:
        response = admin_db.schema("surveys").table("surveys").select("id").eq("id", str(survey_id)).single().execute()
    except Exception as exc:
        if _message_indicates_missing_survey(exc):
            return False
        raise
    return bool(getattr(response, "data", None))


# --- Endpoints ---


@router.get("", response_model=list[Survey])
def list_surveys(
    db: SupabaseClient,
    show_id: UUID | None = Query(default=None),  # noqa: B008
    status: str = Query(default="published"),
    limit: int = Query(default=50, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """
    List surveys, optionally filtered by show and status.
    Only published surveys are returned by default.
    """
    query = db.schema("surveys").table("surveys").select(SURVEY_SELECT_FIELDS)

    if show_id:
        query = query.eq("show_id", str(show_id))
    if status:
        query = query.eq("status", status)

    response = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return get_list_result(response, "listing surveys")


@router.get("/{survey_id}", response_model=SurveyWithQuestions)
def get_survey(db: SupabaseClient, survey_id: UUID) -> dict:
    """
    Get a survey with all its questions and options.
    """
    # Get survey
    survey_response = (
        db.schema("surveys").table("surveys").select(SURVEY_SELECT_FIELDS).eq("id", str(survey_id)).single().execute()
    )
    survey = require_single_result(survey_response, "Survey")

    # Get questions with options
    survey["questions"] = _load_survey_questions(db, str(survey_id))
    return survey


@router.get("/by-slug/{slug}", response_model=SurveyWithQuestions)
def get_survey_by_slug(db: SupabaseClient, slug: str) -> dict:
    """
    Get a survey by its URL-friendly slug.
    """
    # Get survey by slug
    survey_response = (
        db.schema("surveys").table("surveys").select(SURVEY_SELECT_FIELDS).eq("slug", slug).single().execute()
    )
    survey = require_single_result(survey_response, "Survey")

    # Get questions with options
    survey["questions"] = _load_survey_questions(db, str(survey["id"]))
    return survey


@router.get("/{survey_id}/results", response_model=SurveyResults)
def get_survey_results(admin_db: SupabaseAdminClient, survey_id: UUID) -> dict:
    """
    Get current aggregate results for a survey.
    Results are computed live from the answers table.

    Uses admin client to bypass RLS on answers/responses tables,
    which have user-scoped policies that would block aggregate queries.
    """
    return _compute_survey_results(admin_db, survey_id)


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
    started_at = time.perf_counter()
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
            raise HTTPException(
                status_code=409, detail="You have already submitted a response to this survey"
            ) from None
        if _message_indicates_missing_survey(e):
            raise HTTPException(status_code=404, detail="Survey not found") from None
        if "not accepting" in error_msg:
            raise HTTPException(status_code=400, detail="Survey is not accepting responses") from None
        raise HTTPException(status_code=500, detail=f"Failed to submit survey: {e}") from e

    if not rpc_response.data:
        if not _survey_exists(admin_db, survey_id):
            raise HTTPException(status_code=404, detail="Survey not found")
        raise HTTPException(status_code=500, detail="Failed to create response")

    response_id = rpc_response.data

    # Compute and return live results
    results = _compute_survey_results(admin_db, survey_id)

    # Update aggregates table for caching
    _update_aggregates(admin_db, survey_id, results)

    payload = {
        "response_id": response_id,
        "results": results,
    }
    log_read_path(
        "surveys.submit",
        latency_ms=(time.perf_counter() - started_at) * 1000.0,
        query_count=3,
        payload=payload,
        extra={"answer_count": len(submission.answers), "survey_id": survey_id},
    )
    return payload


def _compute_survey_results(db: SupabaseClient, survey_id: UUID) -> dict:
    """
    Compute aggregate results for a survey from the answers table.
    """
    _ = db
    started_at = time.perf_counter()
    rows = pg.fetch_all(SURVEY_RESULTS_SQL, [str(survey_id), str(survey_id)])
    total_responses = int(rows[0]["survey_total_responses"]) if rows else 0
    question_aggregates = []
    for row in rows:
        distribution_raw = row.get("distribution") or {}
        distribution = {str(key): int(value) for key, value in distribution_raw.items()}
        question_total = int(row.get("total_responses") or 0)
        percentages = {}
        if question_total > 0:
            for key, count in distribution.items():
                percentages[key] = round(count / question_total * 100, 1)
        question_aggregates.append(
            {
                "question_id": row["question_id"],
                "total_responses": question_total,
                "distribution": distribution,
                "percentages": percentages,
            }
        )

    payload = {
        "survey_id": str(survey_id),
        "total_responses": total_responses,
        "questions": question_aggregates,
    }
    log_read_path(
        "surveys.results",
        latency_ms=(time.perf_counter() - started_at) * 1000.0,
        query_count=1,
        payload=payload,
        extra={"survey_id": survey_id},
    )
    return payload


def _update_aggregates(
    admin_db: SupabaseAdminClient,
    survey_id: UUID,
    results: dict,
) -> None:
    """
    Update the aggregates table with computed results (for caching).
    Uses upsert to handle both insert and update cases.
    """
    _ = admin_db
    rows = [
        (
            str(survey_id),
            question_result["question_id"],
            {
                "total_responses": question_result["total_responses"],
                "distribution": question_result["distribution"],
                "percentages": question_result["percentages"],
            },
        )
        for question_result in results["questions"]
    ]
    if not rows:
        return

    pg.execute_values_no_return(
        """
        INSERT INTO surveys.aggregates (survey_id, question_id, aggregate)
        VALUES %s
        ON CONFLICT (survey_id, question_id)
        DO UPDATE SET
          aggregate = EXCLUDED.aggregate,
          updated_at = now()
        """,
        rows,
    )
