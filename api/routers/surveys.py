"""
Survey endpoints with instant live results.

The key feature is that after submitting a survey response, the user
immediately gets the current aggregate results across all respondents.
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from api.auth import OptionalUser
from api.deps import (
    SupabaseAdminClient,
    SupabaseClient,
    get_list_result,
    raise_for_supabase_error,
    require_single_result,
)

router = APIRouter(prefix="/surveys", tags=["surveys"])


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
    query = db.schema("surveys").table("surveys").select("*")

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
    survey_response = db.schema("surveys").table("surveys").select("*").eq("id", str(survey_id)).single().execute()
    survey = require_single_result(survey_response, "Survey")

    # Get questions with options
    questions_response = (
        db.schema("surveys")
        .table("questions")
        .select("*, options(*)")
        .eq("survey_id", str(survey_id))
        .order("question_order")
        .execute()
    )
    questions = get_list_result(questions_response, "fetching survey questions")

    # Sort options within each question
    for q in questions:
        q["options"] = sorted(q.get("options", []), key=lambda x: x["option_order"])

    survey["questions"] = questions
    return survey


@router.get("/by-slug/{slug}", response_model=SurveyWithQuestions)
def get_survey_by_slug(db: SupabaseClient, slug: str) -> dict:
    """
    Get a survey by its URL-friendly slug.
    """
    # Get survey by slug
    survey_response = db.schema("surveys").table("surveys").select("*").eq("slug", slug).single().execute()
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
        if "not found" in error_msg:
            raise HTTPException(status_code=404, detail="Survey not found") from None
        if "not accepting" in error_msg:
            raise HTTPException(status_code=400, detail="Survey is not accepting responses") from None
        raise HTTPException(status_code=500, detail=f"Failed to submit survey: {e}") from e

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


def _compute_survey_results(db: SupabaseClient, survey_id: UUID) -> dict:
    """
    Compute aggregate results for a survey from the answers table.
    """
    # Get all answers for this survey
    answers_response = (
        db.schema("surveys").table("answers").select("question_id, answer").eq("survey_id", str(survey_id)).execute()
    )
    answers = get_list_result(answers_response, "fetching survey answers")

    # Get questions for context
    questions_response = (
        db.schema("surveys").table("questions").select("id, question_type").eq("survey_id", str(survey_id)).execute()
    )
    questions_list = get_list_result(questions_response, "fetching survey questions")
    questions = {q["id"]: q for q in questions_list}

    # Group answers by question
    question_answers: dict[str, list] = {}
    for ans in answers:
        qid = ans["question_id"]
        if qid not in question_answers:
            question_answers[qid] = []
        question_answers[qid].append(ans["answer"])

    # Get unique response count
    responses_response = db.schema("surveys").table("responses").select("id").eq("survey_id", str(survey_id)).execute()
    responses_list = get_list_result(responses_response, "counting survey responses")
    total_responses = len(responses_list)

    # Compute aggregates per question
    question_aggregates = []
    for qid, answers_list in question_answers.items():
        question_type = questions.get(qid, {}).get("question_type", "single_choice")
        aggregate = _compute_question_aggregate(qid, answers_list, question_type)
        question_aggregates.append(aggregate)

    return {
        "survey_id": str(survey_id),
        "total_responses": total_responses,
        "questions": question_aggregates,
    }


def _compute_question_aggregate(
    question_id: str,
    answers: list[dict],
    question_type: str,
) -> dict:
    """
    Compute aggregate for a single question.
    """
    total = len(answers)

    # Extract values from answer objects
    values = []
    for ans in answers:
        if isinstance(ans, dict):
            val = ans.get("value")
            if isinstance(val, list):
                values.extend(val)  # multiple_choice
            else:
                values.append(val)
        else:
            values.append(ans)

    # Count distribution
    distribution = dict(Counter(str(v) for v in values if v is not None))

    # Calculate percentages
    percentages = {}
    if total > 0:
        for key, count in distribution.items():
            percentages[key] = round(count / total * 100, 1)

    return {
        "question_id": question_id,
        "total_responses": total,
        "distribution": distribution,
        "percentages": percentages,
    }


def _update_aggregates(
    admin_db: SupabaseAdminClient,
    survey_id: UUID,
    results: dict,
) -> None:
    """
    Update the aggregates table with computed results (for caching).
    Uses upsert to handle both insert and update cases.
    """
    for question_result in results["questions"]:
        response = (
            admin_db.schema("surveys")
            .table("aggregates")
            .upsert(
                {
                    "survey_id": str(survey_id),
                    "question_id": question_result["question_id"],
                    "aggregate": {
                        "total_responses": question_result["total_responses"],
                        "distribution": question_result["distribution"],
                        "percentages": question_result["percentages"],
                    },
                },
                on_conflict="survey_id,question_id",
            )
            .execute()
        )
        raise_for_supabase_error(response, "updating survey aggregates")
