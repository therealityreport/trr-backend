"""Tests for survey submission endpoint."""

from __future__ import annotations

from uuid import uuid4

from api.routers import surveys


def test_submit_survey_uses_rpc():
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


def test_compute_survey_results_uses_sql_aggregation(monkeypatch):
    survey_id = uuid4()

    monkeypatch.setattr(
        surveys.pg,
        "fetch_all",
        lambda query, params: [
            {
                "question_id": "question-1",
                "total_responses": 4,
                "distribution": {"yes": 3, "no": 1},
                "survey_total_responses": 4,
            }
        ],
    )

    payload = surveys._compute_survey_results(object(), survey_id)

    assert payload == {
        "survey_id": str(survey_id),
        "total_responses": 4,
        "questions": [
            {
                "question_id": "question-1",
                "total_responses": 4,
                "distribution": {"yes": 3, "no": 1},
                "percentages": {"yes": 75.0, "no": 25.0},
            }
        ],
    }


def test_update_aggregates_batches_rows(monkeypatch):
    captured: dict[str, object] = {}
    survey_id = uuid4()

    def fake_execute(query: str, rows: list[tuple[object, ...]], conn=None):
        captured["query"] = query
        captured["rows"] = rows
        captured["conn"] = conn

    monkeypatch.setattr(surveys.pg, "execute_values_no_return", fake_execute)

    surveys._update_aggregates(
        object(),
        survey_id,
        {
            "questions": [
                {
                    "question_id": "question-1",
                    "total_responses": 4,
                    "distribution": {"yes": 3, "no": 1},
                    "percentages": {"yes": 75.0, "no": 25.0},
                }
            ]
        },
    )

    assert "INSERT INTO surveys.aggregates" in str(captured["query"])
    assert captured["rows"] == [
        (
            str(survey_id),
            "question-1",
            {
                "total_responses": 4,
                "distribution": {"yes": 3, "no": 1},
                "percentages": {"yes": 75.0, "no": 25.0},
            },
        )
    ]
