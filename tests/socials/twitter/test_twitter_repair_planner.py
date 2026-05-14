from __future__ import annotations

from trr_backend.socials.twitter.repair_planner import build_twitter_repair_plan


def test_repair_planner_ranks_by_expected_unique_gain() -> None:
    plan = build_twitter_repair_plan(
        catalog_rows=[
            {"source_id": "small", "replies_count": 10, "quotes_count": 0},
            {"source_id": "large", "replies_count": 200, "quotes_count": 0},
        ],
        saved_replies_by_root={"small": 5, "large": 50},
    )

    assert [(item.root_source_id, item.interaction_kind, item.raw_missing) for item in plan] == [
        ("large", "reply", 150),
        ("small", "reply", 5),
    ]


def test_repair_planner_suppresses_exhausted_state_by_default() -> None:
    plan = build_twitter_repair_plan(
        catalog_rows=[{"source_id": "root", "replies_count": 20, "quotes_count": 0}],
        saved_replies_by_root={"root": 3},
        interaction_states=[
            {
                "root_source_id": "root",
                "interaction_kind": "reply",
                "status": "exhausted",
                "exhaustion_reason": "x_search_exhausted",
            }
        ],
    )

    assert plan == []


def test_repair_planner_can_include_suppressed_exhausted_state_for_audit() -> None:
    plan = build_twitter_repair_plan(
        catalog_rows=[{"source_id": "root", "replies_count": 20, "quotes_count": 0}],
        saved_replies_by_root={"root": 3},
        interaction_states=[{"root_source_id": "root", "interaction_kind": "reply", "status": "exhausted"}],
        include_suppressed=True,
    )

    assert len(plan) == 1
    assert plan[0].suppressed is True
    assert plan[0].suppression_reason == "exhausted"
    assert plan[0].actionable_missing == 0
    assert plan[0].exhausted_missing == 17


def test_repair_planner_suppresses_duplicate_heavy_low_yield_state() -> None:
    plan = build_twitter_repair_plan(
        catalog_rows=[{"source_id": "root", "replies_count": 787, "quotes_count": 792}],
        saved_replies_by_root={"root": 604},
        saved_quotes_by_root={"root": 580},
        interaction_states=[
            {
                "root_source_id": "root",
                "interaction_kind": "quote",
                "status": "completed",
                "unique_saved_delta": 2,
                "duplicate_count": 1183,
            }
        ],
    )

    assert [(item.interaction_kind, item.raw_missing) for item in plan] == [("reply", 183)]


def test_repair_planner_force_keeps_low_yield_candidates() -> None:
    plan = build_twitter_repair_plan(
        catalog_rows=[{"source_id": "root", "replies_count": 0, "quotes_count": 792}],
        saved_quotes_by_root={"root": 580},
        interaction_states=[
            {
                "root_source_id": "root",
                "interaction_kind": "quote",
                "status": "completed",
                "unique_saved_delta": 2,
                "duplicate_count": 1183,
            }
        ],
        force=True,
    )

    assert len(plan) == 1
    assert plan[0].suppressed is False
    assert plan[0].interaction_kind == "quote"
