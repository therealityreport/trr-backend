from __future__ import annotations

from unittest.mock import patch

import pytest

from trr_backend.repositories import bravotv_image_runs


def test_get_latest_run_returns_none_when_relation_is_missing() -> None:
    with patch(
        "trr_backend.repositories.bravotv_image_runs.pg.fetch_one",
        return_value={"exists": False},
    ) as fetch_mock:
        row = bravotv_image_runs.get_latest_run(mode="show", target_show_id="show-1")

    assert row is None
    fetch_mock.assert_called_once_with(
        "select to_regclass(%s) is not null as exists",
        ["core.bravotv_image_runs"],
    )


def test_create_run_raises_clear_error_when_relation_is_missing() -> None:
    with patch("trr_backend.repositories.bravotv_image_runs.pg.fetch_one", return_value={"exists": False}):
        with pytest.raises(RuntimeError, match="Apply migration 0202_bravotv_image_runs.sql first"):
            bravotv_image_runs.create_run(
                mode="show",
                status="running",
                target_show_id=None,
                target_person_id=None,
                show_name="Watch What Happens Live",
                person_name=None,
                season=None,
                episode=None,
                selected_sources=["getty"],
            )
