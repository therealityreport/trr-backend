from __future__ import annotations

import json

from scripts.socials import repair_instagram_account_health as cli


def test_local_catalog_fill_missing_command_normalizes_account() -> None:
    command = cli._local_catalog_fill_missing_command(account="@TheTraitorsUS", source_scope="Network")

    assert command[-8:] == [
        "--platform",
        "instagram",
        "--account",
        "thetraitorsus",
        "--source-scope",
        "network",
        "--action",
        "fill_missing_posts",
    ]


def test_worker_drain_command_scopes_to_run_stage_and_platform() -> None:
    command = cli._worker_drain_command(run_id="run-1", stage="post_classify", max_run_seconds=45)

    assert "--run-id" in command
    assert "run-1" in command
    assert "--stage" in command
    assert "post_classify" in command
    assert "--platform" in command
    assert "instagram" in command
    assert command[-1] == "45"


def test_json_from_stdout_uses_last_json_object() -> None:
    payload = cli._json_from_stdout("noise\n" + json.dumps({"run_id": "run-1"}) + "\n")

    assert payload == {"run_id": "run-1"}


def test_run_id_from_payload_prefers_catalog_run_id() -> None:
    assert cli._run_id_from_payload({"catalog_run_id": "catalog-run-1"}) == "catalog-run-1"
    assert cli._run_id_from_payload({"executed_run_ids": ["run-2"]}) == "run-2"
