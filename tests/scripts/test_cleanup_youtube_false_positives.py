from __future__ import annotations

import scripts.socials.cleanup_youtube_false_positives as mod


def test_main_defaults_to_dry_run_without_deleting(monkeypatch) -> None:
    monkeypatch.setattr(mod, "_resolve_show_id", lambda **_kwargs: "show-1")
    monkeypatch.setattr(mod, "_find_candidate_rows", lambda **_kwargs: [{"id": "row-1"}])
    delete_calls: list[list[str]] = []
    monkeypatch.setattr(mod, "_delete_rows", lambda *, row_ids: delete_calls.append(row_ids))

    mod.main([])

    assert delete_calls == []


def test_main_apply_deletes_candidate_rows(monkeypatch) -> None:
    monkeypatch.setattr(mod, "_resolve_show_id", lambda **_kwargs: "show-1")
    monkeypatch.setattr(mod, "_find_candidate_rows", lambda **_kwargs: [{"id": "row-1"}])
    delete_calls: list[list[str]] = []
    monkeypatch.setattr(mod, "_delete_rows", lambda *, row_ids: delete_calls.append(row_ids) or 1)

    mod.main(["--apply"])

    assert delete_calls == [["row-1"]]
