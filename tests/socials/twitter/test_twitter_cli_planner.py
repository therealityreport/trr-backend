from __future__ import annotations

import sys

from scripts.socials.twitter import scrape


def test_cli_plan_repairs_prints_ranked_candidates(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        scrape,
        "_load_repair_plan_inputs",
        lambda _account: (
            [
                {"source_id": "small", "replies_count": 10, "quotes_count": 0},
                {"source_id": "large", "replies_count": 100, "quotes_count": 0},
            ],
            {"small": 9, "large": 20},
            {},
            [],
        ),
    )
    monkeypatch.setattr(sys, "argv", ["scrape.py", "--plan-repairs", "--account", "TheTraitorsUS"])

    scrape.main()

    output = capsys.readouterr().out
    assert "Twitter repair plan:" in output
    assert "1. large reply: missing=80" in output
    assert "2. small reply: missing=1" in output


def test_cli_plan_repairs_supports_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        scrape,
        "_load_repair_plan_inputs",
        lambda _account: (
            [{"source_id": "root", "replies_count": 10, "quotes_count": 0}],
            {"root": 5},
            {},
            [],
        ),
    )
    monkeypatch.setattr(sys, "argv", ["scrape.py", "--plan-repairs", "--json"])

    scrape.main()

    output = capsys.readouterr().out
    assert '"root_source_id": "root"' in output
    assert '"raw_missing": 5' in output
