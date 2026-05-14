from __future__ import annotations

from scripts.socials import backfill_socialblade_saved_instagram_accounts as cli


def test_load_saved_instagram_accounts_dedupes_and_preserves_person_id(monkeypatch) -> None:
    monkeypatch.setattr(
        cli.pg,
        "fetch_all",
        lambda _sql, _params: [
            {"person_id": None, "raw_handle": "@NetworkOfficial", "source": "shared_account_sources"},
            {"person_id": None, "raw_handle": "networkofficial", "source": "instagram_profiles"},
            {"person_id": "person-1", "raw_handle": "SomeCast", "source": "cast_tmdb.instagram_id"},
            {"person_id": "person-1", "raw_handle": "somecast", "source": "people_external_ids.instagram_id"},
            {"person_id": "person-2", "raw_handle": "sharedbytwo", "source": "cast_tmdb.instagram_id"},
            {"person_id": "person-3", "raw_handle": "sharedbytwo", "source": "people_external_ids.instagram"},
            {"person_id": None, "raw_handle": "", "source": "empty"},
        ],
    )

    accounts = cli.load_saved_instagram_accounts()

    assert accounts == [
        cli.SavedInstagramAccount(
            handle="networkofficial",
            person_id=None,
            sources=("instagram_profiles", "shared_account_sources"),
        ),
        cli.SavedInstagramAccount(
            handle="sharedbytwo",
            person_id=None,
            sources=("cast_tmdb.instagram_id", "people_external_ids.instagram"),
        ),
        cli.SavedInstagramAccount(
            handle="somecast",
            person_id="person-1",
            sources=("cast_tmdb.instagram_id", "people_external_ids.instagram_id"),
        ),
    ]


def test_dispatch_backfill_passes_account_scoped_handles(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_dispatch(**kwargs):
        calls.append(kwargs)
        return {"dispatched": True, "call_id": f"fc-{len(calls)}"}

    monkeypatch.setattr(cli, "dispatch_socialblade_scrape", fake_dispatch)

    result = cli.dispatch_backfill(
        [
            cli.SavedInstagramAccount(handle="networkofficial", person_id=None, sources=("shared",)),
            cli.SavedInstagramAccount(handle="somecast", person_id="person-1", sources=("people",)),
        ],
        source="all_saved_instagram_backfill",
        force=True,
        scrape_following=True,
        dry_run=False,
    )

    assert result["ok"] is True
    assert result["accepted_count"] == 2
    assert calls == [
        {
            "person_id": None,
            "handle": "networkofficial",
            "source": "all_saved_instagram_backfill",
            "force": True,
            "platform": "instagram",
            "scrape_following": True,
        },
        {
            "person_id": "person-1",
            "handle": "somecast",
            "source": "all_saved_instagram_backfill",
            "force": True,
            "platform": "instagram",
            "scrape_following": True,
        },
    ]
