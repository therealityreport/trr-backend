from __future__ import annotations

import pytest

from trr_backend.services import public_identity

SHOW_ID = "00000000-0000-0000-0000-000000000001"
OTHER_SHOW_ID = "00000000-0000-0000-0000-000000000002"
PERSON_ID = "00000000-0000-0000-0000-000000000003"
MISSING_SHOW_ID = "00000000-0000-0000-0000-000000000099"


def _show_candidate(*, show_id: str, canonical_slug: str, canonical_match: bool) -> dict[str, object]:
    return {
        "show_id": show_id,
        "show_name": f"Show {show_id[-1]}",
        "canonical_slug": canonical_slug,
        "matched_is_canonical": canonical_match,
    }


def test_canonical_show_match_wins_over_colliding_direct_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        public_identity.identity_repo,
        "list_show_slug_candidates",
        lambda slug: [
            _show_candidate(show_id=SHOW_ID, canonical_slug="rhobh", canonical_match=True),
            _show_candidate(show_id=OTHER_SHOW_ID, canonical_slug="other-show", canonical_match=False),
        ],
    )

    resolved = public_identity.resolve_show("RHOBH")

    assert resolved["show_id"] == SHOW_ID
    assert resolved["match_kind"] == "canonical"
    assert resolved["requested_slug"] == "rhobh"
    assert resolved["canonical_path"] == "/shows/rhobh"


def test_ambiguous_show_alias_returns_stable_candidates(monkeypatch) -> None:
    monkeypatch.setattr(
        public_identity.identity_repo,
        "list_show_slug_candidates",
        lambda slug: [
            _show_candidate(show_id=SHOW_ID, canonical_slug="show-one", canonical_match=False),
            _show_candidate(show_id=OTHER_SHOW_ID, canonical_slug="show-two", canonical_match=False),
        ],
    )

    with pytest.raises(public_identity.IdentityResolutionError) as raised:
        public_identity.resolve_show("shared-show")

    assert raised.value.code == "IDENTITY_AMBIGUOUS"
    assert raised.value.status == 409
    assert raised.value.detail["candidate_count"] == 2
    assert raised.value.detail["candidates"] == [
        {"show_id": SHOW_ID, "canonical_slug": "show-one"},
        {"show_id": OTHER_SHOW_ID, "canonical_slug": "show-two"},
    ]


def test_season_resolution_is_bounded_to_two_repository_queries(monkeypatch) -> None:
    calls: list[str] = []

    def list_shows(slug: str):
        calls.append("show")
        return [_show_candidate(show_id=SHOW_ID, canonical_slug="rhobh", canonical_match=False)]

    def get_season(*, show_id: str, season_number: int):
        calls.append("season")
        return {
            "season_id": "00000000-0000-0000-0000-000000000014",
            "show_id": show_id,
            "season_number": season_number,
            "season_title": "Season 14",
        }

    monkeypatch.setattr(public_identity.identity_repo, "list_show_slug_candidates", list_shows)
    monkeypatch.setattr(public_identity.identity_repo, "get_season_identity", get_season)

    resolved = public_identity.resolve_season(show_slug="beverly-hills", season_number="14")

    assert calls == ["show", "season"]
    assert resolved["canonical_show_slug"] == "rhobh"
    assert resolved["canonical_path"] == "/shows/rhobh/seasons/14"


def test_person_show_slug_context_is_bounded_and_narrows_candidates(monkeypatch) -> None:
    calls: list[tuple[str, str | None]] = []
    monkeypatch.setattr(
        public_identity.identity_repo,
        "list_show_slug_candidates",
        lambda slug: [_show_candidate(show_id=SHOW_ID, canonical_slug="rhobh", canonical_match=True)],
    )

    def list_people(*, slug: str, show_id: str | None):
        calls.append((slug, show_id))
        return [
            {
                "person_id": PERSON_ID,
                "full_name": "Alex Smith",
                "canonical_slug": "alex-smith--00000000",
                "matched_is_canonical": False,
            }
        ]

    monkeypatch.setattr(public_identity.identity_repo, "list_person_slug_candidates", list_people)

    resolved = public_identity.resolve_person("alex-smith", show_slug="rhobh")

    assert calls == [("alex-smith", SHOW_ID)]
    assert resolved["match_kind"] == "alias"
    assert resolved["show_context"] == {
        "show_id": SHOW_ID,
        "show_name": "Show 1",
        "canonical_slug": "rhobh",
    }


@pytest.mark.parametrize("slug", ["", "bad_slug", "-bad", "bad/slug"])
def test_invalid_slug_is_rejected_before_database_access(slug: str, monkeypatch) -> None:
    called = False

    def list_candidates(value: str):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(public_identity.identity_repo, "list_show_slug_candidates", list_candidates)

    with pytest.raises(public_identity.IdentityResolutionError) as raised:
        public_identity.resolve_show(slug)

    assert raised.value.code == "INVALID_IDENTITY_SLUG"
    assert called is False


@pytest.mark.parametrize("season_number", ["-1", "season-fourteen", "", "2147483648"])
def test_invalid_season_number_is_rejected_before_show_lookup(season_number: str, monkeypatch) -> None:
    called = False

    def list_candidates(value: str):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(public_identity.identity_repo, "list_show_slug_candidates", list_candidates)

    with pytest.raises(public_identity.IdentityResolutionError) as raised:
        public_identity.resolve_season(show_slug="rhobh", season_number=season_number)

    assert raised.value.code == "INVALID_SEASON_NUMBER"
    assert raised.value.status == 400
    assert called is False


@pytest.mark.parametrize("show_id", ["", "not-a-uuid", "00000000-0000-0000-0000-00000000000z"])
def test_invalid_show_id_context_is_rejected_before_person_lookup(show_id: str, monkeypatch) -> None:
    called = False

    def get_show(value: str):
        nonlocal called
        called = True
        return None

    monkeypatch.setattr(public_identity.identity_repo, "get_show_identity_by_id", get_show)

    with pytest.raises(public_identity.IdentityResolutionError) as raised:
        public_identity.resolve_person("alex-smith", show_id=show_id)

    assert raised.value.code == "INVALID_IDENTITY_CONTEXT"
    assert raised.value.status == 400
    assert called is False


def test_invalid_person_slug_precedes_valid_but_missing_show_context_lookup(monkeypatch) -> None:
    context_lookup_called = False

    def get_show(value: str):
        nonlocal context_lookup_called
        context_lookup_called = True
        return None

    monkeypatch.setattr(public_identity.identity_repo, "get_show_identity_by_id", get_show)

    with pytest.raises(public_identity.IdentityResolutionError) as raised:
        public_identity.resolve_person("bad_slug", show_id=MISSING_SHOW_ID)

    assert raised.value.code == "INVALID_IDENTITY_SLUG"
    assert raised.value.status == 400
    assert context_lookup_called is False
