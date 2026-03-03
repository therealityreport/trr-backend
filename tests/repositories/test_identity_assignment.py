from __future__ import annotations

from trr_backend.repositories import identity_assignment


def test_build_identity_candidate_person_ids_uses_metadata_signals(monkeypatch) -> None:
    owner_id = "11111111-1111-1111-1111-111111111111"
    alan_id = "22222222-2222-2222-2222-222222222222"
    milo_id = "33333333-3333-3333-3333-333333333333"
    lookup = {
        "Alan Cumming": alan_id,
        "Milo Ventimiglia": milo_id,
    }

    def _resolve(_db, person_name: str, *, person_name_id_cache=None):  # noqa: ANN001
        return lookup.get(person_name)

    monkeypatch.setattr(identity_assignment, "_resolve_person_id_from_name", _resolve)

    candidates = identity_assignment.build_identity_candidate_person_ids(
        db=object(),
        allow_identity_assignment=True,
        owner_person_id=owner_id,
        tagged_people_ids=None,
        tagged_people_names=None,
        metadata_signals=[
            {
                "caption": "Alan Cumming and Milo Ventimiglia in Milo Ventimiglia & Alan Cumming (2023)",
                "name": "Milo Ventimiglia & Alan Cumming",
            }
        ],
        person_name_id_cache={},
    )

    assert candidates[0] == owner_id
    assert set(candidates[1:]) == {alan_id, milo_id}


def test_build_identity_candidate_person_ids_ignores_unresolved_metadata_names(monkeypatch) -> None:
    owner_id = "11111111-1111-1111-1111-111111111111"

    def _resolve(_db, _person_name: str, *, person_name_id_cache=None):  # noqa: ANN001
        return None

    monkeypatch.setattr(identity_assignment, "_resolve_person_id_from_name", _resolve)

    candidates = identity_assignment.build_identity_candidate_person_ids(
        db=object(),
        allow_identity_assignment=True,
        owner_person_id=owner_id,
        tagged_people_ids=None,
        tagged_people_names=None,
        metadata_signals=[
            {
                "caption": "Alan Cumming and Milo Ventimiglia in Milo Ventimiglia & Alan Cumming (2023)",
                "name": "Milo Ventimiglia & Alan Cumming",
            }
        ],
        person_name_id_cache={},
    )

    assert candidates == [owner_id]
