from __future__ import annotations

from trr_backend.vision import people_count_engine as engine


def test_load_person_facebank_centroids_uses_ml_reference_embeddings(monkeypatch) -> None:
    engine._FACE_MATCH_CACHE["entries"] = []
    engine._FACE_MATCH_CACHE["expires_at"] = 0.0

    class _Cursor:
        def execute(self, sql: str, params=None) -> None:
            assert "FROM ml.face_reference_images AS fri" in sql
            assert "JOIN ml.face_reference_embeddings AS fre" in sql
            assert "coalesce(fre.metadata->>'contract_key', '') = %s" in sql
            assert params == [engine.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY]

        def fetchall(self):
            return [
                {
                    "person_id": "person-1",
                    "person_name": "Person One",
                    "embedding": "[1,0,0]",
                },
                {
                    "person_id": "person-1",
                    "person_name": "Person One",
                    "embedding": [0, 1, 0],
                },
                {
                    "person_id": "person-2",
                    "person_name": "Person Two",
                    "embedding": "[0,0,1]",
                },
            ]

    class _CursorContext:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(engine, "db_cursor", lambda: _CursorContext())

    entries = engine._load_person_facebank_centroids()

    assert len(entries) == 2
    by_person = {entry["person_id"]: entry for entry in entries}
    assert by_person["person-1"]["person_name"] == "Person One"
    assert by_person["person-2"]["person_name"] == "Person Two"
    assert len(by_person["person-1"]["embedding"]) == 3
    assert len(by_person["person-2"]["embedding"]) == 3
