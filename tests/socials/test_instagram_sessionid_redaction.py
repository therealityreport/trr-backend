from trr_backend.socials.instagram.scraper import _sessionid_fingerprint


def test_sessionid_fingerprint_is_deterministic_without_leaking_value() -> None:
    raw = "SECRET_VALUE"

    fingerprint = _sessionid_fingerprint({"sessionid": raw})

    assert fingerprint == _sessionid_fingerprint({"sessionid": raw})
    assert raw not in fingerprint
    assert raw[:8] not in fingerprint


def test_sessionid_fingerprint_changes_for_different_sessionids() -> None:
    assert _sessionid_fingerprint({"sessionid": "SECRET_VALUE"}) != _sessionid_fingerprint(
        {"sessionid": "OTHER_SECRET_VALUE"}
    )


def test_sessionid_fingerprint_missing_sessionid_is_none() -> None:
    assert _sessionid_fingerprint({}) == "none"
