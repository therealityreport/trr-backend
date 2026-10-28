"""Only required strict provider booleans qualify the bounded public query."""

import json
import time

import pytest

from tests.ingestion.test_profile_graphql import Session, bootstrap
from trr_backend.ingestion.profile_transport import PROVIDERS, PublicGraphQLProfileSource, provider_flags
from trr_backend.ingestion.profiles import ProfileError


def maps():
    return json.loads(bootstrap())


def html(*chunks):
    return "\n".join(json.dumps(chunk) for chunk in chunks)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("encoding", ["boolean", "result", "r", "both"])
def test_normalized_duplicate_required_flags_ignore_metadata_and_order(reverse, encoding):
    original = maps()
    second = {"gkxData": {}, "justknobxData": {}}
    expected = provider_flags(html(original))
    for group, key in PROVIDERS.values():
        value = original[group][key]["result"]
        second[group][key] = (
            value
            if encoding == "boolean"
            else {
                **({"result": value} if encoding in ("result", "both") else {}),
                **({"r": value} if encoding in ("r", "both") else {}),
                "metadata": {"ignored": "different"},
            }
        )
    chunks = [original, second]
    assert provider_flags(html(*(reversed(chunks) if reverse else chunks))) == expected


def test_unrelated_conflicts_malformed_values_and_unknown_groups_are_ignored():
    original = maps()
    unrelated = [
        {"gkxData": {"unrelated": {"result": True}}},
        {"gkxData": {"unrelated": {"result": False}}},
        {"justknobxData": {"unrelated": {"result": "invalid"}}},
        {"otherData": {"17028": False}},
    ]
    assert provider_flags(html(original, *unrelated)) == provider_flags(html(original))


@pytest.mark.parametrize("bad", [None, 0, 1, "true", [], {}, {"result": 1}, {"result": True, "r": None}])
def test_present_required_malformed_values_fail_even_with_other_valid_copy(bad):
    original = maps()
    group, key = next(iter(PROVIDERS.values()))
    with pytest.raises(ProfileError, match="profile_provider_flags_missing"):
        provider_flags(html(original, {group: {key: bad}}))


@pytest.mark.parametrize("mixed", [{"result": True, "r": False}, {"result": False, "r": True}])
def test_conflicting_result_and_r_are_never_silently_preferred(mixed):
    original = maps()
    group, key = next(iter(PROVIDERS.values()))
    original[group][key] = mixed
    with pytest.raises(ProfileError, match="profile_provider_flags_ambiguous"):
        provider_flags(html(original))


def test_required_boolean_conflict_and_missing_key_still_reject():
    original = maps()
    group, key = next(iter(PROVIDERS.values()))
    value = original[group][key]["result"]
    with pytest.raises(ProfileError, match="profile_provider_flags_ambiguous"):
        provider_flags(html(original, {group: {key: not value}}))
    del original[group][key]
    with pytest.raises(ProfileError, match="profile_provider_flags_missing"):
        provider_flags(html(original))


@pytest.mark.parametrize("failure", ["missing", "malformed", "ambiguous", "mixed"])
def test_rejected_bootstrap_counts_one_request_and_never_posts(failure):
    native = maps()
    group, key = next(iter(PROVIDERS.values()))
    if failure == "missing":
        del native[group][key]
    elif failure == "malformed":
        native[group][key] = {"result": 1}
    elif failure == "mixed":
        native[group][key] = {"result": True, "r": False}
    body = html(native)
    if failure == "ambiguous":
        body += html({group: {key: not native[group][key]["result"]}})

    class BootstrapSession(Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "GET":
                response._content = body.encode()
            return response

    session = BootstrapSession({"data": {"user": {"id": "123", "username": "reader"}}})
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    counted = []
    try:
        with pytest.raises(ProfileError):
            source(username="reader", pre_source_request=lambda: counted.append(True))
        assert len(counted) == len(session.calls) == source._sends == 1
        assert session.calls[0][0] == "GET"
    finally:
        source.close()


@pytest.mark.parametrize("extra", ["metadata", "unrelated", "mixed_agree"])
def test_valid_normalized_bootstrap_keeps_exact_two_request_budget(extra):
    original = maps()
    group, key = next(iter(PROVIDERS.values()))
    value = original[group][key]["result"]
    second = {group: {key: {"r": value, "metadata": "ignored"}}}
    if extra == "unrelated":
        original["gkxData"]["unrelated"] = {"result": True}
        second = {"gkxData": {"unrelated": {"result": False}}}
    elif extra == "mixed_agree":
        second = {group: {key: {"result": value, "r": value}}}
    body = html(original, second)

    class BootstrapSession(Session):
        def request(self, method, url, **kwargs):
            response = super().request(method, url, **kwargs)
            if method == "GET":
                response._content = body.encode()
            return response

    payload = {"data": {"user": {"id": "123", "username": "reader"}}}
    session = BootstrapSession(payload)
    source = PublicGraphQLProfileSource(user_id="123", deadline=time.monotonic() + 60, session=session)
    counted = []
    try:
        assert source(username="reader", pre_source_request=lambda: counted.append(True)) == payload
        assert len(counted) == len(session.calls) == source._sends == 2
        assert [call[0] for call in session.calls] == ["GET", "POST"]
        assert source.provenance is not None
        assert source.provenance["profile_provider_flags"] == provider_flags(html(original))
    finally:
        source.close()
