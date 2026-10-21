"""Runtime adapters keep transient source failures separate from deletion evidence."""

from unittest.mock import Mock

import pytest

from trr_backend.repositories import reddit_runtime as runtime
from trr_backend.repositories import reddit_transport as transport


def test_lifecycle_only_returns_explicit_signals(monkeypatch):
    monkeypatch.setattr(runtime.pg, "fetch_all", lambda *a: [{"reddit_post_id": "p", "reddit_comment_id": "c"}])
    client = Mock()
    client.get_json.return_value = {
        "data": {
            "children": [
                {"kind": "t3", "data": {"name": "t3_p", "selftext": "[deleted]", "author": "[deleted]"}},
                {"kind": "t1", "data": {"name": "t1_c", "body": "Still present", "author": "[deleted]"}},
            ]
        }
    }
    monkeypatch.setattr(transport, "build_client", lambda **kw: client)
    result = runtime.lifecycle_observations(["p", "missing"])
    assert [row["signal"] for row in result] == ["deleted", "account_deleted"]
    assert all(row["explicit"] for row in result)
    assert not any(row["reddit_post_id"] == "missing" for row in result)


def test_lifecycle_transport_failure_is_not_deletion(monkeypatch):
    monkeypatch.setattr(runtime.pg, "fetch_all", lambda *a: [])
    client = Mock()
    client.get_json.side_effect = transport.RedditTransportError("Forbidden", status=403)
    monkeypatch.setattr(transport, "build_client", lambda **kw: client)
    with pytest.raises(transport.RedditTransportError):
        runtime.lifecycle_observations(["p"])


def test_object_deletion_refuses_shared_legacy_namespace(monkeypatch):
    from trr_backend.media import s3_mirror

    client = Mock()
    monkeypatch.setattr(s3_mirror, "get_s3_client", lambda: client)
    monkeypatch.setattr(s3_mirror, "get_object_storage_bucket", lambda: "test-bucket")
    with pytest.raises(runtime.SharedLegacyObjectError):
        runtime.delete_object("shared/hash.jpg")
    client.delete_object.assert_not_called()
    runtime.delete_object("reddit/hash.jpg")
    client.delete_object.assert_called_once_with(Bucket="test-bucket", Key="reddit/hash.jpg")


def test_schedule_template_uses_current_scoped_community_rules(monkeypatch):
    monkeypatch.setattr(
        runtime.pg,
        "fetch_one",
        lambda *a: {
            "trr_show_id": "show",
            "subreddit": "bravo",
            "show_name": "Housewives",
            "show_aliases": ["RH"],
            "analysis_flairs": ["News"],
            "analysis_all_flairs": ["Episode"],
            "is_show_focused": False,
        },
    )
    monkeypatch.setattr(runtime.pg, "fetch_all", lambda *a: [{"name": "A Person"}, {"name": "A Person"}])
    payload = runtime.schedule_capture_template("community", "season")
    assert payload["community_id"] == "community"
    assert payload["cast_names"] == ["A Person"]
    assert payload["force_include_flairs"] == ["Episode"]
    assert payload["show_aliases"] == ["RH"]
    assert payload["refresh_policy"] == "stale"
    assert payload["requested_phases"] == ["comments", "discovery", "media"]


def test_schedule_template_rejects_cross_show_scope(monkeypatch):
    monkeypatch.setattr(runtime.pg, "fetch_one", lambda *a: None)
    with pytest.raises(ValueError, match="same show"):
        runtime.schedule_capture_template("community", "wrong-season")
