from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from trr_backend.ingestion.domain import Failure, ReplayUnavailable, RunIntent, Stage, StageCounts, run_complete
from trr_backend.ingestion.evidence import Evidence, sanitize_payload


def test_launch_preserves_utc_partial_window_and_canonical_task_order():
    intent = RunIntent(
        account="@BravoTV",
        scope="bounded_window",
        date_start=datetime(2025, 1, 1),
        selected_tasks=(Stage.MEDIA, Stage.DETAILS, Stage.MEDIA),
    )
    assert intent.account == "bravotv"
    assert intent.date_start == datetime(2025, 1, 1, tzinfo=UTC)
    assert intent.date_end is None
    assert intent.selected_tasks == (Stage.DETAILS, Stage.MEDIA)
    assert RunIntent.from_payload(intent.payload()) == intent
    assert RunIntent.from_payload({"account": "bravotv"}).selected_tasks == (Stage.DETAILS, Stage.COMMENTS, Stage.MEDIA)


def test_full_history_clears_dates_and_empty_tasks_rejected():
    assert RunIntent(account="bravotv", date_start=datetime(2025, 1, 1)).date_start is None
    with pytest.raises(ValueError):
        RunIntent(account="bravotv", selected_tasks=())


def test_completion_requires_discovery_and_every_selected_stage():
    intent = RunIntent(account="bravotv")
    empty = StageCounts(0, 0, 0, 0, 0, True)
    stages = dict.fromkeys(Stage, empty)
    assert run_complete(intent, stages)
    stages[Stage.COMMENTS] = StageCounts(3, 1, 0, 1, 1, True)
    assert not run_complete(intent, stages)
    stages[Stage.COMMENTS] = replace(empty, discovery_complete=False)
    assert not run_complete(intent, stages)
    with pytest.raises(ValueError):
        StageCounts(2, 1, 1, 1, 0, True)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"identity_blocked": True, "evidence_kind": "restricted"},
        {"retry_at": datetime.now(UTC), "evidence_kind": "deleted"},
        {"evidence_kind": "timeout"},
    ],
)
def test_operational_failures_cannot_satisfy_source_unavailable(kwargs):
    with pytest.raises(ValueError):
        Failure("provider_failure", source_unavailable=True, **kwargs)


def test_evidence_allowlist_excludes_session_and_viewer_data_recursively():
    raw = {
        "items": [
            {
                "pk": "123",
                "caption": {"text": "Public caption"},
                "user": {"username": "bravotv", "sessionid": "secret", "friendship_status": {"following": True}},
                "viewer_has_liked": True,
                "image_versions2": {
                    "candidates": [{"url": "https://cdn.example/image.jpg?token=secret#fragment", "width": 100}]
                },
            }
        ],
        "headers": {"authorization": "secret"},
        "cookies": [{"value": "secret"}],
    }
    result = sanitize_payload(raw)
    assert result["items"][0]["user"] == {"username": "bravotv"}
    assert result["items"][0]["image_versions2"]["candidates"][0]["url"] == "https://cdn.example/image.jpg"
    assert "secret" not in str(result)
    assert "viewer_has_liked" not in result["items"][0]
    assert "token=secret" in raw["items"][0]["image_versions2"]["candidates"][0]["url"]


def test_evidence_expiry_pin_integrity_and_original_timestamp():
    captured = datetime(2025, 1, 1, tzinfo=UTC)
    evidence = Evidence.capture(
        source_identity="instagram:post:abc",
        captured_at=captured,
        parser_version="v1",
        payload={"pk": "123", "caption": {"text": "A"}},
    )
    assert evidence.captured_at == captured
    assert evidence.read(now=captured + timedelta(days=29))["pk"] == "123"
    with pytest.raises(ReplayUnavailable):
        evidence.read(now=captured + timedelta(days=30))
    assert evidence.read(now=captured + timedelta(days=31), pinned=True)["pk"] == "123"
    with pytest.raises(ValueError, match="hash mismatch"):
        replace(evidence, payload_json='{"pk":"456"}').read(now=captured)


def test_evidence_rejects_nonjson_and_credential_url():
    assert sanitize_payload({"url": "https://user:password@example.com/x"}) == {"url": None}
    with pytest.raises(ValueError):
        sanitize_payload({"items": [object()]})
    with pytest.raises(ValueError):
        sanitize_payload({"caption": {"text": float("nan")}})


def test_only_reviewed_cdn_delivery_parameters_survive_evidence():
    url = "https://scontent.cdninstagram.com/image.jpg?oh=asset%2Bsignature&oe=ABC&_nc_cat=1&sessionid=secret&access_token=secret#fragment"
    expected = "https://scontent.cdninstagram.com/image.jpg?oh=asset%2Bsignature&oe=ABC&_nc_cat=1"
    assert sanitize_payload({"display_url": url})["display_url"] == expected
    for host in ("cdninstagram.com.evil.example", "example.com", "fbcdn.net.evil.example"):
        assert sanitize_payload({"url": f"https://{host}/asset?oh=secret"})["url"] == f"https://{host}/asset"
    assert (
        sanitize_payload({"url": "https://scontent.fbcdn.net/asset?oe=ABC&cookie=secret"})["url"]
        == "https://scontent.fbcdn.net/asset?oe=ABC"
    )
