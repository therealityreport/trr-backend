"""A malformed server delay must not reopen the public source lane."""

import time
from datetime import UTC, datetime

import pytest
from test_identity_policy import lanes as lanes_fixture
from test_persistence import database as database_fixture
from test_public_response import response

from trr_backend.ingestion.identity_policy import IdentityDeferred, SourceRequestGate
from trr_backend.socials.instagram.public_response import PublicRequestClient
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

database = database_fixture
lanes = lanes_fixture


def test_negative_header_rejection_keeps_default_public_backoff(lanes):
    def gate():
        return SourceRequestGate(
            cookies={},
            public_only=True,
            transaction=lanes,
            check_cancelled=lambda: None,
            deadline=time.monotonic() + 30,
        )

    first = gate()
    permit = first.before_request()
    with pytest.raises(InstagramRequestFailure) as rejected:
        PublicRequestClient(session=None)._classify_response(
            response("Access denied", status=403, headers={"Retry-After": "-10"})
        )
    assert rejected.value.retry_after_seconds is None
    failure = first.finish_failure(
        rejected.value.error_code,
        retry_after_seconds=rejected.value.retry_after_seconds,
        permit=permit,
    )
    assert failure.code == "public_access_backoff"
    assert not failure.identity_blocked
    assert 895 < (failure.retry_at - datetime.now(UTC)).total_seconds() <= 900
    with pytest.raises(IdentityDeferred, match="identity_cooldown"):
        gate().before_request()
