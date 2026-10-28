"""A malformed server delay must not invent a public source wait."""

import time

import pytest
from test_identity_policy import lanes as lanes_fixture
from test_persistence import database as database_fixture
from test_public_response import response

from trr_backend.ingestion.identity_policy import SourceRequestGate
from trr_backend.socials.instagram.public_response import PublicRequestClient
from trr_backend.socials.instagram.request_client import InstagramRequestFailure

database = database_fixture
lanes = lanes_fixture


def test_negative_header_rejection_invents_no_public_wait(lanes):
    # Retry dates come only from valid source headers or real leases (8bd871e1);
    # a malformed negative Retry-After therefore schedules no local cooldown.
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
        PublicRequestClient(session=None)._classify_response(  # pyright: ignore[reportArgumentType]
            response("Access denied", status=403, headers={"Retry-After": "-10"})
        )
    assert rejected.value.retry_after_seconds is None
    failure = first.finish_failure(
        rejected.value.error_code,
        retry_after_seconds=rejected.value.retry_after_seconds,
        permit=permit,
    )
    assert failure is not None
    assert failure.code == "public_access_backoff"
    assert not failure.identity_blocked
    assert failure.retry_at is None
    # The lane stays usable: no invented wait blocks the next request.
    assert gate().before_request().request_number == 1
