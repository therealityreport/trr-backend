"""Exact bounded diagnostic types and per-attempt first-event collection."""

import pytest

from tests.ingestion.test_runtime import PUBLIC_DIAGNOSTIC
from trr_backend.ingestion.public_diagnostics import (
    current_public_diagnostic,
    record_public_diagnostic,
    reset_public_diagnostic,
    validate_public_diagnostic,
)


@pytest.mark.parametrize(
    "field,value",
    [
        ("event", "private-event-sentinel"),
        ("method", "DELETE"),
        ("endpoint_category", "/private-url-sentinel/"),
        ("status", "403"),
        ("status", -1),
        ("status", True),
        ("error_code", ["forbidden"]),
    ],
)
def test_diagnostic_contract_rejects_non_allowlisted_values(field, value):
    assert validate_public_diagnostic(dict(PUBLIC_DIAGNOSTIC, **{field: value})) is None


def test_diagnostic_is_one_copied_event_and_reset_clears_attempt():
    reset_public_diagnostic()
    try:
        first = dict(PUBLIC_DIAGNOSTIC)
        record_public_diagnostic(first)
        first["error_code"] = "private-mutation-sentinel"
        record_public_diagnostic(dict(PUBLIC_DIAGNOSTIC, error_code="unauthorized"))
        assert current_public_diagnostic() == PUBLIC_DIAGNOSTIC
        output = current_public_diagnostic()
        assert output is not None
        output["method"] = "private-output-sentinel"
        assert current_public_diagnostic() == PUBLIC_DIAGNOSTIC
    finally:
        reset_public_diagnostic()
    assert current_public_diagnostic() is None
