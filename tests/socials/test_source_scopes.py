from __future__ import annotations

import pytest

from trr_backend.socials import social_season_analytics_impl as legacy
from trr_backend.socials.source_scopes import (
    LEGACY_SOURCE_SCOPE_ALIASES,
    SUPPORTED_SCOPES,
    normalize_source_scope,
    normalize_source_scope_input,
    source_scope_is_network_family,
)


def test_source_scope_helpers_normalize_canonical_and_legacy_values() -> None:
    assert normalize_source_scope(None) == "network"
    assert normalize_source_scope(" Creator ") == "creator"
    assert normalize_source_scope("bravo") == "network"
    assert normalize_source_scope_input("bravo") == "bravo"
    assert source_scope_is_network_family("bravo") is True


def test_source_scope_helpers_reject_unsupported_values() -> None:
    with pytest.raises(ValueError, match="Unsupported source scope"):
        normalize_source_scope("unsupported")


def test_social_analytics_module_reexports_source_scope_helpers() -> None:
    assert legacy.SUPPORTED_SCOPES is SUPPORTED_SCOPES
    assert legacy.LEGACY_SOURCE_SCOPE_ALIASES is LEGACY_SOURCE_SCOPE_ALIASES
    assert legacy.normalize_source_scope("bravo") == "network"
    assert legacy._normalize_source_scope_input("bravo") == "bravo"
    assert legacy._source_scope_is_network_family("creator") is False
