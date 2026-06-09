from __future__ import annotations

import pytest

from trr_backend.socials.rollout_flags import resolve_rollout_flag


def test_rollout_flag_default_enabled_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_TEST_ROLLOUT_ENABLED", raising=False)

    assert resolve_rollout_flag("SOCIAL_TEST_ROLLOUT_ENABLED", default_enabled=True) == {
        "env_var": "SOCIAL_TEST_ROLLOUT_ENABLED",
        "enabled": True,
        "default_enabled": True,
        "configured_value": None,
    }


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off", "disabled"])
def test_rollout_flag_false_values_disable(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SOCIAL_TEST_ROLLOUT_ENABLED", value)

    result = resolve_rollout_flag("SOCIAL_TEST_ROLLOUT_ENABLED", default_enabled=True)

    assert result["enabled"] is False
    assert result["configured_value"] == value


@pytest.mark.parametrize("value", ["1", "true", "True", "yes", "on", "enabled"])
def test_rollout_flag_true_values_enable(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SOCIAL_TEST_ROLLOUT_ENABLED", value)

    result = resolve_rollout_flag("SOCIAL_TEST_ROLLOUT_ENABLED", default_enabled=False)

    assert result["enabled"] is True
    assert result["configured_value"] == value


def test_rollout_flag_unknown_value_uses_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_TEST_ROLLOUT_ENABLED", "maybe")

    assert resolve_rollout_flag("SOCIAL_TEST_ROLLOUT_ENABLED", default_enabled=False)["enabled"] is False
    assert resolve_rollout_flag("SOCIAL_TEST_ROLLOUT_ENABLED", default_enabled=True)["enabled"] is True
