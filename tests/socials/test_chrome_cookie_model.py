from __future__ import annotations

import pytest

from trr_backend.socials.chrome_cookie_model import build_social_cookie_chrome_model


def test_build_social_cookie_chrome_model_defaults_to_codex_chrome_profile() -> None:
    model = build_social_cookie_chrome_model("instagram", account_handle="@thetraitorsus")

    assert model["platform"] == "instagram"
    assert model["account_handle"] == "thetraitorsus"
    assert model["chrome"] == {
        "plugin": "@chrome",
        "profile": "codex@thereality.report",
        "route": "@chrome(profile: codex@thereality.report)",
    }
    assert model["login_url"] == "https://www.instagram.com/accounts/login/"
    assert model["required_cookie_names"] == ["sessionid", "csrftoken", "ds_user_id"]
    assert model["output_contract"]["cookie_json_env"] == "SOCIAL_INSTAGRAM_COOKIES_JSON"
    assert model["output_contract"]["do_not_print_cookie_values"] is True
    assert model["safety"]["browser_plugin_may_not_inspect_cookie_store"] is True


@pytest.mark.parametrize(
    ("platform", "expected_platform", "expected_cookie_json_env"),
    [
        ("threads", "threads", "SOCIAL_THREADS_COOKIES_JSON"),
        ("tiktok", "tiktok", "SOCIAL_TIKTOK_COOKIES_JSON"),
        ("facebook", "facebook", "SOCIAL_FACEBOOK_COOKIES_JSON"),
        ("twitter", "twitter", "SOCIAL_TWITTER_COOKIES_JSON"),
        ("x", "twitter", "SOCIAL_TWITTER_COOKIES_JSON"),
    ],
)
def test_build_social_cookie_chrome_model_supports_social_cookie_platforms(
    platform: str,
    expected_platform: str,
    expected_cookie_json_env: str,
) -> None:
    model = build_social_cookie_chrome_model(platform)

    assert model["platform"] == expected_platform
    assert model["output_contract"]["cookie_json_env"] == expected_cookie_json_env
    assert model["validation_command"][-1] == "--validate-only"
    assert all("cookie" not in key.lower() or "value" not in key.lower() for key in model)


def test_build_social_cookie_chrome_model_rejects_unknown_platform() -> None:
    with pytest.raises(ValueError, match="Unsupported social cookie platform"):
        build_social_cookie_chrome_model("linkedin")
