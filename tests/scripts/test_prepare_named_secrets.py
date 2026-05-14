from __future__ import annotations

import pytest

from scripts.modal import prepare_named_secrets as cli


def test_split_env_excludes_modal_deploy_tokens_from_runtime_secret() -> None:
    runtime_values, social_values = cli._split_env(
        {
            "TRR_DB_URL": "postgresql://example",
            "TRR_INTERNAL_ADMIN_SHARED_SECRET": "shared-secret",
            "MODAL_TOKEN_ID": "modal-token-id",
            "MODAL_TOKEN_SECRET": "modal-token-secret",
            "SOCIAL_TWITTER_COOKIES_JSON": '{"cookies": []}',
            "SOCIALBLADE_EMAIL": "ops@example.com",
            "SOCIALBLADE_PASSWORD": "secret",
            "SOCIAL_INSTAGRAM_IG_WWW_CLAIM": "claim-token",
            "SOCIAL_INSTAGRAM_WEB_SESSION_ID": "session-fragment",
            "INSTAGRAM_WEB_BLOKS_VERSION_ID": "bloks-version",
            "INSTAGRAM_WEB_X_ASBD_ID": "359341",
        }
    )

    assert runtime_values == {
        "TRR_DB_URL": "postgresql://example",
        "TRR_INTERNAL_ADMIN_SHARED_SECRET": "shared-secret",
    }
    assert social_values == {
        "SOCIAL_TWITTER_COOKIES_JSON": '{"cookies": []}',
        "SOCIALBLADE_EMAIL": "ops@example.com",
        "SOCIALBLADE_PASSWORD": "secret",
        "SOCIAL_INSTAGRAM_IG_WWW_CLAIM": "claim-token",
        "SOCIAL_INSTAGRAM_WEB_SESSION_ID": "session-fragment",
        "INSTAGRAM_WEB_BLOKS_VERSION_ID": "bloks-version",
        "INSTAGRAM_WEB_X_ASBD_ID": "359341",
    }


def test_apply_runtime_overrides_injects_canonical_modal_defaults() -> None:
    result = cli._apply_runtime_overrides({"TRR_DB_URL": "postgresql://example"}, disabled=False)

    assert result["TRR_DB_URL"] == "postgresql://example"
    assert result["TRR_JOB_PLANE_MODE"] == "remote"
    assert result["TRR_REMOTE_EXECUTOR"] == "modal"
    assert result["TRR_DB_POOL_MINCONN"] == "1"
    assert result["TRR_DB_POOL_MAXCONN"] == "1"
    assert result["TRR_SOCIAL_CONTROL_DB_POOL_MAXCONN"] == "1"
    assert result["TRR_SOCIAL_PROGRESS_DB_POOL_MAXCONN"] == "1"
    assert result["TRR_DB_POOL_CLOSE_AFTER_RETURN"] == "1"
    assert result["TRR_DB_POOL_ACQUIRE_ATTEMPTS"] == "30"
    assert result["TRR_DB_POOL_ACQUIRE_SLEEP_MS"] == "200"
    assert (
        result["TRR_MODAL_RUNTIME_SECRET_NAME"]
        == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_RUNTIME_SECRET_NAME"]
    )
    assert (
        result["TRR_MODAL_SOCIAL_SECRET_NAME"] == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_SOCIAL_SECRET_NAME"]
    )
    assert (
        result["TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION"]
        == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION"]
    )
    assert (
        result["TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION"]
        == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION"]
    )
    assert result["TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT"] == "5"
    assert result["SOCIAL_MODAL_DISPATCH_LIMIT"] == "5"
    assert result["SOCIAL_WORKER_POOL_COMMENTS"] == "2"
    assert result["SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY"] == "3"
    assert result["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "2"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT"] == "2"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE"] == "file_lock"
    assert result["SOCIAL_THREADS_POSTS_PROXY_PROVIDER"] == "decodo"
    assert result["SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS"] == "180"
    assert result["SOCIAL_PLATFORM_CAP_PER_ACCOUNT_SCALING"] == "false"


def test_apply_runtime_overrides_preserves_explicit_social_caps() -> None:
    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_URL": "postgresql://example",
            "TRR_DB_POOL_MAXCONN": "1",
            "SOCIAL_WORKER_POOL_COMMENTS": "4",
            "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM": "4",
        },
        disabled=False,
    )

    assert result["SOCIAL_WORKER_POOL_COMMENTS"] == "4"
    assert result["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "4"
    assert result["TRR_DB_POOL_MAXCONN"] == "1"


def test_apply_runtime_overrides_can_be_disabled() -> None:
    original = {"TRR_DB_URL": "postgresql://example"}

    assert cli._apply_runtime_overrides(original, disabled=True) == {
        "TRR_DB_URL": "postgresql://example",
    }


def test_apply_runtime_overrides_preserves_object_storage_values() -> None:
    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_URL": "postgresql://canonical",
            "OBJECT_STORAGE_BUCKET": "trr-media-prod",
            "OBJECT_STORAGE_PUBLIC_BASE_URL": "https://media.thereality.report",
        },
        disabled=False,
    )

    assert result["TRR_DB_URL"] == "postgresql://canonical"
    assert result["OBJECT_STORAGE_BUCKET"] == "trr-media-prod"
    assert result["OBJECT_STORAGE_PUBLIC_BASE_URL"] == "https://media.thereality.report"


def test_apply_runtime_overrides_preserves_existing_trr_db_url() -> None:
    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_DIRECT_URL": "postgresql://postgres:secret@db.example.supabase.co:5432/postgres",
            "TRR_DB_URL": "postgresql://canonical",
            "SUPABASE_DB_URL": "postgresql://legacy",
            "DATABASE_URL": "postgresql://legacy-tooling",
        },
        disabled=False,
    )

    assert result["TRR_DB_URL"] == "postgresql://canonical"
    assert "TRR_DB_DIRECT_URL" not in result
    assert "SUPABASE_DB_URL" not in result
    assert "DATABASE_URL" not in result


def test_apply_runtime_overrides_requires_canonical_trr_db_url() -> None:
    try:
        cli._apply_runtime_overrides({"SUPABASE_DB_URL": "postgresql://legacy"}, disabled=False)
    except KeyError as exc:
        assert "TRR_DB_URL" in str(exc)
    else:
        raise AssertionError("Expected KeyError when TRR_DB_URL is missing")


def test_split_env_materializes_file_backed_social_auth(tmp_path) -> None:
    cookie_file = tmp_path / "instagram-cookies.json"
    cookie_file.write_text('{\n  "sessionid": "abc123"\n}\n', encoding="utf-8")

    runtime_values, social_values = cli._split_env(
        {
            "TRR_DB_URL": "postgresql://example",
            "SOCIAL_INSTAGRAM_COOKIES_FILE": str(cookie_file),
            "SOCIALBLADE_EMAIL": "ops@example.com",
        }
    )

    assert runtime_values == {"TRR_DB_URL": "postgresql://example"}
    assert social_values == {
        "SOCIAL_INSTAGRAM_COOKIES_JSON": '{"sessionid":"abc123"}',
        "SOCIALBLADE_EMAIL": "ops@example.com",
    }


def test_split_env_prefers_configured_cookie_file_over_stale_inline_json(tmp_path) -> None:
    cookie_file = tmp_path / "instagram-cookies.json"
    cookie_file.write_text('{\n  "sessionid": "fresh-file-session"\n}\n', encoding="utf-8")

    _runtime_values, social_values = cli._split_env(
        {
            "TRR_DB_URL": "postgresql://example",
            "SOCIAL_INSTAGRAM_COOKIES_JSON": '{"sessionid":"stale-env-session"}',
            "SOCIAL_INSTAGRAM_COOKIES_FILE": str(cookie_file),
        }
    )

    assert social_values["SOCIAL_INSTAGRAM_COOKIES_JSON"] == '{"sessionid":"fresh-file-session"}'


def test_split_env_raises_for_missing_file_backed_social_auth() -> None:
    with pytest.raises(FileNotFoundError, match="SOCIAL_INSTAGRAM_COOKIES_FILE"):
        cli._split_env(
            {
                "TRR_DB_URL": "postgresql://example",
                "SOCIAL_INSTAGRAM_COOKIES_FILE": "/tmp/does-not-exist-instagram-cookies.json",
            }
        )
