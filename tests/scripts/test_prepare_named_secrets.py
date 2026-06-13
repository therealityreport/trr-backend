from __future__ import annotations

import pytest

from scripts.modal import prepare_named_secrets as cli


def test_default_source_env_can_follow_guardrail_source_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    source_env = tmp_path / "deploy.env"
    monkeypatch.setenv("TRR_MODAL_SOURCE_ENV", str(source_env))

    assert cli._default_source_env() == source_env


def test_split_env_excludes_modal_deploy_tokens_from_runtime_secret(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "DEFAULT_SOCIALBLADE_COOKIE_FILE", tmp_path / "missing-socialblade-cookies.json")

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


def test_apply_runtime_overrides_injects_canonical_modal_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WORKSPACE_ALLOW_MODAL_ALWAYS_ON_BILLING", raising=False)

    result = cli._apply_runtime_overrides({"TRR_DB_URL": "postgresql://example"}, disabled=False)

    assert result["TRR_DB_URL"] == "postgresql://example"
    assert result["TRR_JOB_PLANE_MODE"] == "remote"
    assert result["TRR_REMOTE_EXECUTOR"] == "modal"
    assert result["TRR_DB_POOL_MINCONN"] == "1"
    assert result["TRR_DB_POOL_MAXCONN"] == "2"
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
    assert result["SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY"] == "true"
    assert result["SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED"] == "false"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"] == "decodo"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY"] == "true"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY"] == "false"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS"] == "600"
    assert result["TRR_MODAL_MAINTENANCE_OWNER_REQUIRED"] == "1"
    assert result["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "0"
    assert result["TRR_MODAL_RUNTIME_SCHEDULER_ENABLED"] == "1"
    assert result["TRR_MODAL_API_MIN_CONTAINERS"] == "0"
    assert result["TRR_MODAL_ADMIN_KEEP_WARM"] == "0"
    assert result["TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT"] == "8"
    assert result["TRR_MODAL_SOCIAL_COMMENTS_JOB_CONCURRENCY_LIMIT"] == "10"
    assert result["TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_CONCURRENCY_LIMIT"] == "2"
    assert result["TRR_MODAL_SOCIAL_COMMENTS_RECOVERY_JOB_FUNCTION"] == "run_social_comments_recovery_job"
    assert result["SOCIAL_MODAL_DISPATCH_LIMIT"] == "10"
    assert result["SOCIAL_WORKER_POOL_COMMENTS"] == "10"
    assert result["SOCIAL_WORKER_POOL_SHARED_ACCOUNT_DISCOVERY"] == "3"
    assert result["SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS"] == "8"
    assert result["SOCIAL_SHARED_ACCOUNT_POSTS_PLATFORM_CAP_INSTAGRAM"] == "2"
    assert result["SOCIAL_CATALOG_RUN_IN_FLIGHT_CAP"] == "8"
    assert result["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "10"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROFILE_SHARD_COUNT"] == "8"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_GLOBAL_RATE_LIMIT_MODE"] == "advisory"
    assert result["SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED"] == "true"
    assert result["SOCIAL_THREADS_POSTS_PROXY_PROVIDER"] == "decodo"
    assert "SOCIALBLADE_PROXY_PROVIDER" not in result
    assert "SOCIALBLADE_USE_STICKY_PROXY" not in result
    assert "SOCIALBLADE_PROXY_SESSION_TTL_SECONDS" not in result
    assert result["SOCIAL_TIKTOK_COMMENT_FETCH_TIMEOUT_SECONDS"] == "180"
    assert result["SOCIAL_PLATFORM_CAP_PER_ACCOUNT_SCALING"] == "false"


def test_apply_runtime_overrides_preserves_explicit_social_caps() -> None:
    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_URL": "postgresql://example",
            "TRR_DB_POOL_MAXCONN": "1",
            "SOCIAL_WORKER_POOL_COMMENTS": "4",
            "SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM": "4",
            "SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY": "true",
            "SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER": "none",
            "SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY": "false",
            "SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY": "true",
            "SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS": "120",
        },
        disabled=False,
    )

    assert result["SOCIAL_WORKER_POOL_COMMENTS"] == "4"
    assert result["SOCIAL_POSTS_COMMENTS_PLATFORM_CAP_INSTAGRAM"] == "4"
    assert result["SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY"] == "true"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER"] == "decodo"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY"] == "true"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY"] == "false"
    assert result["SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS"] == "600"
    assert result["TRR_DB_POOL_MAXCONN"] == "2"


def test_apply_runtime_overrides_resets_always_on_billing_values_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKSPACE_ALLOW_MODAL_ALWAYS_ON_BILLING", raising=False)

    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_URL": "postgresql://example",
            "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED": "True",
            "TRR_MODAL_API_MIN_CONTAINERS": "2",
            "TRR_MODAL_ADMIN_KEEP_WARM": "1",
        },
        disabled=False,
    )

    assert result["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "0"
    assert result["TRR_MODAL_API_MIN_CONTAINERS"] == "0"
    assert result["TRR_MODAL_ADMIN_KEEP_WARM"] == "0"


def test_apply_runtime_overrides_preserves_always_on_billing_values_with_break_glass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKSPACE_ALLOW_MODAL_ALWAYS_ON_BILLING", "1")

    result = cli._apply_runtime_overrides(
        {
            "TRR_DB_URL": "postgresql://example",
            "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED": "1",
            "TRR_MODAL_API_MIN_CONTAINERS": "2",
            "TRR_MODAL_ADMIN_KEEP_WARM": "1",
        },
        disabled=False,
    )

    assert result["TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"] == "1"
    assert result["TRR_MODAL_API_MIN_CONTAINERS"] == "2"
    assert result["TRR_MODAL_ADMIN_KEEP_WARM"] == "1"


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


def test_split_env_materializes_file_backed_social_auth(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "DEFAULT_SOCIALBLADE_COOKIE_FILE", tmp_path / "missing-socialblade-cookies.json")
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


def test_split_env_materializes_default_socialblade_cookie_file_when_inline_json_is_empty(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cookie_file = tmp_path / "socialblade-cookies.json"
    cookie_file.write_text(
        '{\n  "cf_clearance": "fresh-clearance",\n  "session": "fresh-session"\n}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "DEFAULT_SOCIALBLADE_COOKIE_FILE", cookie_file)

    _runtime_values, social_values = cli._split_env(
        {
            "TRR_DB_URL": "postgresql://example",
            "SOCIALBLADE_COOKIES_JSON": "[]",
            "SOCIALBLADE_EMAIL": "ops@example.com",
        }
    )

    assert social_values["SOCIALBLADE_COOKIES_JSON"] == '{"cf_clearance":"fresh-clearance","session":"fresh-session"}'
    assert social_values["SOCIALBLADE_EMAIL"] == "ops@example.com"


def test_split_env_preserves_non_empty_socialblade_inline_json(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cookie_file = tmp_path / "socialblade-cookies.json"
    cookie_file.write_text('{\n  "session": "file-session"\n}\n', encoding="utf-8")
    monkeypatch.setattr(cli, "DEFAULT_SOCIALBLADE_COOKIE_FILE", cookie_file)

    _runtime_values, social_values = cli._split_env(
        {
            "TRR_DB_URL": "postgresql://example",
            "SOCIALBLADE_COOKIES_JSON": '{"session":"inline-session"}',
        }
    )

    assert social_values["SOCIALBLADE_COOKIES_JSON"] == '{"session":"inline-session"}'


def test_split_env_raises_for_missing_file_backed_social_auth() -> None:
    with pytest.raises(FileNotFoundError, match="SOCIAL_INSTAGRAM_COOKIES_FILE"):
        cli._split_env(
            {
                "TRR_DB_URL": "postgresql://example",
                "SOCIAL_INSTAGRAM_COOKIES_FILE": "/tmp/does-not-exist-instagram-cookies.json",
            }
        )
