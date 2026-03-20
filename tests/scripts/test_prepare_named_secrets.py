from __future__ import annotations

from scripts.modal import prepare_named_secrets as cli


def test_split_env_excludes_modal_deploy_tokens_from_runtime_secret() -> None:
    runtime_values, social_values = cli._split_env(
        {
            "SUPABASE_DB_URL": "postgresql://example",
            "TRR_INTERNAL_ADMIN_SHARED_SECRET": "shared-secret",
            "MODAL_TOKEN_ID": "modal-token-id",
            "MODAL_TOKEN_SECRET": "modal-token-secret",
            "SOCIAL_TWITTER_COOKIES_JSON": '{"cookies": []}',
            "SOCIALBLADE_EMAIL": "ops@example.com",
            "SOCIALBLADE_PASSWORD": "secret",
        }
    )

    assert runtime_values == {
        "SUPABASE_DB_URL": "postgresql://example",
        "TRR_INTERNAL_ADMIN_SHARED_SECRET": "shared-secret",
    }
    assert social_values == {
        "SOCIAL_TWITTER_COOKIES_JSON": '{"cookies": []}',
        "SOCIALBLADE_EMAIL": "ops@example.com",
        "SOCIALBLADE_PASSWORD": "secret",
    }


def test_apply_runtime_overrides_injects_canonical_modal_defaults() -> None:
    result = cli._apply_runtime_overrides({"SUPABASE_DB_URL": "postgresql://example"}, disabled=False)

    assert result["SUPABASE_DB_URL"] == "postgresql://example"
    assert result["TRR_JOB_PLANE_MODE"] == "remote"
    assert result["TRR_REMOTE_EXECUTOR"] == "modal"
    assert (
        result["TRR_MODAL_RUNTIME_SECRET_NAME"]
        == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_RUNTIME_SECRET_NAME"]
    )
    assert (
        result["TRR_MODAL_SOCIAL_SECRET_NAME"] == cli.CANONICAL_REMOTE_RUNTIME_OVERRIDES["TRR_MODAL_SOCIAL_SECRET_NAME"]
    )


def test_apply_runtime_overrides_can_be_disabled() -> None:
    original = {"SUPABASE_DB_URL": "postgresql://example"}

    assert cli._apply_runtime_overrides(original, disabled=True) == original


def test_apply_runtime_overrides_preserves_object_storage_values() -> None:
    result = cli._apply_runtime_overrides(
        {
            "OBJECT_STORAGE_BUCKET": "trr-media-prod",
            "OBJECT_STORAGE_PUBLIC_BASE_URL": "https://media.thereality.report",
        },
        disabled=False,
    )

    assert result["OBJECT_STORAGE_BUCKET"] == "trr-media-prod"
    assert result["OBJECT_STORAGE_PUBLIC_BASE_URL"] == "https://media.thereality.report"
