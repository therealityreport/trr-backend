from __future__ import annotations

import json

from scripts.render import sync_render_service_from_aws as cli


def test_parse_env_text_keeps_last_value_and_ignores_noise() -> None:
    payload = """
DATABASE_URL=postgres://one
TRR_MODAL_ENABLED=0
no equals here
TRR_MODAL_ENABLED=1
"""

    env = cli.parse_env_text(payload)

    assert env == {
        "DATABASE_URL": "postgres://one",
        "TRR_MODAL_ENABLED": "1",
    }


def test_build_service_payload_uses_docker_standard_runtime() -> None:
    config = cli.RenderServiceConfig(
        name="trr-backend-api",
        owner_id="tea-owner",
        repo_url="https://github.com/therealityreport/trr-backend.git",
        branch="main",
        plan="standard",
        render_region="virginia",
        health_check_path="/health",
        auto_deploy="no",
    )

    payload = cli.build_service_payload(
        config,
        {
            "DATABASE_URL": "postgres://db",
            "TRR_MODAL_ENABLED": "1",
        },
    )

    assert payload["type"] == "web_service"
    assert payload["name"] == "trr-backend-api"
    assert payload["ownerId"] == "tea-owner"
    assert payload["repo"] == "https://github.com/therealityreport/trr-backend.git"
    assert payload["branch"] == "main"
    assert payload["autoDeploy"] == "no"
    assert payload["serviceDetails"] == {
        "runtime": "docker",
        "plan": "standard",
        "region": "virginia",
        "healthCheckPath": "/health",
        "numInstances": 1,
    }
    assert payload["envVars"] == [
        {"key": "DATABASE_URL", "value": "postgres://db"},
        {"key": "TRR_MODAL_ENABLED", "value": "1"},
    ]


def test_select_owner_id_prefers_named_owner(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_render_request",
        lambda api_key, method, path, payload=None, query=None: (
            200,
            [
                {"owner": {"id": "tea-first", "name": "Other workspace"}},
                {"owner": {"id": "tea-target", "name": "The Reality's workspace"}},
            ],
        ),
    )

    owner_id = cli.select_owner_id("token", owner_id="", owner_name="The Reality's workspace")

    assert owner_id == "tea-target"


def test_build_effective_env_normalizes_render_paths() -> None:
    env = cli.build_effective_env(
        render_env={
            "TRR_API_URL": "http://localhost:8000",
            "REDIS_URL": "__UNSET__",
            "GOOGLE_APPLICATION_CREDENTIALS": "../../keys/trr-backend-df2c438612e1.json",
            "GOOGLE_SERVICE_ACCOUNT_FILE": "keys/trr-backend-df2c438612e1.json",
            "FIREBASE_SERVICE_ACCOUNT_FILE": "keys/trr-web-25d2e-38499515994a.json",
            "TWIKIT_COOKIES_FILE": "data/twitter_cookies.json",
            "TIKTOK_COOKIES_FILE": "data/tiktok_cookies.json",
            "SOCIAL_FACEBOOK_COOKIES_FILE": "/Users/thomashulihan/secrets-archive/facebook_cookies.json",
            "SOCIAL_THREADS_COOKIES_FILE": "/Users/thomashulihan/secrets-archive/threads_cookies.json",
        },
        live_env={},
        ssm_env={"TRR_MODAL_ENABLED": "1"},
        service_url="https://trr-backend.onrender.com",
    )

    assert env["TRR_API_URL"] == "https://trr-backend.onrender.com"
    assert env["GOOGLE_APPLICATION_CREDENTIALS"] == "/etc/secrets/trr-backend-gcp.json"
    assert env["GOOGLE_SERVICE_ACCOUNT_FILE"] == "/etc/secrets/trr-backend-gcp.json"
    assert env["FIREBASE_SERVICE_ACCOUNT_FILE"] == "/etc/secrets/firebase-service-account.json"
    assert env["TWIKIT_COOKIES_FILE"] == "/etc/secrets/twikit-cookies.json"
    assert env["TIKTOK_COOKIES_FILE"] == "/etc/secrets/tiktok-cookies.json"
    assert env["SOCIAL_FACEBOOK_COOKIES_FILE"] == "/etc/secrets/facebook-cookies.json"
    assert env["SOCIAL_THREADS_COOKIES_FILE"] == "/etc/secrets/threads-cookies.json"
    assert env["TRR_MODAL_ENABLED"] == "1"
    assert "REDIS_URL" not in env


def test_build_secret_file_payloads_resolves_files_and_synthesizes_twikit(tmp_path, monkeypatch) -> None:
    keys_dir = tmp_path / "keys"
    data_dir = tmp_path / "data"
    secrets_dir = tmp_path / "secrets-archive"
    keys_dir.mkdir()
    data_dir.mkdir()
    secrets_dir.mkdir()

    google_path = keys_dir / "trr-backend-df2c438612e1.json"
    google_path.write_text('{"client_email":"bot@example.com"}')
    firebase_path = keys_dir / "trr-web-25d2e-38499515994a.json"
    firebase_path.write_text('{"project_id":"trr-web-25d2e"}')
    tiktok_path = data_dir / "tiktok_cookies.json"
    tiktok_path.write_text('{"cookies":[{"name":"sessionid","value":"abc"}]}')
    facebook_path = secrets_dir / "facebook_cookies.json"
    facebook_path.write_text('{"cookies":[{"name":"c_user","value":"1"}]}')
    threads_path = secrets_dir / "threads_cookies.json"
    threads_path.write_text('{"cookies":[{"name":"ds_user_id","value":"2"}]}')

    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(cli, "WORKSPACE_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path)

    payload = cli.build_secret_file_payloads(
        {
            "GOOGLE_SERVICE_ACCOUNT_FILE": "keys/trr-backend-df2c438612e1.json",
            "FIREBASE_SERVICE_ACCOUNT_FILE": "keys/trr-web-25d2e-38499515994a.json",
            "SOCIAL_TWITTER_COOKIES_JSON": '{"auth_token":"token","ct0":"csrf"}',
            "TIKTOK_COOKIES_FILE": "data/tiktok_cookies.json",
            "SOCIAL_FACEBOOK_COOKIES_FILE": str(facebook_path),
            "SOCIAL_THREADS_COOKIES_FILE": str(threads_path),
        }
    )

    payload_by_name = {item["name"]: item["content"] for item in payload}

    assert json.loads(payload_by_name["trr-backend-gcp.json"]) == {"client_email": "bot@example.com"}
    assert json.loads(payload_by_name["firebase-service-account.json"]) == {"project_id": "trr-web-25d2e"}
    assert json.loads(payload_by_name["twikit-cookies.json"]) == {"auth_token": "token", "ct0": "csrf"}
    assert json.loads(payload_by_name["tiktok-cookies.json"]) == {
        "cookies": [{"name": "sessionid", "value": "abc"}]
    }
    assert json.loads(payload_by_name["facebook-cookies.json"]) == {
        "cookies": [{"name": "c_user", "value": "1"}]
    }
    assert json.loads(payload_by_name["threads-cookies.json"]) == {
        "cookies": [{"name": "ds_user_id", "value": "2"}]
    }
