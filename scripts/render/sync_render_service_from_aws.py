#!/usr/bin/env python3
"""Create or update the Render API service from the current TRR runtime contract."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

DEFAULT_SERVICE_NAME = "trr-backend"
DEFAULT_REPO_URL = "https://github.com/therealityreport/trr-backend.git"
DEFAULT_BRANCH = "main"
DEFAULT_RENDER_REGION = "virginia"
DEFAULT_PLAN = "standard"
DEFAULT_HEALTH_CHECK_PATH = "/health"
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_INSTANCE_ID = "i-01a7b672f5946d19a"
DEFAULT_OWNER_NAME = "The Reality's workspace"
DEFAULT_SSM_PATH = "/trr/staging/"
_POLL_SECONDS = 1.0
_POLL_ATTEMPTS = 30
_PAGE_SIZE = 100

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parent

SECRET_FILE_TARGETS = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/etc/secrets/trr-backend-gcp.json",
    "GOOGLE_SERVICE_ACCOUNT_FILE": "/etc/secrets/trr-backend-gcp.json",
    "FIREBASE_SERVICE_ACCOUNT_FILE": "/etc/secrets/firebase-service-account.json",
    "TWIKIT_COOKIES_FILE": "/etc/secrets/twikit-cookies.json",
    "TIKTOK_COOKIES_FILE": "/etc/secrets/tiktok-cookies.json",
    "SOCIAL_FACEBOOK_COOKIES_FILE": "/etc/secrets/facebook-cookies.json",
    "SOCIAL_THREADS_COOKIES_FILE": "/etc/secrets/threads-cookies.json",
}

SECRET_FILE_SPECS = {
    "trr-backend-gcp.json": (
        "GOOGLE_SERVICE_ACCOUNT_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ),
    "firebase-service-account.json": ("FIREBASE_SERVICE_ACCOUNT_FILE",),
    "twikit-cookies.json": ("TWIKIT_COOKIES_FILE",),
    "tiktok-cookies.json": ("TIKTOK_COOKIES_FILE", "SOCIAL_TIKTOK_COOKIES_FILE"),
    "facebook-cookies.json": ("SOCIAL_FACEBOOK_COOKIES_FILE", "FACEBOOK_COOKIES_FILE"),
    "threads-cookies.json": ("SOCIAL_THREADS_COOKIES_FILE", "THREADS_COOKIES_FILE"),
}

SECRET_FILE_LOCAL_FALLBACKS = {
    "trr-backend-gcp.json": (
        REPO_ROOT / "keys" / "trr-backend-df2c438612e1.json",
        Path.home() / "Downloads" / "trr-backend-df2c438612e1.json",
    ),
    "firebase-service-account.json": (
        REPO_ROOT / "keys" / "trr-web-25d2e-38499515994a.json",
        Path.home() / "Downloads" / "trr-web-25d2e-firebase-adminsdk-fbsvc-aeacdac9e1.json",
        Path.home() / "secrets-archive" / "TRR-APP" / "trr-web-25d2e-firebase-adminsdk-fbsvc-6f39292bd3.json",
    ),
    "tiktok-cookies.json": (
        REPO_ROOT / "data" / "tiktok_cookies.json",
    ),
    "facebook-cookies.json": (
        Path.home() / "secrets-archive" / "facebook_cookies.json",
    ),
    "threads-cookies.json": (
        Path.home() / "secrets-archive" / "threads_cookies.json",
    ),
}


class RenderAPIError(RuntimeError):
    """Raised when the Render API returns an unexpected response."""


@dataclass(frozen=True)
class RenderServiceConfig:
    name: str
    owner_id: str
    repo_url: str
    branch: str
    plan: str
    render_region: str
    health_check_path: str
    auto_deploy: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--render-api-key",
        default=os.getenv("RENDER_API_KEY", ""),
        help="Render API key. Defaults to RENDER_API_KEY.",
    )
    parser.add_argument(
        "--owner-id",
        default=os.getenv("RENDER_OWNER_ID", ""),
        help="Render workspace/team owner ID. If omitted, uses the first owner or the named default workspace.",
    )
    parser.add_argument(
        "--owner-name",
        default=DEFAULT_OWNER_NAME,
        help="Preferred Render owner name when owner-id is not supplied.",
    )
    parser.add_argument("--service-name", default=DEFAULT_SERVICE_NAME)
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL)
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    parser.add_argument("--plan", default=DEFAULT_PLAN)
    parser.add_argument("--render-region", default=DEFAULT_RENDER_REGION)
    parser.add_argument("--health-check-path", default=DEFAULT_HEALTH_CHECK_PATH)
    parser.add_argument("--auto-deploy", choices=("yes", "no"), default="no")
    parser.add_argument("--aws-region", default=DEFAULT_AWS_REGION)
    parser.add_argument("--instance-id", default=DEFAULT_INSTANCE_ID)
    parser.add_argument(
        "--ssm-path",
        default=DEFAULT_SSM_PATH,
        help="SSM parameter path used to overlay the current staging runtime contract.",
    )
    parser.add_argument(
        "--service-id",
        default="",
        help="Existing Render service ID. If omitted, resolves by service name.",
    )
    parser.add_argument(
        "--print-payload-only",
        action="store_true",
        help="Print the rendered service payload and env payload without calling Render.",
    )
    parser.add_argument(
        "--skip-trigger-deploy",
        action="store_true",
        help="When updating an existing service, do not trigger a fresh deploy after syncing config/env.",
    )
    return parser.parse_args()


def _aws_ssm_client(region: str):
    return boto3.client("ssm", region_name=region)


def fetch_live_api_env(*, region: str, instance_id: str) -> dict[str, str]:
    ssm = _aws_ssm_client(region)
    try:
        response = ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": ["cat /etc/trr-api.env"]},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "InvalidInstanceId":
            return {}
        raise
    command_id = response["Command"]["CommandId"]

    for _ in range(_POLL_ATTEMPTS):
        invocation = ssm.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        status = invocation["Status"]
        if status in {"Pending", "InProgress", "Delayed"}:
            time.sleep(_POLL_SECONDS)
            continue
        if status != "Success":
            error_output = invocation.get("StandardErrorContent", "")
            raise RuntimeError(
                f"Unable to read /etc/trr-api.env from {instance_id}: "
                f"{status} {error_output}"
            )
        return parse_env_text(invocation.get("StandardOutputContent", ""))

    raise TimeoutError(f"Timed out waiting for SSM command {command_id} on {instance_id}")


def fetch_ssm_env(*, region: str, path: str) -> dict[str, str]:
    ssm = _aws_ssm_client(region)
    next_token: str | None = None
    env: dict[str, str] = {}

    while True:
        params: dict[str, Any] = {
            "Path": path,
            "Recursive": True,
            "WithDecryption": True,
            "MaxResults": 10,
        }
        if next_token:
            params["NextToken"] = next_token
        response = ssm.get_parameters_by_path(**params)
        for item in response.get("Parameters", []):
            name = item["Name"].rsplit("/", 1)[-1]
            env[name] = item["Value"]
        next_token = response.get("NextToken")
        if not next_token:
            return env


def parse_env_text(text: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key] = value
    return env


def _resolve_candidate_path(candidate: str) -> Path | None:
    raw = (candidate or "").strip()
    if not raw:
        return None

    path = Path(raw).expanduser()
    candidate_paths = [path]
    if not path.is_absolute():
        candidate_paths.extend(
            [
                REPO_ROOT / path,
                WORKSPACE_ROOT / path,
                REPO_ROOT / "keys" / path.name,
                REPO_ROOT / "data" / path.name,
                Path.home() / "secrets-archive" / path.name,
                Path.home() / "Downloads" / path.name,
            ]
        )
    else:
        candidate_paths.extend(
            [
                REPO_ROOT / "keys" / path.name,
                REPO_ROOT / "data" / path.name,
                Path.home() / "secrets-archive" / path.name,
                Path.home() / "Downloads" / path.name,
            ]
        )

    for resolved in candidate_paths:
        if resolved.exists():
            return resolved
    return None


def normalize_env(env: dict[str, str], *, service_url: str | None) -> dict[str, str]:
    normalized = dict(env)

    for key, value in list(normalized.items()):
        if value.strip() == "__UNSET__":
            normalized.pop(key, None)

    for key, target in SECRET_FILE_TARGETS.items():
        if key in normalized:
            normalized[key] = target

    if service_url:
        normalized["TRR_API_URL"] = service_url.rstrip("/")

    return normalized


def build_service_payload(config: RenderServiceConfig, env: dict[str, str]) -> dict[str, Any]:
    return {
        "type": "web_service",
        "name": config.name,
        "ownerId": config.owner_id,
        "repo": config.repo_url,
        "branch": config.branch,
        "autoDeploy": config.auto_deploy,
        "envVars": build_env_var_payload(env),
        "serviceDetails": {
            "runtime": "docker",
            "plan": config.plan,
            "region": config.render_region,
            "healthCheckPath": config.health_check_path,
            "numInstances": 1,
        },
    }


def build_service_patch_payload(config: RenderServiceConfig) -> dict[str, Any]:
    return {
        "repo": config.repo_url,
        "branch": config.branch,
        "autoDeploy": config.auto_deploy,
        "serviceDetails": {
            "runtime": "docker",
            "plan": config.plan,
            "healthCheckPath": config.health_check_path,
        },
    }


def build_env_var_payload(env: dict[str, str]) -> list[dict[str, str]]:
    return [{"key": key, "value": value} for key, value in sorted(env.items())]


def build_secret_file_payloads(env: dict[str, str]) -> list[dict[str, str]]:
    payloads: list[dict[str, str]] = []

    for filename, source_keys in SECRET_FILE_SPECS.items():
        content: str | None = None
        if filename == "twikit-cookies.json":
            raw_json = (env.get("SOCIAL_TWITTER_COOKIES_JSON") or "").strip()
            if raw_json:
                content = json.dumps(json.loads(raw_json), indent=2, sort_keys=True)
            else:
                for source_key in source_keys:
                    resolved = _resolve_candidate_path(env.get(source_key, ""))
                    if resolved:
                        content = resolved.read_text()
                        break
        else:
            for source_key in source_keys:
                resolved = _resolve_candidate_path(env.get(source_key, ""))
                if resolved:
                    content = resolved.read_text()
                    break
        if content is None:
            for fallback_path in SECRET_FILE_LOCAL_FALLBACKS.get(filename, ()):
                if fallback_path.exists():
                    content = fallback_path.read_text()
                    break
        if content is None:
            raise FileNotFoundError(
                f"Unable to resolve content for Render secret file {filename} "
                f"from env keys {', '.join(source_keys)}"
            )
        payloads.append({"name": filename, "content": content})

    return payloads


def _render_request(
    api_key: str,
    method: str,
    path: str,
    *,
    payload: Any | None = None,
    query: dict[str, str] | None = None,
) -> tuple[int, Any]:
    url = f"https://api.render.com/v1{path}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"
    body = None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request) as response:
            data = response.read().decode("utf-8")
            return response.getcode(), json.loads(data) if data else None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"message": raw}
        return exc.code, payload


def select_owner_id(api_key: str, *, owner_id: str, owner_name: str) -> str:
    if owner_id:
        return owner_id
    status, payload = _render_request(api_key, "GET", "/owners")
    if status != 200:
        raise RenderAPIError(f"Unable to list Render owners: {status} {payload}")
    owners = [entry["owner"] for entry in payload]
    for owner in owners:
        if owner["name"] == owner_name:
            return owner["id"]
    if owners:
        return owners[0]["id"]
    raise RenderAPIError("Render account has no accessible owners/workspaces")


def find_service_by_name(api_key: str, *, name: str, owner_id: str) -> dict[str, Any] | None:
    status, payload = _render_request(api_key, "GET", "/services", query={"name": name, "ownerId": owner_id})
    if status != 200:
        raise RenderAPIError(f"Unable to list Render services: {status} {payload}")
    for service in payload:
        if service.get("service", {}).get("name") == name:
            return service["service"]
    return None


def get_service(api_key: str, *, service_id: str) -> dict[str, Any]:
    status, payload = _render_request(api_key, "GET", f"/services/{service_id}")
    if status != 200:
        raise RenderAPIError(f"Unable to fetch Render service {service_id}: {status} {payload}")
    return payload


def list_render_env_vars(api_key: str, *, service_id: str) -> dict[str, str]:
    cursor: str | None = None
    env: dict[str, str] = {}
    while True:
        query = {"limit": str(_PAGE_SIZE)}
        if cursor:
            query["cursor"] = cursor
        status, payload = _render_request(api_key, "GET", f"/services/{service_id}/env-vars", query=query)
        if status != 200:
            raise RenderAPIError(f"Unable to list Render env vars: {status} {payload}")
        batch = payload or []
        if not batch:
            return env
        for item in batch:
            env[item["envVar"]["key"]] = item["envVar"]["value"]
        if len(batch) < _PAGE_SIZE:
            return env
        cursor = batch[-1].get("cursor")


def replace_secret_files(api_key: str, *, service_id: str, payload: list[dict[str, str]]) -> list[dict[str, Any]]:
    status, response = _render_request(api_key, "PUT", f"/services/{service_id}/secret-files", payload=payload)
    if status != 200:
        raise RenderAPIError(f"Render secret-file sync failed: {status} {response}")
    return response


def create_service(api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    status, response = _render_request(api_key, "POST", "/services", payload=payload)
    if status != 201:
        raise RenderAPIError(f"Render service create failed: {status} {response}")
    return response


def update_service(api_key: str, *, service_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    status, response = _render_request(api_key, "PATCH", f"/services/{service_id}", payload=payload)
    if status != 200:
        raise RenderAPIError(f"Render service update failed: {status} {response}")
    return response


def replace_env_vars(api_key: str, *, service_id: str, payload: list[dict[str, str]]) -> list[dict[str, Any]]:
    status, response = _render_request(api_key, "PUT", f"/services/{service_id}/env-vars", payload=payload)
    if status != 200:
        raise RenderAPIError(f"Render env-var sync failed: {status} {response}")
    return response


def trigger_deploy(api_key: str, *, service_id: str) -> dict[str, Any] | None:
    status, response = _render_request(
        api_key,
        "POST",
        f"/services/{service_id}/deploys",
        payload={"clearCache": "do_not_clear"},
    )
    if status not in {201, 202}:
        raise RenderAPIError(f"Render deploy trigger failed: {status} {response}")
    return response


def _public_summary(*, action: str, service: dict[str, Any], deploy: dict[str, Any] | None) -> dict[str, Any]:
    summary = {
        "action": action,
        "service": {
            "id": service.get("id"),
            "name": service.get("name"),
            "slug": service.get("slug"),
            "dashboardUrl": service.get("dashboardUrl"),
        },
    }
    if deploy is not None:
        summary["deploy"] = deploy
    return summary


def build_effective_env(
    *,
    render_env: dict[str, str],
    live_env: dict[str, str],
    ssm_env: dict[str, str],
    service_url: str | None,
) -> dict[str, str]:
    env = dict(render_env)
    env.update(live_env)
    env.update(ssm_env)
    return normalize_env(env, service_url=service_url)


def main() -> int:
    args = _parse_args()
    if not args.render_api_key:
        raise SystemExit("RENDER_API_KEY is required")

    owner_id = select_owner_id(args.render_api_key, owner_id=args.owner_id, owner_name=args.owner_name)
    config = RenderServiceConfig(
        name=args.service_name,
        owner_id=owner_id,
        repo_url=args.repo_url,
        branch=args.branch,
        plan=args.plan,
        render_region=args.render_region,
        health_check_path=args.health_check_path,
        auto_deploy=args.auto_deploy,
    )

    existing = None
    if args.service_id:
        existing = get_service(args.render_api_key, service_id=args.service_id)
    else:
        existing = find_service_by_name(args.render_api_key, name=config.name, owner_id=owner_id)

    render_env = list_render_env_vars(args.render_api_key, service_id=existing["id"]) if existing else {}
    live_env = fetch_live_api_env(region=args.aws_region, instance_id=args.instance_id)
    ssm_env = fetch_ssm_env(region=args.aws_region, path=args.ssm_path)
    service_url = (
        existing.get("serviceDetails", {}).get("url")
        if existing
        else None
    )
    merged_env = dict(render_env)
    merged_env.update(live_env)
    merged_env.update(ssm_env)
    effective_env = normalize_env(merged_env, service_url=service_url)
    service_payload = build_service_payload(config, effective_env)
    env_payload = build_env_var_payload(effective_env)
    secret_file_payload = build_secret_file_payloads(merged_env)

    if args.print_payload_only:
        print(
            json.dumps(
                {
                    "ownerId": owner_id,
                    "servicePayload": service_payload,
                    "envPayload": env_payload,
                    "secretFilePayload": [
                        {"name": item["name"], "contentLength": len(item["content"])}
                        for item in secret_file_payload
                    ],
                },
                indent=2,
            )
        )
        return 0

    if existing is None:
        created = create_service(args.render_api_key, service_payload)
        print(
            json.dumps(
                _public_summary(
                    action="created",
                    service=created["service"],
                    deploy={"id": created.get("deployId")},
                ),
                indent=2,
            )
        )
        return 0

    updated = update_service(
        args.render_api_key,
        service_id=existing["id"],
        payload=build_service_patch_payload(config),
    )
    replace_env_vars(args.render_api_key, service_id=existing["id"], payload=env_payload)
    replace_secret_files(args.render_api_key, service_id=existing["id"], payload=secret_file_payload)
    deploy = None
    if not args.skip_trigger_deploy:
        deploy = trigger_deploy(args.render_api_key, service_id=existing["id"])
    print(json.dumps(_public_summary(action="updated", service=updated, deploy=deploy), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
