#!/usr/bin/env python3
"""Execute the final AWS teardown pass after the Render observation window closes."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

DEFAULT_REGION = "us-east-1"
DEFAULT_RENDER_BASE_URL = "https://trr-backend.onrender.com"
DEFAULT_VERCEL_PRODUCTION_URL = "https://trr-app.vercel.app"
DEFAULT_SHOW_ID = "7782652f-783a-488b-8860-41b97de32e75"
DEFAULT_ASG_NAME = "trr-api-asg"
DEFAULT_LAUNCH_TEMPLATE_NAME = "trr-api-lt"
DEFAULT_LOAD_BALANCER_NAME = "trr-api-alb"
DEFAULT_TARGET_GROUP_NAME = "trr-api-tg"
DEFAULT_PRIVATE_ROUTE_TABLE_ID = "rtb-04176689ad967a8ae"
DEFAULT_NAT_GATEWAY_ID = "nat-004581b7931e685e7"
DEFAULT_NAT_EIP_ALLOCATION_ID = "eipalloc-0c6c7ef0913e7a3d8"
DEFAULT_ALB_SECURITY_GROUP_ID = "sg-054ae25e1699a3845"
DEFAULT_API_SECURITY_GROUP_ID = "sg-09ad087d9a6b689dd"
DEFAULT_RDS_SNAPSHOT_ID = "trr-metadata-db-final-2026-03-07"
DEFAULT_METRICS_LAMBDA_NAME = "trr-jobplane-metrics-publisher"
DEFAULT_METRICS_LAMBDA_ROLE_NAME = "trr-metrics-publisher-role"
DEFAULT_OBSERVATION_WINDOW_END = "2026-03-13T16:09:13-04:00"
DEFAULT_DELETE_ALARMS = (
    "trr-api-target-5xx",
    "trr-api-target-5xx-high",
    "trr-long-job-failures-high",
    "trr-queue-depth-high",
    "trr-stale-leases-high",
)
DEFAULT_DELETE_LOG_GROUPS = (
    "/aws/lambda/trr-jobplane-metrics-publisher",
    "/aws/ssm/AWS-RunShellScript",
    "/trr/api/bootstrap",
    "/trr/ec2/cloud-init",
    "/trr/ec2/cloud-init-output",
    "/trr/worker/bootstrap",
)
DEFAULT_DELETE_BUCKETS = (
    "trr-backend",
    "screenalytics",
    "ltsr-data-bucket",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
_TIMEOUT = 30
_POLL_SECONDS = 5


class TeardownError(RuntimeError):
    """Raised when a teardown validation or action fails."""


@dataclass(frozen=True)
class ResourceConfig:
    region: str
    render_base_url: str
    vercel_production_url: str
    show_id: str
    asg_name: str
    launch_template_name: str
    load_balancer_name: str
    target_group_name: str
    private_route_table_id: str
    nat_gateway_id: str
    nat_eip_allocation_id: str
    alb_security_group_id: str
    api_security_group_id: str
    rds_snapshot_id: str
    metrics_lambda_name: str
    metrics_lambda_role_name: str
    observation_window_end: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--render-base-url", default=DEFAULT_RENDER_BASE_URL)
    parser.add_argument("--vercel-production-url", default=DEFAULT_VERCEL_PRODUCTION_URL)
    parser.add_argument("--show-id", default=DEFAULT_SHOW_ID)
    parser.add_argument("--asg-name", default=DEFAULT_ASG_NAME)
    parser.add_argument("--launch-template-name", default=DEFAULT_LAUNCH_TEMPLATE_NAME)
    parser.add_argument("--load-balancer-name", default=DEFAULT_LOAD_BALANCER_NAME)
    parser.add_argument("--target-group-name", default=DEFAULT_TARGET_GROUP_NAME)
    parser.add_argument("--private-route-table-id", default=DEFAULT_PRIVATE_ROUTE_TABLE_ID)
    parser.add_argument("--nat-gateway-id", default=DEFAULT_NAT_GATEWAY_ID)
    parser.add_argument("--nat-eip-allocation-id", default=DEFAULT_NAT_EIP_ALLOCATION_ID)
    parser.add_argument("--alb-security-group-id", default=DEFAULT_ALB_SECURITY_GROUP_ID)
    parser.add_argument("--api-security-group-id", default=DEFAULT_API_SECURITY_GROUP_ID)
    parser.add_argument("--rds-snapshot-id", default=DEFAULT_RDS_SNAPSHOT_ID)
    parser.add_argument("--metrics-lambda-name", default=DEFAULT_METRICS_LAMBDA_NAME)
    parser.add_argument("--metrics-lambda-role-name", default=DEFAULT_METRICS_LAMBDA_ROLE_NAME)
    parser.add_argument("--observation-window-end", default=DEFAULT_OBSERVATION_WINDOW_END)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform the destructive teardown. Without this flag the script only validates readiness.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the final result as JSON instead of human-readable text.",
    )
    return parser.parse_args()


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key] = value
    return env


def _service_role_key() -> str:
    direct = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if direct:
        return direct
    env_data = _load_env_file(REPO_ROOT / ".env")
    value = str(env_data.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if value:
        return value
    raise TeardownError("SUPABASE_SERVICE_ROLE_KEY is required for the admin-route precheck.")


def _http_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, str]:
    data: bytes | None = None
    request_headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            payload = response.read().decode("utf-8", errors="replace")
            return int(response.getcode()), payload
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), payload


def _boto3_clients(region: str) -> dict[str, Any]:
    session = boto3.session.Session(region_name=region)
    return {
        "autoscaling": session.client("autoscaling"),
        "ec2": session.client("ec2"),
        "elbv2": session.client("elbv2"),
        "cloudwatch": session.client("cloudwatch"),
        "logs": session.client("logs"),
        "rds": session.client("rds"),
        "lambda": session.client("lambda"),
        "iam": session.client("iam"),
        "s3": session.client("s3"),
    }


def _parse_lb_arn_suffix(load_balancer_arn: str) -> str:
    return load_balancer_arn.split("loadbalancer/", 1)[-1]


def _describe_asg(client: Any, name: str) -> dict[str, Any] | None:
    response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[name])
    groups = response.get("AutoScalingGroups") or []
    return groups[0] if groups else None


def _describe_launch_template(client: Any, name: str) -> dict[str, Any] | None:
    try:
        response = client.describe_launch_templates(LaunchTemplateNames=[name])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "InvalidLaunchTemplateName.NotFoundException":
            return None
        raise
    templates = response.get("LaunchTemplates") or []
    return templates[0] if templates else None


def _describe_load_balancer(client: Any, name: str) -> dict[str, Any] | None:
    try:
        response = client.describe_load_balancers(Names=[name])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "LoadBalancerNotFound":
            return None
        raise
    balancers = response.get("LoadBalancers") or []
    return balancers[0] if balancers else None


def _describe_target_group(client: Any, name: str) -> dict[str, Any] | None:
    try:
        response = client.describe_target_groups(Names=[name])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "TargetGroupNotFound":
            return None
        raise
    groups = response.get("TargetGroups") or []
    return groups[0] if groups else None


def _describe_nat_gateway(client: Any, nat_gateway_id: str) -> dict[str, Any] | None:
    response = client.describe_nat_gateways(NatGatewayIds=[nat_gateway_id])
    gateways = response.get("NatGateways") or []
    return gateways[0] if gateways else None


def _list_addresses(client: Any) -> list[dict[str, Any]]:
    response = client.describe_addresses()
    return response.get("Addresses") or []


def _describe_security_group(client: Any, group_id: str) -> dict[str, Any] | None:
    try:
        response = client.describe_security_groups(GroupIds=[group_id])
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "InvalidGroup.NotFound":
            return None
        raise
    groups = response.get("SecurityGroups") or []
    return groups[0] if groups else None


def _describe_snapshot(client: Any, snapshot_id: str) -> dict[str, Any] | None:
    try:
        response = client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "DBSnapshotNotFound":
            return None
        raise
    snapshots = response.get("DBSnapshots") or []
    return snapshots[0] if snapshots else None


def _describe_lambda_function(client: Any, function_name: str) -> dict[str, Any] | None:
    try:
        response = client.get_function(FunctionName=function_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "ResourceNotFoundException":
            return None
        raise
    return response.get("Configuration") or None


def _describe_role(client: Any, role_name: str) -> dict[str, Any] | None:
    try:
        response = client.get_role(RoleName=role_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "NoSuchEntity":
            return None
        raise
    return response.get("Role") or None


def _list_bucket_names(client: Any) -> set[str]:
    response = client.list_buckets()
    return {
        str(bucket.get("Name") or "").strip()
        for bucket in response.get("Buckets") or []
        if str(bucket.get("Name") or "").strip()
    }


def _list_alarm_names(client: Any) -> set[str]:
    paginator = client.get_paginator("describe_alarms")
    names: set[str] = set()
    for page in paginator.paginate():
        for alarm in page.get("MetricAlarms") or []:
            name = str(alarm.get("AlarmName") or "").strip()
            if name:
                names.add(name)
    return names


def _log_group_exists(client: Any, name: str) -> bool:
    response = client.describe_log_groups(logGroupNamePrefix=name, limit=5)
    for entry in response.get("logGroups") or []:
        if str(entry.get("logGroupName") or "") == name:
            return True
    return False


def _wait_for(predicate: Any, *, description: str, timeout: int = 600) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(_POLL_SECONDS)
    raise TeardownError(f"Timed out waiting for {description}")


def _ensure_observation_window_elapsed(iso_timestamp: str) -> None:
    aware_window_end = datetime.fromisoformat(iso_timestamp)
    now = datetime.now(ZoneInfo("America/New_York"))
    if now < aware_window_end.astimezone(ZoneInfo("America/New_York")):
        raise TeardownError(
            "Observation window has not elapsed yet. "
            f"Now={now.isoformat()} earliest={aware_window_end.astimezone(ZoneInfo('America/New_York')).isoformat()}"
        )


def _precheck_http(config: ResourceConfig) -> dict[str, Any]:
    status, body = _http_request(f"{config.render_base_url}/health")
    if status != 200:
        raise TeardownError(f"Render /health failed: status={status} body={body[:300]}")

    show_status, show_body = _http_request(
        f"{config.render_base_url}/api/v1/shows/{config.show_id}",
        headers={"Accept": "application/json"},
    )
    if show_status != 200:
        raise TeardownError(f"Render show read failed: status={show_status} body={show_body[:300]}")

    service_role = _service_role_key()
    admin_status, admin_body = _http_request(
        f"{config.render_base_url}/api/v1/admin/shows/{config.show_id}/google-news/sync",
        method="POST",
        headers={"Authorization": f"Bearer {service_role}", "Accept": "application/json"},
        body={},
    )
    if admin_status != 200:
        raise TeardownError(f"Render Modal-backed admin route failed: status={admin_status} body={admin_body[:300]}")
    try:
        admin_payload = json.loads(admin_body)
    except json.JSONDecodeError as exc:
        raise TeardownError(f"Render admin route returned invalid JSON: {exc}") from exc
    backend = str(admin_payload.get("execution_backend_canonical") or "").strip().lower()
    if backend != "modal":
        raise TeardownError(f"Render admin route is not Modal-backed: execution_backend_canonical={backend!r}")

    vercel_status, vercel_body = _http_request(config.vercel_production_url)
    if vercel_status != 200:
        raise TeardownError(f"Vercel production root failed: status={vercel_status} body={vercel_body[:300]}")

    return {
        "render_health_status": status,
        "render_show_status": show_status,
        "render_admin_status": admin_status,
        "render_admin_execution_backend": backend,
        "vercel_production_status": vercel_status,
    }


def _inventory(clients: dict[str, Any], config: ResourceConfig) -> dict[str, Any]:
    lb = _describe_load_balancer(clients["elbv2"], config.load_balancer_name)
    addresses = _list_addresses(clients["ec2"])
    alb_public_ips = [entry.get("PublicIp") for entry in addresses if entry.get("ServiceManaged") == "alb"]
    return {
        "asg": _describe_asg(clients["autoscaling"], config.asg_name),
        "launch_template": _describe_launch_template(clients["ec2"], config.launch_template_name),
        "load_balancer": lb,
        "target_group": _describe_target_group(clients["elbv2"], config.target_group_name),
        "private_route_table": clients["ec2"].describe_route_tables(RouteTableIds=[config.private_route_table_id]).get(
            "RouteTables", []
        ),
        "nat_gateway": _describe_nat_gateway(clients["ec2"], config.nat_gateway_id),
        "nat_eip_present": any(
            str(entry.get("AllocationId") or "") == config.nat_eip_allocation_id for entry in addresses
        ),
        "alb_public_ips": [ip for ip in alb_public_ips if ip],
        "alb_security_group": _describe_security_group(clients["ec2"], config.alb_security_group_id),
        "api_security_group": _describe_security_group(clients["ec2"], config.api_security_group_id),
        "snapshot": _describe_snapshot(clients["rds"], config.rds_snapshot_id),
        "metrics_lambda": _describe_lambda_function(clients["lambda"], config.metrics_lambda_name),
        "metrics_role": _describe_role(clients["iam"], config.metrics_lambda_role_name),
        "buckets_present": sorted(name for name in DEFAULT_DELETE_BUCKETS if name in _list_bucket_names(clients["s3"])),
        "alarm_names": sorted(_list_alarm_names(clients["cloudwatch"])),
        "log_groups_present": {
            name: _log_group_exists(clients["logs"], name) for name in DEFAULT_DELETE_LOG_GROUPS
        },
        "ec2_instances_running_or_stopped": clients["ec2"]
        .describe_instances(
            Filters=[
                {
                    "Name": "instance-state-name",
                    "Values": ["running", "pending", "stopping", "stopped"],
                }
            ]
        )
        .get("Reservations", []),
        "volumes_present": clients["ec2"]
        .describe_volumes(Filters=[{"Name": "status", "Values": ["available", "in-use"]}])
        .get("Volumes", []),
    }


def _delete_asg(client: Any, name: str) -> None:
    group = _describe_asg(client, name)
    if not group:
        return
    client.delete_auto_scaling_group(AutoScalingGroupName=name, ForceDelete=True)
    _wait_for(lambda: _describe_asg(client, name) is None, description=f"ASG {name} deletion")


def _delete_launch_template(client: Any, name: str) -> None:
    template = _describe_launch_template(client, name)
    if not template:
        return
    client.delete_launch_template(LaunchTemplateName=name)
    _wait_for(lambda: _describe_launch_template(client, name) is None, description=f"launch template {name} deletion")


def _delete_load_balancer(client: Any, name: str) -> None:
    lb = _describe_load_balancer(client, name)
    if not lb:
        return
    client.delete_load_balancer(LoadBalancerArn=lb["LoadBalancerArn"])
    _wait_for(lambda: _describe_load_balancer(client, name) is None, description=f"ALB {name} deletion")


def _delete_target_group(client: Any, name: str) -> None:
    tg = _describe_target_group(client, name)
    if not tg:
        return
    client.delete_target_group(TargetGroupArn=tg["TargetGroupArn"])
    _wait_for(lambda: _describe_target_group(client, name) is None, description=f"target group {name} deletion")


def _delete_nat_route(client: Any, route_table_id: str, nat_gateway_id: str) -> None:
    route_tables = client.describe_route_tables(RouteTableIds=[route_table_id]).get("RouteTables") or []
    if not route_tables:
        return
    routes = route_tables[0].get("Routes") or []
    has_nat_route = any(
        str(route.get("DestinationCidrBlock") or "") == "0.0.0.0/0"
        and str(route.get("NatGatewayId") or "") == nat_gateway_id
        for route in routes
    )
    if not has_nat_route:
        return
    client.delete_route(RouteTableId=route_table_id, DestinationCidrBlock="0.0.0.0/0")
    _wait_for(
        lambda: not any(
            str(route.get("DestinationCidrBlock") or "") == "0.0.0.0/0"
            and str(route.get("NatGatewayId") or "") == nat_gateway_id
            for route in (
                client.describe_route_tables(RouteTableIds=[route_table_id]).get("RouteTables") or [{}]
            )[0].get("Routes", [])
        ),
        description=f"NAT route removal from {route_table_id}",
    )


def _delete_security_group(client: Any, group_id: str) -> None:
    group = _describe_security_group(client, group_id)
    if not group:
        return
    try:
        client.delete_security_group(GroupId=group_id)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code == "DependencyViolation":
            raise TeardownError(
                f"Security group {group_id} still has dependencies. Stop and inspect before forcing anything."
            ) from exc
        raise
    _wait_for(
        lambda: _describe_security_group(client, group_id) is None,
        description=f"security group {group_id} deletion",
    )


def _delete_nat_gateway(client: Any, nat_gateway_id: str) -> None:
    gateway = _describe_nat_gateway(client, nat_gateway_id)
    if not gateway:
        return
    state = str(gateway.get("State") or "").lower()
    if state == "deleted":
        return
    if state not in {"deleting", "deleted"}:
        client.delete_nat_gateway(NatGatewayId=nat_gateway_id)
    _wait_for(
        lambda: (_describe_nat_gateway(client, nat_gateway_id) or {}).get("State") == "deleted",
        description=f"NAT gateway {nat_gateway_id} deletion",
        timeout=900,
    )


def _release_eip(client: Any, allocation_id: str) -> None:
    addresses = _list_addresses(client)
    if not any(str(entry.get("AllocationId") or "") == allocation_id for entry in addresses):
        return
    client.release_address(AllocationId=allocation_id)
    _wait_for(
        lambda: not any(str(entry.get("AllocationId") or "") == allocation_id for entry in _list_addresses(client)),
        description=f"EIP {allocation_id} release",
    )


def _delete_alarms(client: Any, names: tuple[str, ...]) -> None:
    existing = _list_alarm_names(client)
    targets = [name for name in names if name in existing]
    if not targets:
        return
    client.delete_alarms(AlarmNames=targets)
    _wait_for(
        lambda: not any(name in _list_alarm_names(client) for name in targets),
        description=f"CloudWatch alarms {', '.join(targets)} deletion",
    )


def _delete_log_groups(client: Any, names: tuple[str, ...]) -> None:
    for name in names:
        if not _log_group_exists(client, name):
            continue
        client.delete_log_group(logGroupName=name)
    _wait_for(
        lambda: not any(_log_group_exists(client, name) for name in names),
        description="legacy CloudWatch log group deletion",
    )


def _delete_snapshot(client: Any, snapshot_id: str) -> None:
    snapshot = _describe_snapshot(client, snapshot_id)
    if not snapshot:
        return
    client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_id)
    _wait_for(lambda: _describe_snapshot(client, snapshot_id) is None, description=f"snapshot {snapshot_id} deletion")


def _delete_lambda_function(client: Any, function_name: str) -> None:
    config = _describe_lambda_function(client, function_name)
    if not config:
        return
    client.delete_function(FunctionName=function_name)
    _wait_for(
        lambda: _describe_lambda_function(client, function_name) is None,
        description=f"Lambda function {function_name} deletion",
    )


def _delete_role(client: Any, role_name: str) -> None:
    role = _describe_role(client, role_name)
    if not role:
        return
    attached = client.list_attached_role_policies(RoleName=role_name).get("AttachedPolicies") or []
    for policy in attached:
        policy_arn = str(policy.get("PolicyArn") or "").strip()
        if policy_arn:
            client.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    inline_names = client.list_role_policies(RoleName=role_name).get("PolicyNames") or []
    for policy_name in inline_names:
        client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
    client.delete_role(RoleName=role_name)
    _wait_for(
        lambda: _describe_role(client, role_name) is None,
        description=f"IAM role {role_name} deletion",
    )


def _empty_bucket(client: Any, bucket_name: str) -> None:
    paginator = client.get_paginator("list_object_versions")
    try:
        pages = paginator.paginate(Bucket=bucket_name)
        has_versioning_payload = True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code not in {"NoSuchBucket"}:
            raise
        return

    objects_to_delete: list[dict[str, str]] = []
    if has_versioning_payload:
        for page in pages:
            for item in page.get("Versions") or []:
                key = str(item.get("Key") or "")
                version_id = str(item.get("VersionId") or "")
                if key and version_id:
                    objects_to_delete.append({"Key": key, "VersionId": version_id})
            for item in page.get("DeleteMarkers") or []:
                key = str(item.get("Key") or "")
                version_id = str(item.get("VersionId") or "")
                if key and version_id:
                    objects_to_delete.append({"Key": key, "VersionId": version_id})

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for item in page.get("Contents") or []:
            key = str(item.get("Key") or "")
            if key:
                objects_to_delete.append({"Key": key})

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in objects_to_delete:
        dedupe_key = (item["Key"], item.get("VersionId", ""))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(item)

    for idx in range(0, len(deduped), 1000):
        chunk = deduped[idx : idx + 1000]
        client.delete_objects(Bucket=bucket_name, Delete={"Objects": chunk, "Quiet": True})


def _delete_bucket(client: Any, bucket_name: str) -> None:
    if bucket_name not in _list_bucket_names(client):
        return
    _empty_bucket(client, bucket_name)
    client.delete_bucket(Bucket=bucket_name)
    _wait_for(
        lambda: bucket_name not in _list_bucket_names(client),
        description=f"S3 bucket {bucket_name} deletion",
        timeout=300,
    )


def _verify_post_teardown(
    clients: dict[str, Any],
    config: ResourceConfig,
    prior_alb_public_ips: list[str],
) -> dict[str, Any]:
    inventory = _inventory(clients, config)
    if inventory["asg"] is not None:
        raise TeardownError(f"{config.asg_name} still exists after teardown.")
    if inventory["launch_template"] is not None:
        raise TeardownError(f"{config.launch_template_name} still exists after teardown.")
    if inventory["load_balancer"] is not None:
        raise TeardownError(f"{config.load_balancer_name} still exists after teardown.")
    if inventory["target_group"] is not None:
        raise TeardownError(f"{config.target_group_name} still exists after teardown.")
    route_tables = inventory["private_route_table"] or []
    if route_tables:
        routes = route_tables[0].get("Routes") or []
        if any(
            str(route.get("DestinationCidrBlock") or "") == "0.0.0.0/0"
            and str(route.get("NatGatewayId") or "") == config.nat_gateway_id
            for route in routes
        ):
            raise TeardownError(f"{config.private_route_table_id} still points 0.0.0.0/0 at {config.nat_gateway_id}.")
    nat_gateway = inventory["nat_gateway"]
    if nat_gateway is not None and str(nat_gateway.get("State") or "").lower() != "deleted":
        raise TeardownError(f"{config.nat_gateway_id} still exists after teardown.")
    if inventory["nat_eip_present"]:
        raise TeardownError(f"{config.nat_eip_allocation_id} is still allocated after teardown.")
    remaining_alb_public_ips = set(inventory["alb_public_ips"])
    leaked_public_ips = sorted(set(prior_alb_public_ips) & remaining_alb_public_ips)
    if leaked_public_ips:
        raise TeardownError(f"ALB-managed public IPv4 addresses still present after teardown: {leaked_public_ips}")
    if inventory["alb_security_group"] is not None:
        raise TeardownError(f"{config.alb_security_group_id} still exists after teardown.")
    if inventory["api_security_group"] is not None:
        raise TeardownError(f"{config.api_security_group_id} still exists after teardown.")
    if inventory["snapshot"] is not None:
        raise TeardownError(f"{config.rds_snapshot_id} still exists after teardown.")
    if inventory["metrics_lambda"] is not None:
        raise TeardownError(f"{config.metrics_lambda_name} still exists after teardown.")
    if inventory["metrics_role"] is not None:
        raise TeardownError(f"{config.metrics_lambda_role_name} still exists after teardown.")
    remaining_alarms = set(inventory["alarm_names"])
    for alarm_name in DEFAULT_DELETE_ALARMS:
        if alarm_name in remaining_alarms:
            raise TeardownError(f"Deleted alarm {alarm_name} is still present after teardown.")
    for log_group_name, present in inventory["log_groups_present"].items():
        if present:
            raise TeardownError(f"Deleted log group {log_group_name} is still present after teardown.")
    if inventory["buckets_present"]:
        raise TeardownError(f"Deleted S3 buckets are still present after teardown: {inventory['buckets_present']}")
    if inventory["ec2_instances_running_or_stopped"]:
        raise TeardownError("Unexpected EC2 instances appeared during teardown.")
    if inventory["volumes_present"]:
        raise TeardownError("Unexpected EBS volumes appeared during teardown.")
    _precheck_http(config)
    return inventory


def _as_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=2, sort_keys=True, default=str)


def main() -> int:
    args = _parse_args()
    config = ResourceConfig(
        region=args.region,
        render_base_url=args.render_base_url.rstrip("/"),
        vercel_production_url=args.vercel_production_url.rstrip("/"),
        show_id=args.show_id,
        asg_name=args.asg_name,
        launch_template_name=args.launch_template_name,
        load_balancer_name=args.load_balancer_name,
        target_group_name=args.target_group_name,
        private_route_table_id=args.private_route_table_id,
        nat_gateway_id=args.nat_gateway_id,
        nat_eip_allocation_id=args.nat_eip_allocation_id,
        alb_security_group_id=args.alb_security_group_id,
        api_security_group_id=args.api_security_group_id,
        rds_snapshot_id=args.rds_snapshot_id,
        metrics_lambda_name=args.metrics_lambda_name,
        metrics_lambda_role_name=args.metrics_lambda_role_name,
        observation_window_end=args.observation_window_end,
    )
    clients = _boto3_clients(config.region)

    try:
        _ensure_observation_window_elapsed(config.observation_window_end)
        precheck_http = _precheck_http(config)
        inventory_before = _inventory(clients, config)
        result: dict[str, Any] = {
            "mode": "execute" if args.execute else "check-only",
            "precheck_http": precheck_http,
            "inventory_before": inventory_before,
        }

        if inventory_before["ec2_instances_running_or_stopped"]:
            raise TeardownError("Expected no EC2 instances before teardown, but some are still present.")
        if inventory_before["volumes_present"]:
            raise TeardownError("Expected no EBS volumes before teardown, but some are still present.")

        if not args.execute:
            result["ready_for_execute"] = True
            output = _as_json(result) if args.json else (
                "AWS teardown check-only passed.\n"
                f"Observation window ended: {config.observation_window_end}\n"
                f"Render/Modal/Vercel prechecks: ok\n"
                f"ASG present: {inventory_before['asg'] is not None}\n"
                f"ALB present: {inventory_before['load_balancer'] is not None}\n"
                f"NAT present: {inventory_before['nat_gateway'] is not None}\n"
                f"Snapshot present: {inventory_before['snapshot'] is not None}\n"
            )
            print(output)
            return 0

        prior_alb_public_ips = list(inventory_before["alb_public_ips"])
        _delete_asg(clients["autoscaling"], config.asg_name)
        _delete_launch_template(clients["ec2"], config.launch_template_name)
        _delete_load_balancer(clients["elbv2"], config.load_balancer_name)
        _delete_target_group(clients["elbv2"], config.target_group_name)
        _delete_nat_route(clients["ec2"], config.private_route_table_id, config.nat_gateway_id)
        _delete_security_group(clients["ec2"], config.alb_security_group_id)
        _delete_security_group(clients["ec2"], config.api_security_group_id)
        _delete_nat_gateway(clients["ec2"], config.nat_gateway_id)
        _release_eip(clients["ec2"], config.nat_eip_allocation_id)
        _delete_alarms(clients["cloudwatch"], DEFAULT_DELETE_ALARMS)
        _delete_log_groups(clients["logs"], DEFAULT_DELETE_LOG_GROUPS)
        _delete_lambda_function(clients["lambda"], config.metrics_lambda_name)
        _delete_role(clients["iam"], config.metrics_lambda_role_name)
        _delete_snapshot(clients["rds"], config.rds_snapshot_id)
        for bucket_name in DEFAULT_DELETE_BUCKETS:
            _delete_bucket(clients["s3"], bucket_name)
        inventory_after = _verify_post_teardown(clients, config, prior_alb_public_ips)
        result["inventory_after"] = inventory_after
        output = (
            _as_json(result)
            if args.json
            else "AWS teardown execute passed.\nAll targeted AWS remnants were deleted.\n"
        )
        print(output)
        return 0
    except TeardownError as exc:
        if args.json:
            print(_as_json({"ok": False, "error": str(exc)}))
        else:
            print(f"AWS teardown pass failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
