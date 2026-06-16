#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import ssl
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_DECODO_GATEWAY = "gate.decodo.com:10001"
EXHAUSTED_TRAFFIC_RE = re.compile(
    r"\b(no|zero|0)\s+(traffic|balance|credit|credits)\b"
    r"|\b(traffic|quota|balance|credit|credits|bandwidth)\b.*\b(exhausted|depleted|insufficient|used up|exceeded|limit|empty)\b"
    r"|\b(exhausted|depleted|insufficient|used up|exceeded)\b.*\b(traffic|quota|balance|credit|credits|bandwidth)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ProxyConfig:
    proxy_url: str
    source: str
    host: str
    port: int
    protocol: str
    username: str
    password: str


def read_env_file(path: Path = SOURCE_ENV_PATH) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return values


def launchctl_getenv(key: str) -> str:
    try:
        completed = subprocess.run(
            ["launchctl", "getenv", key],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return ""
    if completed.returncode != 0:
        return ""
    return completed.stdout.strip()


def resolve_value(key: str, env_file: dict[str, str]) -> tuple[str, str]:
    runtime_value = str(os.getenv(key) or "").strip()
    if runtime_value:
        return runtime_value, f"env:{key}"
    file_value = str(env_file.get(key) or "").strip()
    if file_value:
        return file_value, f".env:{key}"
    launchctl_value = launchctl_getenv(key)
    if launchctl_value:
        return launchctl_value, f"launchctl:{key}"
    return "", ""


def load_proxy_config(env_file_path: Path = SOURCE_ENV_PATH) -> ProxyConfig | None:
    env_file = read_env_file(env_file_path)
    proxy_url, proxy_url_source = resolve_value("DECODO_PROXY_URL", env_file)
    if proxy_url:
        return parse_proxy_url(proxy_url, proxy_url_source)

    username, username_source = resolve_value("DECODO_USERNAME", env_file)
    password, password_source = resolve_value("DECODO_PASSWORD", env_file)
    gateway, gateway_source = resolve_value("DECODO_GATEWAY", env_file)
    if not username or not password:
        return None
    gateway = gateway or DEFAULT_DECODO_GATEWAY
    gateway_source = gateway_source or "default:DECODO_GATEWAY"
    host, port = parse_gateway(gateway)
    encoded_user = quote(username, safe="")
    encoded_password = quote(password, safe="")
    source = f"{username_source}+{password_source}+{gateway_source}"
    return parse_proxy_url(f"http://{encoded_user}:{encoded_password}@{host}:{port}", source)


def parse_gateway(gateway: str) -> tuple[str, int]:
    if "://" in gateway:
        parsed = urlparse(gateway)
        host = parsed.hostname or ""
        port = parsed.port
    else:
        host, _, raw_port = gateway.rpartition(":")
        port = int(raw_port) if raw_port else None
    if not host or not port:
        raise ValueError("DECODO_GATEWAY must include host and port")
    return host, port


def parse_proxy_url(proxy_url: str, source: str) -> ProxyConfig:
    parsed = urlparse(proxy_url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("DECODO_PROXY_URL must use http or https for this smoke check")
    if not parsed.hostname or not parsed.port or not parsed.username or not parsed.password:
        raise ValueError("DECODO_PROXY_URL must include host, port, username, and password")
    return ProxyConfig(
        proxy_url=proxy_url,
        source=source,
        host=parsed.hostname,
        port=int(parsed.port),
        protocol=parsed.scheme,
        username=unquote(parsed.username),
        password=unquote(parsed.password),
    )


def redact_proxy_url(proxy_url: str) -> str:
    parsed = urlparse(proxy_url)
    if not parsed.hostname:
        return "<redacted>"
    username = "<redacted>" if parsed.username else ""
    password = ":<redacted>" if parsed.password else ""
    auth = f"{username}{password}@" if username or password else ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{auth}{parsed.hostname}{port}"


def classify_proxy_response(http_status: int | None, response_text: str = "", error: str = "") -> str | None:
    text = f"{http_status or ''} {response_text} {error}"
    if http_status == 402 or EXHAUSTED_TRAFFIC_RE.search(text):
        return "traffic_exhausted_or_plan_limit"
    if http_status == 407:
        return "proxy_auth_407"
    if http_status == 403 and re.search(r"\b(plan|subscription|payment|billing|blocked|disabled)\b", text, re.I):
        return "proxy_plan_or_account_blocked"
    if http_status and http_status >= 400:
        return "proxy_http_error"
    if error:
        return "network_error"
    return None


def probe_proxy(config: ProxyConfig, *, target_host: str, target_port: int, timeout_seconds: float) -> dict[str, Any]:
    response = ""
    try:
        raw_socket = socket.create_connection((config.host, config.port), timeout=timeout_seconds)
        with raw_socket:
            raw_socket.settimeout(timeout_seconds)
            sock: socket.socket | ssl.SSLSocket
            if config.protocol == "https":
                context = ssl.create_default_context()
                sock = context.wrap_socket(raw_socket, server_hostname=config.host)
            else:
                sock = raw_socket
            with sock:
                auth = base64.b64encode(f"{config.username}:{config.password}".encode("utf-8")).decode("ascii")
                request = (
                    f"CONNECT {target_host}:{target_port} HTTP/1.1\r\n"
                    f"Host: {target_host}:{target_port}\r\n"
                    f"Proxy-Authorization: Basic {auth}\r\n"
                    "User-Agent: trr-decodo-residential-smoke/1.0\r\n"
                    "Proxy-Connection: close\r\n\r\n"
                )
                sock.sendall(request.encode("ascii"))
                while "\r\n\r\n" not in response and len(response) < 16384:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response += chunk.decode("utf-8", errors="replace")
    except OSError as exc:
        return {
            "state": "fail",
            "reason": "decodo_proxy_network_error",
            "failure_class": classify_proxy_response(None, error=str(exc)),
            "error": str(exc),
        }

    first_line = response.splitlines()[0] if response.splitlines() else ""
    match = re.search(r"HTTP/\d(?:\.\d)?\s+(\d{3})", first_line)
    http_status = int(match.group(1)) if match else None
    if http_status and 200 <= http_status < 300:
        return {
            "state": "ok",
            "reason": None,
            "http_status": http_status,
            "details": f"CONNECT tunnel established to {target_host}:{target_port}",
        }
    failure_class = classify_proxy_response(http_status, response)
    return {
        "state": "fail",
        "reason": failure_class or "decodo_proxy_smoke_failed",
        "http_status": http_status,
        "failure_class": failure_class,
        "traffic_exhausted_suspected": failure_class == "traffic_exhausted_or_plan_limit",
        "details": first_line or "No proxy response status line received.",
        "remediation": remediation_for_failure(failure_class),
    }


def remediation_for_failure(failure_class: str | None) -> str:
    if failure_class == "traffic_exhausted_or_plan_limit":
        return "Upgrade/add Decodo residential traffic or wait for the residential proxy plan to renew; TRR does not need Web Scraping API activation for this smoke."
    if failure_class == "proxy_auth_407":
        return "Confirm the dashboard-generated Decodo residential proxy username, password, endpoint, and account access. A Web Scraping API token will not fix this TRR proxy path."
    if failure_class == "proxy_plan_or_account_blocked":
        return "Resolve the Decodo residential proxy account/plan block in the Decodo dashboard."
    return "Check network access and Decodo residential proxy dashboard status."


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema_version": "trr.decodo_residential_proxy_smoke.v1",
        "dry_run": args.dry_run,
        "live": args.live,
        "network_sent": False,
        "web_scraping_api_required": False,
        "required_decodo_product": "residential_proxy",
        "custom_scraper_env": {
            "preferred": "DECODO_PROXY_URL",
            "fallback": ["DECODO_USERNAME", "DECODO_PASSWORD", "DECODO_GATEWAY"],
            "not_required": ["SCRAPER_API_TOKEN"],
        },
    }
    try:
        config = load_proxy_config(Path(args.env_file))
    except ValueError as exc:
        report.update({
            "state": "advisory" if args.dry_run else "fail",
            "reason": "decodo_proxy_config_malformed",
            "remediation": str(exc),
        })
        return report

    if config is None:
        report.update({
            "state": "advisory" if args.dry_run else "fail",
            "reason": "decodo_proxy_unconfigured",
            "proxy_configured": False,
            "remediation": "Set DECODO_PROXY_URL or DECODO_USERNAME, DECODO_PASSWORD, and DECODO_GATEWAY for TRR custom scraper residential proxy lanes.",
        })
        return report

    report.update({
        "state": "ok",
        "reason": None,
        "proxy_configured": True,
        "proxy_source": config.source,
        "proxy": {
            "url": redact_proxy_url(config.proxy_url),
            "host": config.host,
            "port": config.port,
            "protocol": config.protocol,
        },
    })
    if not args.live:
        return report

    probe = probe_proxy(
        config,
        target_host=args.target_host,
        target_port=args.target_port,
        timeout_seconds=args.timeout_seconds,
    )
    report["network_sent"] = True
    report["probe"] = probe
    report["state"] = probe["state"]
    report["reason"] = probe.get("reason")
    if probe.get("remediation"):
        report["remediation"] = probe["remediation"]
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRR-only Decodo residential proxy smoke check.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True, help="Validate config shape only; send no network traffic.")
    mode.add_argument("--live", action="store_true", help="Send one CONNECT probe through the Decodo residential proxy.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    parser.add_argument("--env-file", default=str(SOURCE_ENV_PATH), help="Env file to read after shell env and before launchctl.")
    parser.add_argument("--target-host", default="ip.decodo.com", help="CONNECT target host for live smoke.")
    parser.add_argument("--target-port", type=int, default=443, help="CONNECT target port for live smoke.")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="Socket timeout for live smoke.")
    args = parser.parse_args(argv)
    if args.live:
        args.dry_run = False
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps(report, sort_keys=True))
    return 0 if report["state"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
