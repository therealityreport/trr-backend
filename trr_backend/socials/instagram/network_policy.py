"""Instagram browser network policy for scraper cost control."""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

STATIC_CDN_HOST = "static.cdninstagram.com"

DEFAULT_BLOCKED_DOMAINS: frozenset[str] = frozenset(
    {
        STATIC_CDN_HOST,
        "static.xx.fbcdn.net",
        "static.facebook.com",
        "googletagmanager.com",
        "googleads.g.doubleclick.net",
        "content-autofill.googleapis.com",
    }
)

DEFAULT_BLOCKED_RESOURCE_TYPES: frozenset[str] = frozenset(
    {
        "font",
        "image",
        "media",
        "beacon",
        "object",
        "imageset",
        "texttrack",
        "websocket",
        "csp_report",
        "stylesheet",
    }
)

_SOURCE_MAP_SUFFIXES = (".map", ".js.map", ".css.map")


def _env_truthy(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off", ""}


def _host_matches(host: str, domain: str) -> bool:
    normalized_host = str(host or "").strip().lower().rstrip(".")
    normalized_domain = str(domain or "").strip().lower().rstrip(".")
    return normalized_host == normalized_domain or normalized_host.endswith(f".{normalized_domain}")


def _url_host(url: str) -> str:
    try:
        return str(urlparse(str(url or "")).hostname or "").strip().lower()
    except Exception:  # noqa: BLE001
        return ""


def _url_path(url: str) -> str:
    try:
        return str(urlparse(str(url or "")).path or "").strip().lower()
    except Exception:  # noqa: BLE001
        return ""


@dataclass(slots=True)
class InstagramNetworkDecision:
    action: str
    reason: str | None = None
    host: str = ""
    resource_type: str = ""

    @property
    def blocked(self) -> bool:
        return self.action == "block"


@dataclass(slots=True)
class InstagramNetworkPolicy:
    enabled: bool = True
    block_static_assets: bool = True
    disable_extra_resources: bool = True
    blocked_domains: frozenset[str] = DEFAULT_BLOCKED_DOMAINS
    blocked_resource_types: frozenset[str] = DEFAULT_BLOCKED_RESOURCE_TYPES
    mode: str = "enforcing"

    @classmethod
    def from_env(cls) -> InstagramNetworkPolicy:
        enabled = _env_truthy("INSTAGRAM_BROWSER_NETWORK_POLICY_ENABLED", True)
        block_static_assets = _env_truthy("INSTAGRAM_BROWSER_BLOCK_STATIC_ASSETS", True)
        disable_extra_resources = _env_truthy("INSTAGRAM_BROWSER_DISABLE_EXTRA_RESOURCES", True)
        report_only = _env_truthy("INSTAGRAM_BROWSER_NETWORK_POLICY_REPORT_ONLY", False)
        mode = "disabled" if not enabled else ("report_only" if report_only else "enforcing")
        return cls(
            enabled=enabled,
            block_static_assets=block_static_assets,
            disable_extra_resources=disable_extra_resources,
            mode=mode,
        )

    @property
    def enforcing(self) -> bool:
        return self.enabled and self.mode == "enforcing"

    def decide(self, url: str, resource_type: str | None = None) -> InstagramNetworkDecision:
        host = _url_host(url)
        normalized_type = str(resource_type or "").strip().lower()
        if not self.enabled:
            return InstagramNetworkDecision("allow", host=host, resource_type=normalized_type)
        path = _url_path(url)
        if path.endswith(_SOURCE_MAP_SUFFIXES):
            return InstagramNetworkDecision("block", "source_map", host=host, resource_type=normalized_type)
        if self.block_static_assets and any(_host_matches(host, domain) for domain in self.blocked_domains):
            return InstagramNetworkDecision("block", "blocked_domain", host=host, resource_type=normalized_type)
        if self.disable_extra_resources and normalized_type in self.blocked_resource_types:
            return InstagramNetworkDecision("block", "blocked_resource_type", host=host, resource_type=normalized_type)
        return InstagramNetworkDecision("allow", host=host, resource_type=normalized_type)

    def scrapling_fetch_kwargs(self) -> dict[str, Any]:
        if not self.enabled:
            return {}
        return {
            "disable_resources": self.disable_extra_resources,
            "blocked_domains": set(self.blocked_domains) if self.block_static_assets else set(),
            "block_ads": self.block_static_assets,
        }

    def to_metadata(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "block_static_assets": self.block_static_assets,
            "disable_extra_resources": self.disable_extra_resources,
            "blocked_domains": sorted(self.blocked_domains) if self.block_static_assets else [],
            "blocked_resource_types": sorted(self.blocked_resource_types) if self.disable_extra_resources else [],
        }


@dataclass(slots=True)
class InstagramNetworkPolicyRecorder:
    policy: InstagramNetworkPolicy = field(default_factory=InstagramNetworkPolicy.from_env)
    request_count_by_host: Counter[str] = field(default_factory=Counter)
    blocked_request_count_by_host: Counter[str] = field(default_factory=Counter)
    blocked_reason_counts: Counter[str] = field(default_factory=Counter)

    def record_decision(self, decision: InstagramNetworkDecision) -> None:
        host = decision.host or "unknown"
        self.request_count_by_host[host] += 1
        if decision.blocked:
            self.blocked_request_count_by_host[host] += 1
            self.blocked_reason_counts[decision.reason or "blocked"] += 1

    def to_metadata(self) -> dict[str, Any]:
        metadata = self.policy.to_metadata()
        metadata.update(
            {
                "request_count_by_host": dict(sorted(self.request_count_by_host.items())),
                "blocked_request_count_by_host": dict(sorted(self.blocked_request_count_by_host.items())),
                "blocked_reason_counts": dict(sorted(self.blocked_reason_counts.items())),
                "blocked_bytes_estimate_by_host": dict.fromkeys(sorted(self.blocked_request_count_by_host), 0),
                "static_cdn_blocked_request_count": int(self.blocked_request_count_by_host.get(STATIC_CDN_HOST, 0)),
            }
        )
        return metadata


def default_instagram_network_policy() -> InstagramNetworkPolicy:
    return InstagramNetworkPolicy.from_env()


def default_instagram_network_recorder() -> InstagramNetworkPolicyRecorder:
    return InstagramNetworkPolicyRecorder(policy=default_instagram_network_policy())


def install_sync_context_network_policy(
    context: Any,
    *,
    recorder: InstagramNetworkPolicyRecorder | None = None,
) -> InstagramNetworkPolicyRecorder:
    active_recorder = recorder or default_instagram_network_recorder()
    policy = active_recorder.policy
    if not policy.enabled:
        return active_recorder

    def _handler(route: Any) -> None:
        request = getattr(route, "request", None)
        url = str(getattr(request, "url", "") or "")
        resource_type = str(getattr(request, "resource_type", "") or "")
        decision = policy.decide(url, resource_type)
        active_recorder.record_decision(decision)
        if decision.blocked and policy.enforcing:
            route.abort()
            return
        route.continue_()

    try:
        context.route("**/*", _handler)
    except Exception:  # noqa: BLE001
        pass
    return active_recorder


def instagram_scrapling_network_kwargs(
    *,
    policy: InstagramNetworkPolicy | None = None,
) -> dict[str, Any]:
    return (policy or default_instagram_network_policy()).scrapling_fetch_kwargs()


def merge_network_policy_metadata(
    base: Mapping[str, Any] | None,
    *,
    policy: InstagramNetworkPolicy | None = None,
) -> dict[str, Any]:
    result = dict(base or {})
    result["network_policy"] = (policy or default_instagram_network_policy()).to_metadata()
    return result
