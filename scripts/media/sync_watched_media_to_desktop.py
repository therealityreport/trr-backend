"""Safely mirror immutable watcher revisions into an operator-owned local folder.

The backend manifest is authoritative only for revision metadata.  The local
configuration maps a destination label to a folder; a server response can
never select a local path.  This module intentionally uses only the standard
library so it can run from a LaunchAgent without a second dependency runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import secrets
import stat
import subprocess
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

STATE_FILE_NAME = ".trr-media-watcher-state.json"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_REDIRECTS = 3
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_IDENTIFIER_RE = re.compile(r"[^A-Za-z0-9._-]+")
_MIME_EXTENSION = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "audio/mpeg": ".mp3",
    "audio/mp4": ".m4a",
    "audio/aac": ".aac",
    "audio/wav": ".wav",
}


class SyncError(RuntimeError):
    """A safe sync failure that leaves existing destination files untouched."""


class OfflineSyncError(SyncError):
    """Network failure; a later invocation can safely retry it."""


class _NoRedirect(HTTPRedirectHandler):
    """Expose redirects to the caller so each hop can be policy checked."""

    def redirect_request(self, req: Request, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None

    def http_error_301(self, req: Request, fp: Any, code: int, msg: str, headers: Any) -> Any:
        return fp

    http_error_302 = http_error_303 = http_error_307 = http_error_308 = http_error_301


@dataclass(frozen=True)
class SyncConfig:
    api_base_url: str
    allowed_api_hosts: frozenset[str]
    allowed_download_hosts: frozenset[str]
    destinations: Mapping[str, Path]
    max_download_bytes: int
    allowed_content_types: frozenset[str]
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    keychain_service: str | None = None
    keychain_account: str | None = None


@dataclass(frozen=True)
class ManifestRevision:
    revision_id: str
    media_asset_id: str
    sha256: str
    size_bytes: int
    content_type: str
    hosted_url: str


def _normalise_host(host: str) -> str:
    return host.strip().lower().rstrip(".")


def _https_url(url: str, allowed_hosts: frozenset[str], *, purpose: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise SyncError(f"{purpose} URL must be an absolute HTTPS URL without user credentials")
    try:
        ipaddress.ip_address(parsed.hostname)
    except ValueError:
        pass
    else:
        raise SyncError(f"{purpose} URL must use an allowlisted DNS host, not an IP address")
    if _normalise_host(parsed.hostname) not in allowed_hosts:
        raise SyncError(f"{purpose} URL host is not allowlisted: {parsed.hostname}")
    return url


def _required_str(raw: Mapping[str, Any], name: str) -> str:
    value = raw.get(name)
    if not isinstance(value, str) or not value.strip():
        raise SyncError(f"configuration requires non-empty {name}")
    return value.strip()


def _host_set(raw: Any, name: str) -> frozenset[str]:
    if not isinstance(raw, list) or not raw:
        raise SyncError(f"configuration requires a non-empty {name} list")
    values = frozenset(_normalise_host(value) for value in raw if isinstance(value, str) and value.strip())
    if not values or any("/" in value or ":" in value for value in values):
        raise SyncError(f"configuration {name} must contain bare host names")
    return values


def load_config(path: Path) -> SyncConfig:
    """Load only explicit local policy.  Config contains no token material."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SyncError(f"could not read local sync configuration: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise SyncError("sync configuration must be a JSON object")

    destinations_raw = raw.get("destinations")
    if not isinstance(destinations_raw, Mapping) or not destinations_raw:
        raise SyncError("configuration requires a non-empty destinations object")
    destinations: dict[str, Path] = {}
    for label, destination in destinations_raw.items():
        if not isinstance(label, str) or not label or not isinstance(destination, str) or not destination:
            raise SyncError("destination labels and paths must be non-empty strings")
        destinations[label] = Path(destination).expanduser()

    content_types_raw = raw.get("allowed_content_types")
    if not isinstance(content_types_raw, list) or not content_types_raw:
        raise SyncError("configuration requires allowed_content_types")
    content_types = frozenset(
        value.split(";", 1)[0].strip().lower()
        for value in content_types_raw
        if isinstance(value, str) and value.strip()
    )
    if not content_types:
        raise SyncError("configuration allowed_content_types must contain MIME types")
    try:
        maximum = int(raw["max_download_bytes"])
        timeout = int(raw.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    except (KeyError, TypeError, ValueError) as exc:
        raise SyncError("configuration requires integer max_download_bytes and optional timeout_seconds") from exc
    if maximum <= 0 or timeout <= 0 or timeout > 300:
        raise SyncError("max_download_bytes and timeout_seconds are outside safe bounds")

    api_base_url = _required_str(raw, "api_base_url").rstrip("/")
    api_hosts = _host_set(raw.get("allowed_api_hosts"), "allowed_api_hosts")
    download_hosts = _host_set(raw.get("allowed_download_hosts"), "allowed_download_hosts")
    _https_url(api_base_url, api_hosts, purpose="API")
    keychain_service = raw.get("keychain_service")
    keychain_account = raw.get("keychain_account")
    if keychain_service is not None and not isinstance(keychain_service, str):
        raise SyncError("keychain_service must be a string when configured")
    if keychain_account is not None and not isinstance(keychain_account, str):
        raise SyncError("keychain_account must be a string when configured")
    return SyncConfig(
        api_base_url=api_base_url,
        allowed_api_hosts=api_hosts,
        allowed_download_hosts=download_hosts,
        destinations=destinations,
        max_download_bytes=maximum,
        allowed_content_types=content_types,
        timeout_seconds=timeout,
        keychain_service=keychain_service or None,
        keychain_account=keychain_account or None,
    )


def _keychain_token(config: SyncConfig) -> str | None:
    if not config.keychain_service:
        return None
    command = ["/usr/bin/security", "find-generic-password", "-s", config.keychain_service, "-w"]
    if config.keychain_account:
        command.extend(["-a", config.keychain_account])
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    token = result.stdout.strip()
    return token or None


def resolve_auth_token(config: SyncConfig, environment: Mapping[str, str] | None = None) -> str:
    environment = os.environ if environment is None else environment
    for name in ("TRR_MEDIA_WATCHER_TOKEN", "TRR_ADMIN_BEARER_TOKEN"):
        token = str(environment.get(name) or "").strip()
        if token:
            return token
    token = _keychain_token(config)
    if token:
        return token
    raise SyncError("no admin token found in environment or configured macOS Keychain item")


def _lstat_regular(path: Path, *, allow_missing: bool = False) -> os.stat_result | None:
    try:
        details = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return None
        raise SyncError(f"required local path is missing: {path}") from None
    if stat.S_ISLNK(details.st_mode):
        raise SyncError(f"symlink is not permitted in sync path: {path}")
    if not stat.S_ISREG(details.st_mode):
        raise SyncError(f"local path is not a regular file: {path}")
    return details


def resolve_destination(config: SyncConfig, label: str) -> Path:
    """Resolve an exact config label to an existing, non-symlink directory."""
    if label not in config.destinations:
        raise SyncError(f"destination label is not allowlisted: {label}")
    root = config.destinations[label]
    try:
        details = root.lstat()
    except FileNotFoundError as exc:
        raise SyncError(f"configured destination does not exist: {root}") from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        raise SyncError(f"configured destination must be a non-symlink directory: {root}")
    # Store and use the canonical root for every later containment check. A
    # system-level alias such as /var -> /private/var is harmless once the
    # configured root itself is verified as a real directory.
    return root.resolve(strict=True)


def _safe_identifier(value: str, name: str) -> str:
    cleaned = _SAFE_IDENTIFIER_RE.sub("-", value.strip()).strip(".-")
    if not cleaned:
        raise SyncError(f"manifest {name} cannot be made into a safe local identifier")
    return cleaned[:120]


def _extension(content_type: str) -> str:
    return _MIME_EXTENSION.get(content_type, ".bin")


def _filename(revision: ManifestRevision, collision: int = 0) -> str:
    stem = "asset-{}-revision-{}-{}".format(
        _safe_identifier(revision.media_asset_id, "media_asset_id"),
        _safe_identifier(revision.revision_id, "revision_id"),
        revision.sha256[:16],
    )
    if collision:
        stem += f"-collision-{collision}"
    return stem + _extension(revision.content_type)


def _destination_path(root: Path, filename: str) -> Path:
    target = root / filename
    if target.parent != root or target.parent.resolve(strict=True) != root:
        raise SyncError("destination path escaped configured root")
    return target


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _choose_target(root: Path, revision: ManifestRevision) -> tuple[Path, bool]:
    for collision in range(10_000):
        target = _destination_path(root, _filename(revision, collision))
        details = _lstat_regular(target, allow_missing=True)
        if details is None:
            return target, False
        if _hash_file(target) == revision.sha256:
            return target, True
    raise SyncError("could not find a collision-safe local filename")


def _parse_revision(raw: Any, config: SyncConfig) -> ManifestRevision:
    if not isinstance(raw, Mapping):
        raise SyncError("manifest revision must be an object")
    revision_id = _required_str(raw, "revision_id")
    media_asset_id = _required_str(raw, "media_asset_id")
    sha256 = _required_str(raw, "sha256").lower()
    if not _SHA256_RE.fullmatch(sha256):
        raise SyncError("manifest revision has invalid SHA-256")
    try:
        size_bytes = int(raw["size_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SyncError("manifest revision has invalid size_bytes") from exc
    if size_bytes < 0 or size_bytes > config.max_download_bytes:
        raise SyncError("manifest revision exceeds local download size limit")
    content_type = _required_str(raw, "content_type").split(";", 1)[0].lower()
    if content_type not in config.allowed_content_types:
        raise SyncError(f"manifest MIME type is not allowlisted: {content_type}")
    hosted_url = _https_url(_required_str(raw, "hosted_url"), config.allowed_download_hosts, purpose="download")
    return ManifestRevision(revision_id, media_asset_id, sha256, size_bytes, content_type, hosted_url)


def _response_status(response: Any) -> int:
    return int(getattr(response, "status", None) or response.getcode())


def _response_header(response: Any, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    value = headers.get(name) if headers is not None else None
    return str(value) if value is not None else None


def _open_checked(
    url: str,
    *,
    headers: Mapping[str, str],
    allowed_hosts: frozenset[str],
    purpose: str,
    timeout_seconds: int,
    opener: Any,
) -> Any:
    current = _https_url(url, allowed_hosts, purpose=purpose)
    for _ in range(MAX_REDIRECTS + 1):
        request = Request(current, headers=dict(headers), method="GET")
        try:
            response = opener.open(request, timeout=timeout_seconds)
        except HTTPError as exc:
            response = exc
        except (URLError, TimeoutError, OSError) as exc:
            raise OfflineSyncError(f"{purpose} request failed: {exc}") from exc
        status_code = _response_status(response)
        if 300 <= status_code < 400:
            location = _response_header(response, "Location")
            close = getattr(response, "close", None)
            if callable(close):
                close()
            if not location:
                raise SyncError(f"{purpose} redirect did not include Location")
            current = _https_url(urljoin(current, location), allowed_hosts, purpose=purpose)
            continue
        if not 200 <= status_code < 300:
            close = getattr(response, "close", None)
            if callable(close):
                close()
            raise SyncError(f"{purpose} request returned HTTP {status_code}")
        return response
    raise SyncError(f"{purpose} exceeded redirect limit")


def _read_limited(handle: BinaryIO, maximum: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = handle.read(min(1024 * 1024, maximum - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > maximum:
            raise SyncError("response exceeds configured size limit")
        chunks.append(chunk)
    return b"".join(chunks)


def fetch_manifest(
    config: SyncConfig, watch_id: str, token: str, *, opener: Any | None = None
) -> list[ManifestRevision]:
    try:
        normalized_watch_id = str(UUID(watch_id.strip()))
    except (AttributeError, ValueError) as exc:
        raise SyncError("watch_id must be a UUID") from exc
    opener = opener or build_opener(_NoRedirect())
    url = f"{config.api_base_url}/api/v1/admin/media-watchers/{normalized_watch_id}/manifest"
    response = _open_checked(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "trr-media-watcher-sync/1",
        },
        allowed_hosts=config.allowed_api_hosts,
        purpose="manifest",
        timeout_seconds=config.timeout_seconds,
        opener=opener,
    )
    try:
        content_type = (_response_header(response, "Content-Type") or "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise SyncError("manifest response did not declare application/json")
        payload = json.loads(_read_limited(response, 4 * 1024 * 1024).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SyncError("manifest response was not valid JSON") from exc
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()
    if not isinstance(payload, Mapping) or not isinstance(payload.get("revisions"), list):
        raise SyncError("manifest did not match requested watch contract")
    try:
        manifest_watch_id = str(UUID(str(payload.get("watch_id") or "")))
    except ValueError as exc:
        raise SyncError("manifest did not match requested watch contract") from exc
    if manifest_watch_id != normalized_watch_id:
        raise SyncError("manifest did not match requested watch contract")
    expires_at = payload.get("expires_at")
    if not isinstance(expires_at, str):
        raise SyncError("manifest did not include a valid expiry")
    try:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SyncError("manifest did not include a valid expiry") from exc
    if expiry.tzinfo is None or expiry.astimezone(UTC) <= datetime.now(UTC):
        raise SyncError("manifest has expired")
    return [_parse_revision(item, config) for item in payload["revisions"]]


def _write_download(response: BinaryIO, partial: Path, revision: ManifestRevision, config: SyncConfig) -> int:
    declared_length = _response_header(response, "Content-Length")
    if declared_length is not None:
        try:
            if int(declared_length) != revision.size_bytes or int(declared_length) > config.max_download_bytes:
                raise SyncError("download Content-Length disagrees with manifest or local limit")
        except ValueError as exc:
            raise SyncError("download supplied invalid Content-Length") from exc
    response_type = (_response_header(response, "Content-Type") or "").split(";", 1)[0].strip().lower()
    if response_type != revision.content_type or response_type not in config.allowed_content_types:
        raise SyncError("download MIME type does not match allowlisted manifest MIME type")

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(partial, flags, 0o600)
    partial_inode = os.fstat(fd).st_ino
    digest = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(fd, "wb", closefd=True) as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > config.max_download_bytes or total > revision.size_bytes:
                    raise SyncError("download exceeds manifest or local size limit")
                output.write(chunk)
                digest.update(chunk)
            output.flush()
            os.fsync(output.fileno())
        if total != revision.size_bytes:
            raise SyncError("download size does not match manifest")
        if digest.hexdigest() != revision.sha256:
            raise SyncError("download SHA-256 does not match manifest")
        return partial_inode
    except BaseException:
        _remove_own_partial(partial, expected_inode=partial_inode)
        raise


def _remove_own_partial(path: Path, *, expected_inode: int) -> None:
    """Remove only an unfinished regular file created under our random name."""
    try:
        details = path.lstat()
    except FileNotFoundError:
        return
    if details.st_ino == expected_inode and stat.S_ISREG(details.st_mode) and not stat.S_ISLNK(details.st_mode):
        path.unlink()


def _fsync_directory(root: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(root, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_state(root: Path) -> dict[str, dict[str, str]]:
    state_path = _destination_path(root, STATE_FILE_NAME)
    if _lstat_regular(state_path, allow_missing=True) is None:
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SyncError(f"local sync state is invalid: {exc}") from exc
    revisions = payload.get("revisions") if isinstance(payload, Mapping) else None
    if not isinstance(revisions, Mapping):
        raise SyncError("local sync state has no revisions object")
    result: dict[str, dict[str, str]] = {}
    for revision_id, entry in revisions.items():
        if isinstance(revision_id, str) and isinstance(entry, Mapping):
            sha256 = entry.get("sha256")
            filename = entry.get("filename")
            if isinstance(sha256, str) and isinstance(filename, str):
                result[revision_id] = {"sha256": sha256, "filename": filename}
    return result


def _save_state(root: Path, revisions: Mapping[str, Mapping[str, str]]) -> None:
    state_path = _destination_path(root, STATE_FILE_NAME)
    _lstat_regular(state_path, allow_missing=True)
    partial = _destination_path(root, f"{STATE_FILE_NAME}.{secrets.token_hex(12)}.partial")
    payload = json.dumps({"version": 1, "revisions": revisions}, sort_keys=True, indent=2).encode("utf-8") + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(partial, flags, 0o600)
    partial_inode = os.fstat(fd).st_ino
    try:
        with os.fdopen(fd, "wb", closefd=True) as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        details = _lstat_regular(partial)
        if details is None or details.st_ino != partial_inode:
            raise SyncError("local state temporary file changed before atomic rename")
        os.replace(partial, state_path)
        _fsync_directory(root)
    except BaseException:
        _remove_own_partial(partial, expected_inode=partial_inode)
        raise


def _download_one(root: Path, revision: ManifestRevision, config: SyncConfig, opener: Any) -> Path:
    target, already_present = _choose_target(root, revision)
    if already_present:
        return target
    partial = _destination_path(root, f".{target.name}.{secrets.token_hex(12)}.partial")
    response = _open_checked(
        revision.hosted_url,
        headers={"Accept": revision.content_type, "User-Agent": "trr-media-watcher-sync/1"},
        allowed_hosts=config.allowed_download_hosts,
        purpose="download",
        timeout_seconds=config.timeout_seconds,
        opener=opener,
    )
    try:
        partial_inode = _write_download(response, partial, revision, config)
        details = _lstat_regular(partial)
        if details is None or details.st_ino != partial_inode:
            raise SyncError("download temporary file changed before atomic rename")
        # Do not overwrite a file created between our initial collision check and rename.
        if _lstat_regular(target, allow_missing=True) is not None:
            _remove_own_partial(partial, expected_inode=partial_inode)
            return _download_one(root, revision, config, opener)
        os.replace(partial, target)
        _fsync_directory(root)
        return target
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()


def sync_revisions(
    revisions: Iterable[ManifestRevision], config: SyncConfig, destination_label: str, *, opener: Any | None = None
) -> dict[str, int]:
    """Mirror revisions without deleting or modifying pre-existing user files."""
    root = resolve_destination(config, destination_label)
    opener = opener or build_opener(_NoRedirect())
    state = _load_state(root)
    downloaded = skipped = 0
    for revision in revisions:
        prior = state.get(revision.revision_id)
        if prior and prior.get("sha256") == revision.sha256:
            candidate = _destination_path(root, prior.get("filename", ""))
            if _lstat_regular(candidate, allow_missing=True) is not None and _hash_file(candidate) == revision.sha256:
                skipped += 1
                continue
        target = _download_one(root, revision, config, opener)
        state[revision.revision_id] = {"sha256": revision.sha256, "filename": target.name}
        _save_state(root, state)
        downloaded += 1
    return {"downloaded": downloaded, "skipped": skipped}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch-id", required=True, help="UUID of the authorized media watch")
    parser.add_argument("--destination-label", required=True, help="Exact label from local sync config")
    parser.add_argument(
        "--config", default=os.environ.get("TRR_MEDIA_WATCHER_CONFIG"), help="Path to local JSON policy config"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.config:
        print("sync failed: --config or TRR_MEDIA_WATCHER_CONFIG is required", file=sys.stderr)
        return 2
    try:
        config = load_config(Path(args.config))
        revisions = fetch_manifest(config, args.watch_id, resolve_auth_token(config))
        result = sync_revisions(revisions, config, args.destination_label)
    except SyncError as exc:
        print(f"sync failed: {exc}", file=sys.stderr)
        return 1
    print(f"sync complete: downloaded={result['downloaded']} skipped={result['skipped']}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI boundary
    raise SystemExit(main())
