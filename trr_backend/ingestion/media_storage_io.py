"""One owner-bounded S3 operation per private child; no credentials in argv/logs.

The SDK child inherits the activity process group. Its own absolute watchdog and
parent check cover abrupt parent exit; the owner kills and reaps the exact child
on cancellation, ownership loss, or deadline. No SDK work survives an invocation.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any

MAX_BODY_BYTES = 50 * 1024 * 1024
MAX_INPUT_BYTES = 4 * ((MAX_BODY_BYTES + 2) // 3) + 64 * 1024
MAX_OUTPUT_BYTES = 4096
_SAFE_CODES = frozenset(
    {
        "404",
        "NoSuchKey",
        "NotFound",
        "PreconditionFailed",
        "ConditionalRequestConflict",
        "SlowDown",
        "Throttling",
        "RequestTimeout",
        "InternalError",
        "ServiceUnavailable",
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "ExpiredToken",
        "NoSuchBucket",
    }
)


class StorageDeadlineExceededError(TimeoutError):
    """The SDK operation exceeded its owner's absolute source budget."""


class StorageProcessError(RuntimeError):
    """A private SDK operation did not return a bounded valid result."""


class BoundedS3Client:
    """Private constructor configuration crosses only the child's stdin pipe."""

    def __init__(self, *, bucket: str, profile_name: str | None, client_options: dict[str, Any], _entrypoint=None):
        options = dict(client_options)
        config = options.pop("config")
        if (config.retries or {}).get("total_max_attempts") != 1:
            raise ValueError("Storage SDK retries must be disabled")
        self.meta = SimpleNamespace(config=config)
        self._spec = {
            "bucket": bucket,
            "profile_name": profile_name,
            "client_options": options,
            "config": {
                "connect_timeout": config.connect_timeout,
                "read_timeout": config.read_timeout,
                "retries": {"total_max_attempts": 1},
                "signature_version": config.signature_version,
                "s3": dict(config.s3 or {}),
            },
        }
        self._entrypoint = tuple(_entrypoint or (sys.executable, "-m", __name__))
        self._active: subprocess.Popen | None = None

    @staticmethod
    def _reap(process):
        if process.poll() is None:
            process.kill()
        # No descendants own these pipes; SIGKILL closes the sole SDK operation.
        process.communicate()

    def close(self):
        if self._active is not None:
            self._reap(self._active)
            self._active = None

    def call(self, operation: str, *, check, **kwargs):
        from botocore.exceptions import BotoCoreError, ClientError

        if (
            operation not in {"head_object", "put_object", "digest_object"}
            or kwargs.get("Bucket") != self._spec["bucket"]
        ):
            raise StorageProcessError("storage_operation_invalid")
        if self._active is not None:
            raise StorageProcessError("storage_operation_already_active")
        check()
        remaining = check.remaining()
        deadline = time.monotonic() + remaining
        parameters = dict(kwargs)
        if operation == "put_object":
            body = parameters.pop("Body")
            if not isinstance(body, bytes) or len(body) > MAX_BODY_BYTES or parameters.get("IfNoneMatch") != "*":
                raise StorageProcessError("storage_conditional_body_invalid")
            parameters["Body"] = base64.b64encode(body).decode("ascii")
        raw = json.dumps(
            {"spec": self._spec, "operation": operation, "parameters": parameters}, separators=(",", ":")
        ).encode()
        if len(raw) > MAX_INPUT_BYTES:
            raise StorageProcessError("storage_operation_input_limit")
        check()
        if time.monotonic() >= deadline:
            raise StorageDeadlineExceededError("target_deadline")
        process = subprocess.Popen(
            [*self._entrypoint, "--deadline", str(deadline), "--parent-pid", str(os.getpid())],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=False,
        )
        self._active = process
        first = True
        try:
            while True:
                check()
                budget = min(check.remaining(), deadline - time.monotonic())
                if budget <= 0:
                    raise StorageDeadlineExceededError("target_deadline")
                try:
                    output, _ = process.communicate(input=raw if first else None, timeout=min(1.0, budget))
                    break
                except subprocess.TimeoutExpired:
                    first = False
            check()
            if process.returncode == 124 or time.monotonic() >= deadline:
                raise StorageDeadlineExceededError("target_deadline")
            if process.returncode or len(output) > MAX_OUTPUT_BYTES:
                raise StorageProcessError("storage_operation_failed")
            try:
                result = json.loads(output)
            except (TypeError, ValueError):
                raise StorageProcessError("storage_operation_result_invalid") from None
            if not isinstance(result, dict) or type(result.get("ok")) is not bool:
                raise StorageProcessError("storage_operation_result_invalid")
            if result["ok"]:
                return result["result"]
            if result.get("kind") == "client":
                code = result.get("code") if result.get("code") in _SAFE_CODES else "StorageError"
                status = result.get("status")
                status = status if type(status) is int and 100 <= status <= 599 else 503
                raise ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}, operation)
            raise BotoCoreError()
        finally:
            self._reap(process)
            self._active = None


def _build_client(spec):
    import boto3
    from botocore.config import Config

    session = boto3.Session(profile_name=spec["profile_name"]) if spec["profile_name"] else boto3.Session()
    return session.client("s3", **spec["client_options"], config=Config(**spec["config"]))


def _execute(envelope, client_factory):
    spec, operation, parameters = envelope["spec"], envelope["operation"], dict(envelope["parameters"])
    if spec["config"]["retries"] != {"total_max_attempts": 1} or parameters.get("Bucket") != spec["bucket"]:
        raise StorageProcessError("storage_operation_invalid")
    client = client_factory(spec)
    try:
        if operation == "head_object":
            head = client.head_object(**parameters)
            digest = (head.get("Metadata") or {}).get("sha256")
            metadata = (
                {"sha256": digest}
                if isinstance(digest, str) and len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
                else {}
            )
            return {"ContentLength": int(head["ContentLength"]), "Metadata": metadata}
        if operation == "put_object":
            data = base64.b64decode(parameters.pop("Body"), validate=True)
            if len(data) > MAX_BODY_BYTES or parameters.get("IfNoneMatch") != "*":
                raise StorageProcessError("storage_conditional_body_invalid")
            parameters["Body"] = data
            parameters["Metadata"] = {"sha256": hashlib.sha256(data).hexdigest()}
            client.put_object(**parameters)
            return {}
        if operation != "digest_object":
            raise StorageProcessError("storage_operation_invalid")
        body = client.get_object(**parameters)["Body"]
        try:
            digest, size = hashlib.sha256(), 0
            while chunk := body.read(64 * 1024):
                size += len(chunk)
                if size > MAX_BODY_BYTES:
                    raise StorageProcessError("storage_object_size_limit")
                digest.update(chunk)
            return {"sha256": digest.hexdigest(), "bytes": size}
        finally:
            body.close()
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def main(*, client_factory=_build_client):
    import argparse
    import threading

    parser = argparse.ArgumentParser()
    parser.add_argument("--deadline", required=True, type=float)
    parser.add_argument("--parent-pid", required=True, type=int)
    args = parser.parse_args()
    # Establish guards before reading a large pipe or importing the SDK.
    timer = threading.Timer(max(0.0, args.deadline - time.monotonic()), lambda: os._exit(124))
    timer.daemon = True
    timer.start()
    done = threading.Event()

    def parent_guard():
        while not done.is_set():
            if os.getppid() != args.parent_pid:
                os._exit(125)
            done.wait(0.1)

    threading.Thread(target=parent_guard, daemon=True, name="instagram-storage-parent").start()
    from botocore.exceptions import ClientError

    try:
        raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
        if len(raw) > MAX_INPUT_BYTES or time.monotonic() >= args.deadline:
            raise StorageProcessError("storage_operation_input_limit")
        result = {"ok": True, "result": _execute(json.loads(raw), client_factory)}
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        result = {
            "ok": False,
            "kind": "client",
            "code": code if code in _SAFE_CODES else "StorageError",
            "status": status if type(status) is int and 100 <= status <= 599 else 503,
        }
    except Exception:
        result = {"ok": False, "kind": "core"}
    try:
        encoded = json.dumps(result, separators=(",", ":")).encode()
        if len(encoded) > MAX_OUTPUT_BYTES:
            encoded = b'{"ok":false,"kind":"core"}'
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()
    finally:
        done.set()
        timer.cancel()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
