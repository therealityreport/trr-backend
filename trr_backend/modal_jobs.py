"""Modal app for on-demand TRR backend long-running jobs."""

from __future__ import annotations

import os
import pathlib
import socket
import uuid
from typing import Final

try:
    import modal
except ModuleNotFoundError:  # pragma: no cover - exercised by local/test imports without modal installed
    class _ModalImage:
        @classmethod
        def debian_slim(cls, **_kwargs):
            return cls()

        def pip_install_from_requirements(self, *_args, **_kwargs):
            return self

        def add_local_python_source(self, *_args, **_kwargs):
            return self

        def add_local_file(self, *_args, **_kwargs):
            return self

        def add_local_dir(self, *_args, **_kwargs):
            return self

        def apt_install(self, *_args, **_kwargs):
            return self

        def pip_install(self, *_args, **_kwargs):
            return self

    class _ModalSecret:
        @staticmethod
        def from_name(name: str):
            return {"named": name}

        @staticmethod
        def from_dotenv(path: pathlib.Path):
            return {"dotenv": str(path)}

    class _ModalCron:
        def __init__(self, expression: str, *, timezone: str | None = None):
            self.expression = expression
            self.timezone = timezone

    class _ModalApp:
        def __init__(self, *_args, **_kwargs):
            return

        def function(self, *_args, **_kwargs):
            def _decorator(func):
                return func

            return _decorator

    class _ModalModule:
        Image = _ModalImage
        Secret = _ModalSecret
        Cron = _ModalCron
        App = _ModalApp

        @staticmethod
        def asgi_app(*_args, **_kwargs):
            def _decorator(func):
                return func

            return _decorator

    modal = _ModalModule()

from trr_backend.observability import configure_runtime_observability
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
_APP_NAME = str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip() or "trr-backend-jobs"
_TIMEZONE = str(os.getenv("TRR_MODAL_TIMEZONE") or "America/New_York").strip() or "America/New_York"
_API_FUNCTION_NAME = str(os.getenv("TRR_MODAL_API_FUNCTION") or "serve_backend_api").strip() or "serve_backend_api"
_API_LABEL = str(os.getenv("TRR_MODAL_API_LABEL") or "trr-backend-api").strip() or "trr-backend-api"
_API_MIN_CONTAINERS = max(0, int(os.getenv("TRR_MODAL_API_MIN_CONTAINERS", "1")))
_SOCIAL_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "64")))
_SOCIAL_RECOVERY_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_CONCURRENCY_LIMIT", "4")))
_ADMIN_KEEP_WARM = max(0, int(os.getenv("TRR_MODAL_ADMIN_KEEP_WARM", "1")))
_DEFAULT_RUNTIME_SECRET_NAME = "trr-backend-runtime"
_DEFAULT_SOCIAL_SECRET_NAME = "trr-social-auth"
_LOCAL_RUNTIME_MARKERS: Final[frozenset[str]] = frozenset({"local", "dev", "development", "test"})
_CANONICAL_MODAL_RUNTIME_DEFAULTS: Final[dict[str, str]] = {
    "TRR_JOB_PLANE_MODE": "remote",
    "TRR_LONG_JOB_ENFORCE_REMOTE": "1",
    "TRR_REMOTE_EXECUTOR": "modal",
    "TRR_MODAL_ENABLED": "1",
    "TRR_MODAL_APP_NAME": _APP_NAME,
    "TRR_MODAL_API_FUNCTION": _API_FUNCTION_NAME,
    "TRR_MODAL_API_LABEL": _API_LABEL,
    "TRR_MODAL_ADMIN_OPERATION_FUNCTION": "run_admin_operation_v2",
    "TRR_MODAL_GOOGLE_NEWS_FUNCTION": "run_google_news_sync",
    "TRR_MODAL_REDDIT_REFRESH_FUNCTION": "run_reddit_refresh",
    "TRR_MODAL_SOCIAL_JOB_FUNCTION": "run_social_job",
    "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "sweep_social_dispatch_queue",
    "TRR_MODAL_VISION_FUNCTION": "run_admin_vision",
    "TRR_ADMIN_IMAGE_EXECUTION_BACKEND": "modal",
    "SOCIAL_QUEUE_ENABLED": "true",
}

_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements(str(_BACKEND_ROOT / "requirements.lock.txt"))
    .add_local_python_source("api", "trr_backend")
    .add_local_file(str(_BACKEND_ROOT / "scripts" / "_sync_common.py"), remote_path="/root/scripts/_sync_common.py")
    .add_local_dir(str(_BACKEND_ROOT / "scripts" / "sync"), remote_path="/root/scripts/sync")
    .add_local_file(
        str(_BACKEND_ROOT / "scripts" / "socials" / "__init__.py"),
        remote_path="/root/scripts/socials/__init__.py",
    )
    .add_local_file(
        str(_BACKEND_ROOT / "scripts" / "socials" / "refresh_cookies.py"),
        remote_path="/root/scripts/socials/refresh_cookies.py",
    )
    .add_local_dir(
        str(_BACKEND_ROOT / "scripts" / "socials" / "instagram"),
        remote_path="/root/scripts/socials/instagram",
    )
    .add_local_dir(
        str(_BACKEND_ROOT / "scripts" / "socials" / "tiktok"),
        remote_path="/root/scripts/socials/tiktok",
    )
    .add_local_dir(
        str(_BACKEND_ROOT / "scripts" / "socials" / "twitter"),
        remote_path="/root/scripts/socials/twitter",
    )
    .add_local_dir(
        str(_BACKEND_ROOT / "scripts" / "socials" / "threads"),
        remote_path="/root/scripts/socials/threads",
    )
    .add_local_dir(
        str(_BACKEND_ROOT / "scripts" / "socials" / "facebook"),
        remote_path="/root/scripts/socials/facebook",
    )
    .add_local_file(
        str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "__init__.py"),
        remote_path="/root/scripts/socials/youtube/__init__.py",
    )
    .add_local_file(
        str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "scrape.py"),
        remote_path="/root/scripts/socials/youtube/scrape.py",
    )
)

_vision_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("libgl1", "libglib2.0-0")
    .pip_install_from_requirements(str(_BACKEND_ROOT / "requirements.lock.txt"))
    .pip_install(
        "numpy==1.26.4",
        "opencv-python-headless==4.10.0.84",
        "onnxruntime==1.18.1",
        "insightface==0.7.3",
        "ultralytics==8.3.39",
    )
    .add_local_python_source("api", "trr_backend")
)


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _runtime_secret_name() -> str:
    return str(os.getenv("TRR_MODAL_RUNTIME_SECRET_NAME") or os.getenv("TRR_MODAL_SECRET_NAME") or "").strip()


def _social_secret_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_SECRET_NAME") or "").strip()


def _is_local_or_dev_runtime() -> bool:
    runtime_markers = (
        os.getenv("APP_ENV"),
        os.getenv("ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("TRR_ENV"),
        os.getenv("TRR_ENVIRONMENT"),
        os.getenv("WORKSPACE_ENV"),
    )
    normalized = {str(value or "").strip().lower() for value in runtime_markers if str(value or "").strip()}
    if normalized & _LOCAL_RUNTIME_MARKERS:
        return True
    if _env_flag("TRR_LOCAL_DEV") or _env_flag("TRR_MODAL_ALLOW_DOTENV_FALLBACK"):
        return True
    if os.getenv("PYTEST_CURRENT_TEST"):
        return True
    return False


def _require_named_secrets() -> bool:
    return _env_flag("TRR_MODAL_ENABLED", default=False) and not _is_local_or_dev_runtime()


def _api_custom_domains() -> list[str] | None:
    raw = str(os.getenv("TRR_MODAL_API_CUSTOM_DOMAINS") or "").strip()
    if not raw:
        return None
    domains = [segment.strip() for segment in raw.split(",") if segment.strip()]
    return domains or None


def _resolve_modal_secrets() -> list[modal.Secret]:
    explicit_runtime_secret_name = _runtime_secret_name()
    explicit_social_secret_name = _social_secret_name()

    if explicit_runtime_secret_name and explicit_social_secret_name:
        return [
            modal.Secret.from_name(explicit_runtime_secret_name),
            modal.Secret.from_name(explicit_social_secret_name),
        ]

    if explicit_runtime_secret_name or explicit_social_secret_name:
        missing = []
        if not explicit_runtime_secret_name:
            missing.append("TRR_MODAL_RUNTIME_SECRET_NAME")
        if not explicit_social_secret_name:
            missing.append("TRR_MODAL_SOCIAL_SECRET_NAME")
        raise RuntimeError(
            f"Modal secret configuration is partial. Set both named secrets or neither. Missing: {', '.join(missing)}"
        )

    if _is_local_or_dev_runtime():
        return [modal.Secret.from_dotenv(_BACKEND_ROOT)]

    # Keep production/staging deploys deterministic even when the secret name env vars
    # are not present inside the remote import environment.
    return [
        modal.Secret.from_name(_DEFAULT_RUNTIME_SECRET_NAME),
        modal.Secret.from_name(_DEFAULT_SOCIAL_SECRET_NAME),
    ]


def _inject_modal_runtime_defaults() -> None:
    for key, value in _CANONICAL_MODAL_RUNTIME_DEFAULTS.items():
        os.environ[key] = value
    if (os.getenv("AWS_ACCESS_KEY_ID") or "").strip() and (os.getenv("AWS_SECRET_ACCESS_KEY") or "").strip():
        os.environ.pop("AWS_PROFILE", None)
        os.environ.pop("AWS_DEFAULT_PROFILE", None)


_secrets = _resolve_modal_secrets()
_inject_modal_runtime_defaults()
configure_runtime_observability(service_name="trr-backend-modal-jobs")

app = modal.App(_APP_NAME, image=_image)


@app.function(
    name=_API_FUNCTION_NAME,
    secrets=_secrets,
    timeout=60 * 60,
    min_containers=_API_MIN_CONTAINERS,
)
@modal.asgi_app(label=_API_LABEL, custom_domains=_api_custom_domains())
def serve_backend_api():
    from api.main import app as fastapi_app

    return fastapi_app


def _execute_admin_operation(operation_id: str, operation_type: str) -> dict[str, object]:
    from trr_backend.pipeline.admin_operations import claim_and_execute_operation

    worker_id = f"modal:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    claimed = claim_and_execute_operation(
        operation_id=operation_id,
        worker_id=worker_id,
        operation_types=[operation_type],
    )
    return {
        "operation_id": operation_id,
        "operation_type": operation_type,
        "claimed": claimed,
        "worker_id": worker_id,
    }


@app.function(
    name="run_admin_operation",
    secrets=_secrets,
    retries=0,
    timeout=60 * 60,
    min_containers=_ADMIN_KEEP_WARM,
)
def run_admin_operation(operation_id: str, operation_type: str) -> dict[str, object]:
    return _execute_admin_operation(operation_id, operation_type)


@app.function(
    name="run_admin_operation_v2",
    secrets=_secrets,
    retries=0,
    timeout=60 * 60,
    min_containers=_ADMIN_KEEP_WARM,
)
def run_admin_operation_v2(operation_id: str, operation_type: str) -> dict[str, object]:
    return _execute_admin_operation(operation_id, operation_type)


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=60 * 60,
)
def run_google_news_sync(job_id: str) -> dict[str, object]:
    from api.routers.admin_show_news import claim_and_execute_google_news_sync_job

    worker_id = f"modal:google-news:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    claimed = claim_and_execute_google_news_sync_job(job_id=job_id, worker_id=worker_id)
    return {
        "job_id": job_id,
        "claimed": claimed,
        "worker_id": worker_id,
    }


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
)
def run_reddit_refresh(run_id: str) -> dict[str, object]:
    from trr_backend.repositories.reddit_refresh import execute_refresh_run

    worker_id = f"modal:reddit-refresh:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    result = execute_refresh_run(run_id, worker_id=worker_id)
    return {
        "run_id": run_id,
        "status": str(result.get("status") or ""),
        "worker_id": worker_id,
    }


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_job(job_id: str) -> dict[str, object]:
    from trr_backend.repositories.social_season_analytics import claim_and_process_social_job

    worker_id = f"modal:social:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    result = claim_and_process_social_job(job_id=job_id, worker_id=worker_id)
    return {
        "job_id": job_id,
        "claimed": bool(result.get("claimed")),
        "worker_id": worker_id,
        "job": result.get("job"),
    }


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=15 * 60,
    max_containers=_SOCIAL_RECOVERY_CONCURRENCY_LIMIT,
    schedule=modal.Cron("*/2 * * * *", timezone=_TIMEZONE),
)
def sweep_social_dispatch_queue() -> dict[str, object]:
    from trr_backend.repositories.social_season_analytics import recover_and_dispatch_due_social_jobs

    return recover_and_dispatch_due_social_jobs()


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=10 * 60,
    schedule=modal.Cron("* * * * *", timezone=_TIMEZONE),
)
def heartbeat_remote_executors() -> dict[str, object]:
    from trr_backend.modal_dispatch import _record_dispatcher_heartbeat
    from trr_backend.repositories.social_season_analytics import is_queue_enabled

    metadata = {
        "dispatch_enabled": True,
        "heartbeat_source": "modal_cron",
        "heartbeat_call_id": f"heartbeat:{uuid.uuid4().hex[:8]}",
    }
    _record_dispatcher_heartbeat(
        dispatcher_name="admin",
        status="idle",
        metadata_updates=metadata,
    )
    _record_dispatcher_heartbeat(
        dispatcher_name="google-news",
        status="idle",
        metadata_updates=metadata,
    )
    _record_dispatcher_heartbeat(
        dispatcher_name="reddit",
        status="idle",
        metadata_updates=metadata,
    )
    if is_queue_enabled():
        _record_dispatcher_heartbeat(
            dispatcher_name="social",
            status="idle",
            metadata_updates=metadata,
            supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
        )
    return {"ok": True}


@app.function(
    image=_vision_image,
    secrets=_secrets,
    retries=0,
    timeout=20 * 60,
)
def run_admin_vision(payload: dict[str, object], batch: bool = False) -> dict[str, object]:
    from trr_backend.vision.people_count_engine import (
        VisionEngineError,
        VisionEngineUnavailableError,
        compute_people_count,
        compute_people_count_batch,
    )

    try:
        return compute_people_count_batch(payload) if batch else compute_people_count(payload)
    except VisionEngineUnavailableError as exc:
        return {
            "error": str(exc),
            "retry_after_s": int(exc.retry_after_s),
            "unavailable": True,
        }
    except VisionEngineError as exc:
        return {
            "error": str(exc),
            "unavailable": False,
        }
