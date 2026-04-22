"""Modal app for on-demand TRR backend long-running jobs."""

from __future__ import annotations

import os
import pathlib
import socket
import uuid
from typing import Final

from trr_backend.observability import configure_runtime_observability
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS


def _build_modal_stub_module():
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

        def run_commands(self, *_args, **_kwargs):
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
                func.local = func
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

    return _ModalModule()


def _modal_module_is_usable(module: object) -> bool:
    return all(hasattr(module, attribute) for attribute in ("Image", "Secret", "Cron", "App", "asgi_app"))


def _load_modal_module():
    try:
        import modal as imported_modal
    except ModuleNotFoundError:  # pragma: no cover - exercised by local/test imports without modal installed
        return _build_modal_stub_module()
    if not _modal_module_is_usable(imported_modal):
        return _build_modal_stub_module()
    return imported_modal


modal = _load_modal_module()

_BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
_APP_NAME = str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip() or "trr-backend-jobs"
_TIMEZONE = str(os.getenv("TRR_MODAL_TIMEZONE") or "America/New_York").strip() or "America/New_York"
_API_FUNCTION_NAME = str(os.getenv("TRR_MODAL_API_FUNCTION") or "serve_backend_api").strip() or "serve_backend_api"
_API_LABEL = str(os.getenv("TRR_MODAL_API_LABEL") or "trr-backend-api").strip() or "trr-backend-api"
_API_MIN_CONTAINERS = max(0, int(os.getenv("TRR_MODAL_API_MIN_CONTAINERS", "1")))
_SOCIAL_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT", "64")))
_SOCIAL_RECOVERY_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_CONCURRENCY_LIMIT", "4")))
_ADMIN_KEEP_WARM = max(0, int(os.getenv("TRR_MODAL_ADMIN_KEEP_WARM", "1")))
_ADMIN_CONCURRENCY_LIMIT = max(1, int(os.getenv("TRR_MODAL_ADMIN_OPERATION_CONCURRENCY_LIMIT", "8")))
_DEFAULT_RUNTIME_SECRET_NAME = "trr-backend-runtime"
_DEFAULT_SOCIAL_SECRET_NAME = "trr-social-auth"
_LOCAL_RUNTIME_MARKERS: Final[frozenset[str]] = frozenset({"local", "dev", "development", "test"})
_SOCIAL_IMAGE_LOCAL_FILES: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "_sync_common.py"), "/root/scripts/_sync_common.py"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "__init__.py"), "/root/scripts/socials/__init__.py"),
    (
        str(_BACKEND_ROOT / "scripts" / "socials" / "refresh_cookies.py"),
        "/root/scripts/socials/refresh_cookies.py",
    ),
    (
        str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "__init__.py"),
        "/root/scripts/socials/youtube/__init__.py",
    ),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "youtube" / "scrape.py"), "/root/scripts/socials/youtube/scrape.py"),
)
_SOCIAL_IMAGE_LOCAL_DIRS: Final[tuple[tuple[str, str], ...]] = (
    (str(_BACKEND_ROOT / "scripts" / "sync"), "/root/scripts/sync"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "instagram"), "/root/scripts/socials/instagram"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "tiktok"), "/root/scripts/socials/tiktok"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "twitter"), "/root/scripts/socials/twitter"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "threads"), "/root/scripts/socials/threads"),
    (str(_BACKEND_ROOT / "scripts" / "socials" / "facebook"), "/root/scripts/socials/facebook"),
)
_SOCIAL_BROWSER_APT_PACKAGES: Final[tuple[str, ...]] = (
    "libnss3",
    "libnspr4",
    "libatk1.0-0",
    "libatk-bridge2.0-0",
    "libcups2",
    "libdrm2",
    "libxkbcommon0",
    "libxcomposite1",
    "libxdamage1",
    "libxfixes3",
    "libxrandr2",
    "libgbm1",
    "libpango-1.0-0",
    "libcairo2",
    "libasound2",
    "libatspi2.0-0",
)
_SOCIAL_BROWSER_SETUP_COMMANDS: Final[tuple[str, ...]] = (
    "playwright install chromium",
    # P0-2/P0-3: The Scrapling comments lane uses StealthyFetcher (Patchright-
    # backed in v0.4+). `scrapling install` resolves whatever browser bits the
    # installed Scrapling version needs and is a no-op when the binaries are
    # already present via `playwright install chromium`. Belt and suspenders
    # so the Modal image stays intact if Scrapling changes its install flow.
    "scrapling install",
)
_SOCIAL_IMAGE_PIP_PACKAGES: Final[tuple[str, ...]] = ("yt-dlp",)
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
    "TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION": "run_social_posts_job",
    "TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION": "run_social_media_job",
    "TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION": "run_social_comments_job",
    "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "sweep_social_dispatch_queue",
    "TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION": "probe_social_remote_auth",
    "TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION": "probe_getty_remote_access",
    "TRR_MODAL_VISION_FUNCTION": "run_admin_vision",
    "TRR_MODAL_SOCIALBLADE_FUNCTION": "run_socialblade_scrape",
    "TRR_ADMIN_IMAGE_EXECUTION_BACKEND": "modal",
    "SOCIAL_QUEUE_ENABLED": "true",
}


def _build_social_image_base(*, include_browser_runtime: bool = False, image_factory: object | None = None):
    factory = image_factory or modal.Image
    image = factory.debian_slim(python_version="3.11")
    if include_browser_runtime:
        image = image.apt_install(*_SOCIAL_BROWSER_APT_PACKAGES)
    image = image.pip_install_from_requirements(str(_BACKEND_ROOT / "requirements.lock.txt"))
    image = image.pip_install(*_SOCIAL_IMAGE_PIP_PACKAGES)
    if include_browser_runtime:
        image = image.run_commands(*_SOCIAL_BROWSER_SETUP_COMMANDS)
    image = image.add_local_python_source("api", "trr_backend")
    for local_path, remote_path in _SOCIAL_IMAGE_LOCAL_FILES:
        image = image.add_local_file(local_path, remote_path=remote_path)
    for local_path, remote_path in _SOCIAL_IMAGE_LOCAL_DIRS:
        image = image.add_local_dir(local_path, remote_path=remote_path)
    return image


_image = _build_social_image_base()

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

_browser_image = _build_social_image_base(include_browser_runtime=True)
_FUNCTION_IMAGE_BINDINGS: Final[dict[str, object]] = {
    "run_social_job": _browser_image,
    "run_social_posts_job": _browser_image,
    "run_social_media_job": _browser_image,
    "run_social_comments_job": _browser_image,
    "run_socialblade_scrape": _browser_image,
    "run_admin_vision": _vision_image,
}


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
    if (os.getenv("OBJECT_STORAGE_ACCESS_KEY_ID") or "").strip() and (
        os.getenv("OBJECT_STORAGE_SECRET_ACCESS_KEY") or ""
    ).strip():
        os.environ.pop("OBJECT_STORAGE_PROFILE", None)


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
    from trr_backend.pipeline.admin_operations import (
        claim_and_execute_operation,
        wait_for_sub_operation_dependencies,
    )

    worker_id = f"modal:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

    # If this is a sub-operation, wait for dependency targets to complete
    deps_satisfied = wait_for_sub_operation_dependencies(operation_id)
    if not deps_satisfied:
        return {
            "operation_id": operation_id,
            "operation_type": operation_type,
            "claimed": False,
            "worker_id": worker_id,
            "reason": "dependency_not_satisfied",
        }

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
    max_containers=_ADMIN_CONCURRENCY_LIMIT,
)
def run_admin_operation(operation_id: str, operation_type: str) -> dict[str, object]:
    return _execute_admin_operation(operation_id, operation_type)


@app.function(
    name="run_admin_operation_v2",
    secrets=_secrets,
    retries=0,
    timeout=60 * 60,
    min_containers=_ADMIN_KEEP_WARM,
    max_containers=_ADMIN_CONCURRENCY_LIMIT,
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


def _reddit_runtime_probe_payload() -> dict[str, object]:
    from trr_backend.repositories.reddit_refresh import REDDIT_USER_AGENT_DEFAULT

    client_id = (os.getenv("REDDIT_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("REDDIT_CLIENT_SECRET") or "").strip()
    user_agent = (os.getenv("REDDIT_USER_AGENT") or "").strip()
    missing_env = []
    if not client_id:
        missing_env.append("REDDIT_CLIENT_ID")
    if not client_secret:
        missing_env.append("REDDIT_CLIENT_SECRET")
    warnings = []
    if not user_agent:
        warnings.append("REDDIT_USER_AGENT")
    healthy = not missing_env
    return {
        "healthy": healthy,
        "reason": "ok" if healthy else "reddit_oauth_missing",
        "missing_env": missing_env,
        "warnings": warnings,
        "supports_oauth": healthy,
        "user_agent_configured": bool(user_agent),
        "uses_default_user_agent": not bool(user_agent),
        "effective_user_agent": user_agent or REDDIT_USER_AGENT_DEFAULT,
    }


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=60,
)
def probe_reddit_refresh_runtime() -> dict[str, object]:
    return _reddit_runtime_probe_payload()


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION") or "probe_social_remote_auth").strip()
    or "probe_social_remote_auth",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_social_remote_auth(platform: str) -> dict[str, object]:
    from trr_backend.socials.control_plane import probe_remote_auth_health

    return probe_remote_auth_health(platform)


@app.function(
    name=str(os.getenv("TRR_MODAL_GETTY_REMOTE_PROBE_FUNCTION") or "probe_getty_remote_access").strip()
    or "probe_getty_remote_access",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def probe_getty_remote_access() -> dict[str, object]:
    from trr_backend.integrations.getty_local_prefetch import probe_getty_remote_access as _probe_getty_remote_access

    return _probe_getty_remote_access()


def _execute_social_job(job_id: str, *, worker_prefix: str) -> dict[str, object]:
    from trr_backend.socials.control_plane import claim_and_process_social_job

    worker_id = f"{worker_prefix}:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    result = claim_and_process_social_job(job_id=job_id, worker_id=worker_id)
    return {
        "job_id": job_id,
        "claimed": bool(result.get("claimed")),
        "worker_id": worker_id,
        "job": result.get("job"),
    }


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION") or "run_social_posts_job").strip()
    or "run_social_posts_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_posts_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_posts_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-posts")


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION") or "run_social_media_job").strip()
    or "run_social_media_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_media_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_media_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-media")


@app.function(
    name=str(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION") or "run_social_comments_job").strip()
    or "run_social_comments_job",
    image=_FUNCTION_IMAGE_BINDINGS["run_social_comments_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_comments_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social-comments")


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_social_job"],
    secrets=_secrets,
    retries=0,
    timeout=2 * 60 * 60,
    max_containers=_SOCIAL_CONCURRENCY_LIMIT,
)
def run_social_job(job_id: str) -> dict[str, object]:
    return _execute_social_job(job_id, worker_prefix="modal:social")


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=15 * 60,
    max_containers=_SOCIAL_RECOVERY_CONCURRENCY_LIMIT,
    schedule=modal.Cron("*/2 * * * *", timezone=_TIMEZONE),
)
def sweep_social_dispatch_queue() -> dict[str, object]:
    from trr_backend.socials.control_plane import recover_and_dispatch_due_social_jobs

    return recover_and_dispatch_due_social_jobs()


@app.function(
    secrets=_secrets,
    retries=0,
    timeout=10 * 60,
    schedule=modal.Cron("* * * * *", timezone=_TIMEZONE),
)
def heartbeat_remote_executors() -> dict[str, object]:
    from trr_backend.modal_dispatch import _record_dispatcher_heartbeat
    from trr_backend.socials.control_plane import get_worker_auth_capabilities, is_queue_enabled

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
        social_metadata = {
            **metadata,
            "auth_capabilities": get_worker_auth_capabilities(),
        }
        _record_dispatcher_heartbeat(
            dispatcher_name="social",
            status="idle",
            metadata_updates=social_metadata,
            supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
        )
    else:
        social_metadata = None
    return {
        "ok": True,
        "social_auth_capabilities": social_metadata.get("auth_capabilities") if social_metadata else None,
    }


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_admin_vision"],
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


@app.function(
    image=_FUNCTION_IMAGE_BINDINGS["run_socialblade_scrape"],
    secrets=_secrets,
    retries=0,
    timeout=5 * 60,
)
def run_socialblade_scrape(
    handle: str,
    person_id: str | None = None,
    source: str = "person_page",
    force: bool = False,
) -> dict[str, object]:
    from trr_backend.socials.socialblade.auth import load_socialblade_cookies_from_sources
    from trr_backend.socials.socialblade.scraper import scrape_socialblade
    from trr_backend.socials.socialblade.service import refresh_and_persist_socialblade

    cookies = load_socialblade_cookies_from_sources()
    if person_id:
        return refresh_and_persist_socialblade(
            person_id=person_id,
            handle=handle,
            scraper=lambda safe_handle: scrape_socialblade(safe_handle, cookies),
            source=source,
            force=force,
        )
    return scrape_socialblade(handle, cookies)
