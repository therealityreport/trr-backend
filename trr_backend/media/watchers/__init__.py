"""Configurable show-season media watcher contracts."""

from .service import (
    DownloadedMedia,
    Qualification,
    ReconciliationError,
    StoredObject,
    UnsafeDownloadError,
    WatcherAcquisitionService,
    WatcherFenceLostError,
    WatchRunResult,
    build_revision_r2_key,
    qualify_candidate,
    run_show_season_media_watch,
    secure_download_candidate,
    source_fingerprint,
)

__all__ = [
    "DownloadedMedia",
    "Qualification",
    "ReconciliationError",
    "StoredObject",
    "UnsafeDownloadError",
    "WatcherAcquisitionService",
    "WatcherFenceLostError",
    "WatchRunResult",
    "build_revision_r2_key",
    "qualify_candidate",
    "run_show_season_media_watch",
    "secure_download_candidate",
    "source_fingerprint",
]
