"""Deploy drift guard for the Instagram comments lane.

The Modal browser image bundles the ENTIRE ``trr_backend`` package via
``add_local_python_source`` (modal_jobs.py), so deploy drift here is not a missing
file — it is an IMPORT-time break (a new module that fails to import, a missing
third-party dependency, a syntax error). Those only surface at container startup
on Modal, which previously cost hours to diagnose. This test imports every
``comments_scrapling`` runtime module and the comments job entrypoints so such a
break is caught locally, before deploy.
"""

from __future__ import annotations

import importlib
import pkgutil

import trr_backend.socials.instagram.comments_scrapling as comments_pkg


def test_all_comments_scrapling_modules_import_cleanly():
    failures: dict[str, str] = {}
    for module_info in pkgutil.iter_modules(comments_pkg.__path__):
        name = f"{comments_pkg.__name__}.{module_info.name}"
        try:
            importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001
            failures[name] = repr(exc)
    assert not failures, f"comments_scrapling modules failed to import: {failures}"


def test_phase3_runtime_modules_are_importable():
    # The Phase 1/3 additions must be importable on the deploy image.
    for name in (
        "trr_backend.socials.instagram.comments_scrapling.async_http_client",
        "trr_backend.socials.instagram.comments_scrapling.proxy_budget",
    ):
        assert importlib.import_module(name) is not None


def test_comments_job_entrypoints_resolve():
    # The entrypoints are @app.function-wrapped (Modal Function objects, not plain
    # callables); the drift guard only needs them to resolve, i.e. modal_jobs
    # imported and registered them without error.
    mj = importlib.import_module("trr_backend.modal_jobs")
    assert mj.run_social_comments_job is not None
    assert mj.run_social_comments_recovery_job is not None
