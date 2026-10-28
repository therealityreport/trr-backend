"""The API shares bounded Modal input capacity with long-lived status streams."""

import importlib
import sys
import types

from trr_backend import modal_jobs


def test_api_concurrency_is_bounded_without_changing_worker_or_container_limits(monkeypatch):
    original = sys.modules.get("modal")
    monkeypatch.setitem(sys.modules, "modal", types.ModuleType("modal"))
    try:
        module = importlib.reload(modal_jobs)
        assert module.serve_backend_api._modal_concurrency_options == {"max_inputs": 16, "target_inputs": 8}
        assert module.serve_backend_api._modal_function_options["max_containers"] == module._API_MAX_CONTAINERS
        assert module.serve_backend_api._modal_function_options["timeout"] == 3600
        assert not hasattr(module.run_social_comments_job, "_modal_concurrency_options")
    finally:
        if original is None:
            monkeypatch.delitem(sys.modules, "modal", raising=False)
        else:
            monkeypatch.setitem(sys.modules, "modal", original)
        importlib.reload(modal_jobs)
