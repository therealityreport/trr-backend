"""Import-boundary and registration tests for admin operations."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trr_backend.pipeline.admin_operation_registry import (
    AdminOperationProducer,
    AdminOperationRegistry,
    AdminProvider,
    ShowNewsCapabilities,
)

BACKEND_ROOT = Path(__file__).resolve().parents[2]


def _run_fresh_process(source: str) -> None:
    environment = {**os.environ, "PYTHONPATH": str(BACKEND_ROOT), "TRR_ENVIRONMENT": "test"}
    completed = subprocess.run(
        [sys.executable, "-c", source],
        cwd=BACKEND_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_registry_uses_direct_callables_and_rejects_ambiguous_registration() -> None:
    registry = AdminOperationRegistry()
    producer = MagicMock(return_value=object())
    news = ShowNewsCapabilities(_run_google_news_sync_impl=MagicMock())

    registry.register_producer("test_operation", AdminOperationProducer(producer, accepts_operation_id=True))
    registry.register_provider(AdminProvider.SHOW_NEWS, news)

    assert registry.build_producer("test_operation", {"show_id": "show-1"}, "operation-1") is producer.return_value
    producer.assert_called_once_with(request_payload={"show_id": "show-1"}, operation_id="operation-1")
    assert registry.resolve_provider(AdminProvider.SHOW_NEWS, ShowNewsCapabilities) is news

    with pytest.raises(RuntimeError, match="already registered"):
        registry.register_producer("test_operation", AdminOperationProducer(producer))
    with pytest.raises(RuntimeError, match="already registered"):
        registry.register_provider(AdminProvider.SHOW_NEWS, news)


def test_leaf_imports_do_not_load_peer_routers_and_composition_roots_register_them() -> None:
    _run_fresh_process(
        "import sys\n"
        "import trr_backend.pipeline.admin_operations\n"
        "assert not any(name.startswith('api.routers.admin_') for name in sys.modules)\n"
    )
    _run_fresh_process(
        "import sys\n"
        "import trr_backend.pipeline.show_refresh_orchestrator\n"
        "assert not any(name.startswith('api.routers.admin_') for name in sys.modules)\n"
    )
    _run_fresh_process(
        "import sys\n"
        "import api.routers.admin_show_sync\n"
        "peers = [name for name in sys.modules if name.startswith('api.routers.admin_')]\n"
        "assert peers == ['api.routers.admin_show_sync']\n"
    )
    _run_fresh_process(
        "try:\n"
        "    from api.main import app\n"
        "except AttributeError as error:\n"
        "    assert 'strict_content_type' in str(error)\n"
        "from trr_backend.pipeline.admin_operation_registry import get_show_sync_capabilities\n"
        "assert get_show_sync_capabilities()\n"
    )
    _run_fresh_process(
        "from trr_backend.pipeline.admin_operation_bootstrap import register_admin_operation_providers\n"
        "from trr_backend.pipeline.admin_operation_registry import get_show_sync_capabilities\n"
        "register_admin_operation_providers()\n"
        "assert get_show_sync_capabilities()\n"
    )
