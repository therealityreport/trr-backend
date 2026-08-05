"""Cold-import contract for the account-catalog launch provider leaf."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
MODULE = "trr_backend.socials.pipelines.account_catalog.launch"
LEGACY = "trr_backend.socials.social_season_analytics_impl"
SOURCE = BACKEND_ROOT / "trr_backend" / "socials" / "pipelines" / "account_catalog" / "launch.py"


def _fresh(*lines: str) -> None:
    result = subprocess.run(
        [sys.executable, "-B", "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_launch_cold_import_does_not_load_or_access_the_monolith() -> None:
    _fresh(
        "import importlib, sys",
        f"room = importlib.import_module({MODULE!r})",
        f"assert {LEGACY!r} not in sys.modules",
        "assert room._PROVIDER_STATE == 'UNCONFIGURED'",
        "try:",
        "    room._sync_core_overrides()",
        "except RuntimeError as error:",
        "    assert 'ACCOUNT_CATALOG_LAUNCH_PROVIDER_UNCONFIGURED' in str(error)",
        "else:",
        "    raise AssertionError('cold launch provider did not fail closed')",
    )


def test_launch_tail_publication_preserves_module_identity_and_local_rooms() -> None:
    _fresh(
        "import importlib",
        f"room = importlib.import_module({MODULE!r})",
        f"impl = importlib.import_module({LEGACY!r})",
        "assert room._PROVIDER_NAMESPACE is impl.__dict__",
        "assert room._core is impl",
        "assert set(room._LOCAL_ROOM_FUNCTIONS) == room._LOCAL_ROOM_NAMES",
        "name = 'start_social_account_catalog_backfill'",
        "assert room._CORE_ROOM_WRAPPERS[name] is impl.__dict__[name]",
        "assert room._room_callable(name, room._LOCAL_ROOM_FUNCTIONS[name]) is room._LOCAL_ROOM_FUNCTIONS[name]",
    )


def test_launch_reservation_applies_admission_updates_while_holding_its_lock() -> None:
    _fresh(
        "from contextlib import contextmanager",
        "import importlib, json",
        f"core = importlib.import_module({LEGACY!r})",
        "lock_conn = object()",
        "observed = {}",
        "@contextmanager",
        "def db_connection(**_kwargs):",
        "    yield lock_conn",
        "@contextmanager",
        "def db_cursor(**_kwargs):",
        "    yield object()",
        "def fetch_one_with_cursor(_cur, query, params):",
        "    normalized = ' '.join(query.lower().split())",
        "    if 'pg_try_advisory_lock' in normalized:",
        "        return {'locked': True}",
        "    if 'insert into social.scrape_runs' in normalized:",
        "        observed['persisted_config'] = json.loads(params[5])",
        "        return {'id': 'catalog-run-1'}",
        "    if 'pg_advisory_unlock' in normalized:",
        "        return {'unlocked': True}",
        "    raise AssertionError(normalized)",
        "core.pg.db_connection = db_connection",
        "core.pg.db_cursor = db_cursor",
        "core.pg.fetch_one_with_cursor = fetch_one_with_cursor",
        "core.get_active_social_account_catalog_run = lambda *_args, **_kwargs: None",
        "def admit(conn):",
        "    observed['callback_conn'] = conn",
        "    return {'db_session_capacity': {'available': True}, 'budget_decision': {'mode': 'admitted'}}",
        "result = core._reserve_social_account_catalog_launch(",
        "    platform='instagram', account_handle='bravotv', source_scope='network',",
        "    initiated_by=None, placeholder_config={'launch_state': 'pending'},",
        "    initial_status='queued', admission_callback=admit,",
        ")",
        "assert observed['callback_conn'] is lock_conn",
        "assert observed['persisted_config']['launch_state'] == 'pending'",
        "assert observed['persisted_config']['db_session_capacity'] == {'available': True}",
        "assert result['config_updates']['budget_decision'] == {'mode': 'admitted'}",
    )


def test_launch_source_has_no_legacy_import() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    forbidden = {LEGACY, "trr_backend.repositories.social_season_analytics"}
    assert [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden for alias in node.names))
    ] == []
