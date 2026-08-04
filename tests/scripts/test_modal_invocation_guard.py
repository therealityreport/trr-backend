from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from scripts.modal import check_invocations as guard


def test_checked_in_modal_mutation_calls_are_explicitly_allowlisted() -> None:
    violations = guard.find_unapproved_invocations(
        workspace_root=guard.WORKSPACE_ROOT,
        allowlist_path=guard.DEFAULT_ALLOWLIST,
    )

    assert violations == []


def test_new_modal_deploy_call_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "new_deploy.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import subprocess\n"
        "def surprise_deploy():\n"
        "    subprocess.run(['python', '-m', 'modal', 'deploy', '-m', 'trr_backend.modal_jobs'])\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].operation == "deploy"
    assert violations[0].function == "surprise_deploy"


def test_scan_does_not_exclude_paths_for_a_generated_temp_parent(tmp_path: Path) -> None:
    workspace = tmp_path / "generated" / "workspace"
    script = workspace / "TRR-Backend" / "scripts" / "lookup.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import modal\ndef lookup():\n    return modal.Function.from_name('trr-backend-jobs', 'run_social_job')\n",
        encoding="utf-8",
    )

    invocations = guard.scan_invocations(workspace)

    assert [item.approval_key for item in invocations] == [
        ("TRR-Backend/scripts/lookup.py", "lookup", "sdk_function_from_name")
    ]


def test_new_modal_secret_mutation_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "scripts" / "secret.sh"
    script.parent.mkdir(parents=True)
    script.write_text("python -m modal secret create unsafe --force\n", encoding="utf-8")
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].operation == "secret_create"


def test_raw_modal_sdk_function_lookup_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "trr_backend" / "unsafe_lookup.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import modal\ndef lookup():\n    return modal.Function.from_name('trr-backend-jobs', 'run_social_job')\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].operation == "sdk_function_from_name"
    assert violations[0].function == "lookup"


@pytest.mark.parametrize(
    "source",
    [
        "from modal import Function\ndef lookup():\n    return Function.from_name('app', 'function')\n",
        (
            "from modal import Function as ModalFunction\n"
            "def lookup():\n"
            "    return ModalFunction.from_name('app', 'function')\n"
        ),
    ],
)
def test_modal_function_import_lookup_fails_static_guard(tmp_path: Path, source: str) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "trr_backend" / "unsafe_import.py"
    script.parent.mkdir(parents=True)
    script.write_text(source, encoding="utf-8")
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == ["sdk_function_from_name"]


def test_modal_module_alias_lookup_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "trr_backend" / "unsafe_alias.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import modal as m\ndef lookup():\n    return m.Function.from_name('app', 'function')\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == ["sdk_function_from_name"]


def test_unrelated_function_from_name_is_not_a_modal_sdk_lookup(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "trr_backend" / "unrelated.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import unrelated as m\ndef lookup():\n    return m.Function.from_name('app', 'function')\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    assert (
        guard.find_unapproved_invocations(
            workspace_root=workspace,
            allowlist_path=allowlist,
        )
        == []
    )


def test_backend_api_directory_is_scanned_for_modal_sdk_lookups(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "api" / "unsafe_route.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import modal\ndef route():\n    return modal.Function.from_name('app', 'function')\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].path == "TRR-Backend/api/unsafe_route.py"
    assert violations[0].operation == "sdk_function_from_name"


def test_stale_allowlist_row_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    allowlist = workspace / "allowed.json"
    allowlist.write_text(
        json.dumps(
            {
                "version": 1,
                "allowed": [
                    {
                        "path": "TRR-Backend/trr_backend/removed.py",
                        "function": "removed_lookup",
                        "operation": "sdk_function_from_name",
                        "max_occurrences": 1,
                        "reason": "This row is intentionally stale for the test.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].kind == "stale_allowlist"
    assert violations[0].observed_count == 0
    assert violations[0].allowed_count == 1


@pytest.mark.parametrize(
    ("call_count", "allowed_count", "kind"),
    [
        (2, 1, "over_count"),
        (1, 2, "under_count"),
    ],
)
def test_allowlist_count_mismatch_fails_exact_guard(
    tmp_path: Path,
    call_count: int,
    allowed_count: int,
    kind: str,
) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "counted.py"
    script.parent.mkdir(parents=True)
    calls = "\n".join(
        "    command = ['python', '-m', 'modal', 'deploy', '-m', 'trr_backend.modal_jobs']" for _ in range(call_count)
    )
    script.write_text(f"def deploy():\n{calls}\n", encoding="utf-8")
    allowlist = workspace / "allowed.json"
    allowlist.write_text(
        json.dumps(
            {
                "version": 1,
                "allowed": [
                    {
                        "path": "TRR-Backend/scripts/counted.py",
                        "function": "deploy",
                        "operation": "deploy",
                        "max_occurrences": allowed_count,
                        "reason": "Exact-count behavior fixture.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].kind == kind
    assert violations[0].observed_count == call_count
    assert violations[0].allowed_count == allowed_count


def test_same_line_python_invocations_fail_as_over_count(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "same_line.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "def deploy():\n    first = ['modal', 'deploy', '-m', 'one']; second = ['modal', 'deploy', '-m', 'two']\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(
        json.dumps(
            {
                "version": 1,
                "allowed": [
                    {
                        "path": "TRR-Backend/scripts/same_line.py",
                        "function": "deploy",
                        "operation": "deploy",
                        "max_occurrences": 1,
                        "reason": "Same-line over-count fixture.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].kind == "over_count"
    assert violations[0].observed_count == 2
    assert violations[0].allowed_count == 1


def test_same_line_shell_invocations_fail_as_over_count(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "scripts" / "same_line.sh"
    script.parent.mkdir(parents=True)
    script.write_text("modal deploy -m one; modal deploy -m two\n", encoding="utf-8")
    allowlist = workspace / "allowed.json"
    allowlist.write_text(
        json.dumps(
            {
                "version": 1,
                "allowed": [
                    {
                        "path": "scripts/same_line.sh",
                        "function": "<shell>",
                        "operation": "deploy",
                        "max_occurrences": 1,
                        "reason": "Same-line shell over-count fixture.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert len(violations) == 1
    assert violations[0].kind == "over_count"
    assert violations[0].observed_count == 2
    assert violations[0].allowed_count == 1


def test_checked_in_allowlist_exactly_matches_current_modal_invocations() -> None:
    expected = {
        (
            "TRR-Backend/scripts/modal/cleanup_wrong_workspace_deploy.py",
            "cleanup_wrong_workspace_deploy",
            "app_stop",
        ): 1,
        ("TRR-Backend/scripts/modal/deploy_backend.py", "build_deploy_command", "deploy"): 1,
        (
            "TRR-Backend/scripts/modal/diagnose_instagram_comments_remote.py",
            "_invoke_deployed_comments_lane",
            "sdk_function_from_name",
        ): 1,
        ("TRR-Backend/scripts/modal/prepare_named_secrets.py", "_modal_secret_create_command", "secret_create"): 1,
        ("TRR-Backend/scripts/modal/reconcile_modal_runtime.py", "deploy_modal_app", "deploy"): 1,
        ("TRR-Backend/scripts/modal/refresh_instagram_cookies_from_chrome.py", "_deploy_modal", "deploy"): 1,
        (
            "TRR-Backend/scripts/modal/refresh_instagram_cookies_from_chrome.py",
            "_verify_remote_auth",
            "sdk_function_from_name",
        ): 1,
        ("TRR-Backend/scripts/modal/render_cutover_commands.py", "main", "deploy"): 1,
        ("TRR-Backend/scripts/modal/repair_instagram_auth.py", "_deploy_modal_command", "deploy"): 1,
        (
            "TRR-Backend/scripts/modal/verify_instagram_posts_auth.py",
            "verify_instagram_posts_auth",
            "sdk_function_from_name",
        ): 1,
        (
            "TRR-Backend/scripts/modal/verify_instagram_public_history.py",
            "verify_instagram_public_history",
            "sdk_function_from_name",
        ): 1,
        (
            "TRR-Backend/scripts/socials/instagram/one_post_media_mirror.py",
            "_run_modal",
            "sdk_function_from_name",
        ): 1,
        ("TRR-Backend/trr_backend/modal_dispatch.py", "get_trr_modal_function_handle", "sdk_function_from_name"): 1,
        (
            "TRR-Backend/trr_backend/vision/people_count_service.py",
            "_invoke_people_count_modal",
            "sdk_function_from_name",
        ): 1,
    }
    observed = Counter(item.approval_key for item in guard.scan_invocations(guard.WORKSPACE_ROOT))
    allowlist_payload = json.loads(guard.DEFAULT_ALLOWLIST.read_text(encoding="utf-8"))
    allowed = {
        (row["path"], row["function"], row["operation"]): row["max_occurrences"] for row in allowlist_payload["allowed"]
    }

    assert dict(observed) == expected
    assert allowed == expected


@pytest.mark.parametrize(
    ("command", "operation"),
    [
        (["python", "-m", "modal", "run", "-m", "trr_backend.modal_jobs"], "run"),
        (["modal", "app", "stop", "trr-backend-jobs"], "app_stop"),
        (["modal", "app", "rollback", "trr-backend-jobs", "1"], "app_rollback"),
        (["python", "-m", "modal", "profile", "activate", "other"], "profile_activate"),
        (["modal", "secret", "delete", "trr-backend-runtime"], "secret_delete"),
    ],
)
def test_dangerous_modal_cli_operation_fails_static_guard(
    tmp_path: Path,
    command: list[str],
    operation: str,
) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "unsafe_cli.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        f"def mutate():\n    command = {command!r}\n    return command\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == [operation]


def test_modal_command_builder_mutation_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "unsafe_builder.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "def _modal_command(*args):\n"
        "    return ['python', '-m', 'modal', *args]\n"
        "def mutate():\n"
        "    return _modal_command('app', 'stop', 'trr-backend-jobs')\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == ["app_stop"]


def test_read_only_modal_cli_operations_do_not_require_allowlist(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "read_only.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "def inspect():\n"
        "    commands = [\n"
        "        ['python', '-m', 'modal', 'profile', 'list'],\n"
        "        ['modal', 'app', 'list'],\n"
        "        ['modal', 'app', 'history', 'trr-backend-jobs'],\n"
        "        ['modal', 'secret', 'list'],\n"
        "    ]\n"
        "    return commands\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    assert (
        guard.find_unapproved_invocations(
            workspace_root=workspace,
            allowlist_path=allowlist,
        )
        == []
    )


@pytest.mark.parametrize(
    ("line", "operation"),
    [
        ("python -m modal run -m trr_backend.modal_jobs\n", "run"),
        ("modal app stop trr-backend-jobs\n", "app_stop"),
        ("python3.11 -m modal profile activate other\n", "profile_activate"),
    ],
)
def test_dangerous_shell_modal_operation_fails_static_guard(
    tmp_path: Path,
    line: str,
    operation: str,
) -> None:
    workspace = tmp_path
    script = workspace / "scripts" / "unsafe.sh"
    script.parent.mkdir(parents=True)
    script.write_text(line, encoding="utf-8")
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == [operation]


def test_shell_string_modal_subprocess_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "TRR-Backend" / "scripts" / "unsafe_shell_string.py"
    script.parent.mkdir(parents=True)
    script.write_text(
        "import subprocess\n"
        "def deploy():\n"
        "    return subprocess.run('python -m modal deploy -m trr_backend.modal_jobs', shell=True)\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == ["deploy"]


def test_multiline_shell_modal_operation_fails_static_guard(tmp_path: Path) -> None:
    workspace = tmp_path
    script = workspace / "scripts" / "unsafe_multiline.sh"
    script.parent.mkdir(parents=True)
    script.write_text(
        "python -m modal \\\n  app stop trr-backend-jobs\n",
        encoding="utf-8",
    )
    allowlist = workspace / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed": []}), encoding="utf-8")

    violations = guard.find_unapproved_invocations(
        workspace_root=workspace,
        allowlist_path=allowlist,
    )

    assert [item.operation for item in violations] == ["app_stop"]
