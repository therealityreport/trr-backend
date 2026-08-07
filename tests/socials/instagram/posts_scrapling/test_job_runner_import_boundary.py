from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "instagram" / "posts_scrapling" / "job_runner.py"
RUNNER_MODULE = "trr_backend.socials.instagram.posts_scrapling.job_runner"
DISPATCH_MODULE = "trr_backend.socials.control_plane.dispatch_runtime"
REPOSITORY_ALIAS_MODULE = "trr_backend.repositories.social_season_analytics"
LEGACY_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"


def _run_fresh_process(script: str) -> None:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join(path for path in (str(BACKEND_ROOT), existing_pythonpath) if path)
    try:
        subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script)],
            cwd=BACKEND_ROOT,
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "")[-4_000:]
        stderr = (exc.stderr or "")[-4_000:]
        raise AssertionError(
            f"fresh child process failed\nstdout:\n{stdout or '<empty>'}\nstderr:\n{stderr or '<empty>'}"
        ) from exc


def test_runner_source_keeps_the_exact_function_scoped_import_boundary_and_signature() -> None:
    source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    runner = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_instagram_posts_scrapling_job"
    )

    boundary_import = runner.body[0]
    assert isinstance(boundary_import, ast.ImportFrom)
    assert boundary_import.level == 0
    assert boundary_import.module == "trr_backend.socials.control_plane.dispatch_runtime"
    assert [(alias.name, alias.asname) for alias in boundary_import.names] == [("legacy", "repo")]

    module_scoped_imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert boundary_import not in module_scoped_imports
    assert "social_season_analytics" not in source
    assert "social_season_analytics_impl" not in source

    arguments = runner.args
    assert arguments.posonlyargs == []
    assert [argument.arg for argument in arguments.args] == ["job"]
    assert arguments.args[0].annotation is not None
    assert ast.unparse(arguments.args[0].annotation) == "dict[str, Any]"
    assert arguments.defaults == []
    assert arguments.vararg is None
    assert [argument.arg for argument in arguments.kwonlyargs] == ["worker_id"]
    assert arguments.kwonlyargs[0].annotation is not None
    assert ast.unparse(arguments.kwonlyargs[0].annotation) == "str | None"
    assert len(arguments.kw_defaults) == 1
    assert isinstance(arguments.kw_defaults[0], ast.Constant)
    assert arguments.kw_defaults[0].value is None
    assert arguments.kwarg is None
    assert runner.returns is not None
    assert ast.unparse(runner.returns) == "dict[str, Any]"


def test_cold_import_and_first_call_load_the_exact_module_proxy_without_partial_state() -> None:
    _run_fresh_process(
        f"""
        import importlib
        import sys

        runner_name = {RUNNER_MODULE!r}
        dispatch_name = {DISPATCH_MODULE!r}
        alias_name = {REPOSITORY_ALIAS_MODULE!r}
        impl_name = {LEGACY_IMPL_MODULE!r}
        deferred_names = (dispatch_name, alias_name, impl_name)

        assert all(name not in sys.modules for name in deferred_names)
        runner = importlib.import_module(runner_name)
        assert all(name not in sys.modules for name in deferred_names)

        job = {{"id": "job-boundary", "run_id": "run-boundary", "config": {{"account": ""}}}}
        try:
            runner.run_instagram_posts_scrapling_job(job)
        except runner.PostsScraplingRuntimeError as exc:
            assert str(exc) == "Instagram posts Scrapling job is missing an account handle."
            assert exc.error_code == "instagram_posts_account_missing"
            assert exc.retryable is False
        else:
            raise AssertionError("missing-account branch did not raise PostsScraplingRuntimeError")

        alias = importlib.import_module(alias_name)
        dispatch = sys.modules[dispatch_name]
        impl = sys.modules[impl_name]
        assert dispatch.legacy is alias is impl
        assert dispatch.legacy.__dict__ is alias.__dict__ is impl.__dict__
        assert all(
            not getattr(getattr(module, "__spec__", None), "_initializing", False)
            for module in (dispatch, alias, impl)
        )
        """
    )


def test_early_late_and_between_call_monkeypatches_share_the_exact_proxy_module() -> None:
    _run_fresh_process(
        f"""
        import importlib
        import sys

        runner_name = {RUNNER_MODULE!r}
        dispatch_name = {DISPATCH_MODULE!r}
        alias_name = {REPOSITORY_ALIAS_MODULE!r}
        impl_name = {LEGACY_IMPL_MODULE!r}

        alias = importlib.import_module(alias_name)
        alias._r334_boundary_probe = "before-runner-import"
        assert dispatch_name in sys.modules
        assert sys.modules[dispatch_name].legacy is alias

        runner = importlib.import_module(runner_name)
        assert sys.modules[dispatch_name].legacy is alias
        assert alias._r334_boundary_probe == "before-runner-import"
        alias._r334_boundary_probe = "after-runner-import"

        job = {{"id": "job-boundary", "run_id": "run-boundary", "config": {{"account": ""}}}}

        def invoke_missing_account() -> None:
            try:
                runner.run_instagram_posts_scrapling_job(job)
            except runner.PostsScraplingRuntimeError as exc:
                assert exc.error_code == "instagram_posts_account_missing"
            else:
                raise AssertionError("missing-account branch did not raise")

        invoke_missing_account()
        dispatch = sys.modules[dispatch_name]
        impl = sys.modules[impl_name]
        assert dispatch.legacy is alias is impl
        assert dispatch.legacy._r334_boundary_probe == "after-runner-import"

        dispatch.legacy._r334_boundary_probe = "after-first-call"
        assert alias._r334_boundary_probe == "after-first-call"
        invoke_missing_account()
        assert dispatch.legacy._r334_boundary_probe == "after-first-call"

        alias._r334_boundary_probe = "between-calls"
        invoke_missing_account()
        assert dispatch.legacy._r334_boundary_probe == "between-calls"
        assert dispatch.legacy.__dict__ is alias.__dict__ is impl.__dict__
        """
    )
