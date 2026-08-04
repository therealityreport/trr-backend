#!/usr/bin/env python3
"""Fail when a direct dangerous Modal invocation is not approved."""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parent
DEFAULT_ALLOWLIST = Path(__file__).with_name("allowed_invocations.json")
SCAN_ROOTS = (
    "scripts",
    "TRR-Backend/api",
    "TRR-Backend/scripts",
    "TRR-Backend/trr_backend",
    "TRR-APP/scripts",
)
EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "tests",
    ".next",
    ".generated",
    ".plan-work",
    ".plan-grader",
    "build",
    "dist",
    "generated",
}
SHELL_MODAL_PREFIX = r"(?:python(?:3(?:[.]\d+)?)?\s+-m\s+)?modal\s+"
SHELL_DEPLOY = re.compile(SHELL_MODAL_PREFIX + r"deploy\b")
SHELL_RUN = re.compile(SHELL_MODAL_PREFIX + r"run\b")
SHELL_APP_MUTATION = re.compile(SHELL_MODAL_PREFIX + r"app\s+(stop|rollback)\b")
SHELL_PROFILE_MUTATION = re.compile(SHELL_MODAL_PREFIX + r"profile\s+(?!list\b|current\b)([A-Za-z0-9_-]+)\b")
SHELL_SECRET_MUTATION = re.compile(SHELL_MODAL_PREFIX + r"secret\s+(?!list\b)([A-Za-z0-9_-]+)\b")
MODAL_COMMAND_BUILDERS = {
    "_modal_cli",
    "_modal_command",
    "_run_modal_json",
}
SHELL_EXECUTION_FUNCTIONS = {
    ("os", "popen"),
    ("os", "system"),
    ("subprocess", "Popen"),
    ("subprocess", "call"),
    ("subprocess", "check_call"),
    ("subprocess", "check_output"),
    ("subprocess", "run"),
}


@dataclass(frozen=True, order=True)
class Invocation:
    path: str
    line: int
    function: str
    operation: str
    column: int = 0

    @property
    def approval_key(self) -> tuple[str, str, str]:
        return (self.path, self.function, self.operation)


@dataclass(frozen=True)
class GuardViolation:
    path: str
    line: int
    function: str
    operation: str
    kind: str
    observed_count: int
    allowed_count: int


def _operation(strings: list[str]) -> str | None:
    try:
        modal_index = strings.index("modal")
    except ValueError:
        return None
    tail = strings[modal_index + 1 :]
    if not tail:
        return None
    if tail[0] == "deploy":
        return "deploy"
    if tail[0] == "run":
        return "run"
    if tail[0] == "app" and len(tail) > 1 and tail[1] in {"stop", "rollback"}:
        return f"app_{tail[1]}"
    if tail[0] == "profile" and len(tail) > 1 and tail[1] not in {"list", "current"}:
        return f"profile_{tail[1]}"
    if tail[0] == "secret" and len(tail) > 1 and tail[1] != "list":
        return f"secret_{tail[1]}"
    return None


def _shell_operations(text: str) -> list[tuple[str, int]]:
    operations = [("deploy", match.start()) for match in SHELL_DEPLOY.finditer(text)]
    operations.extend(("run", match.start()) for match in SHELL_RUN.finditer(text))
    operations.extend((f"app_{match.group(1)}", match.start()) for match in SHELL_APP_MUTATION.finditer(text))
    operations.extend((f"profile_{match.group(1)}", match.start()) for match in SHELL_PROFILE_MUTATION.finditer(text))
    operations.extend((f"secret_{match.group(1)}", match.start()) for match in SHELL_SECRET_MUTATION.finditer(text))
    return operations


class _PythonInvocationVisitor(ast.NodeVisitor):
    def __init__(
        self,
        path: str,
        *,
        modal_module_aliases: set[str],
        modal_function_aliases: set[str],
    ) -> None:
        self.path = path
        self.modal_module_aliases = modal_module_aliases
        self.modal_function_aliases = modal_function_aliases
        self.function_stack: list[str] = []
        self.invocations: list[Invocation] = []

    def _record(self, node: ast.AST, operation: str, *, column: int | None = None) -> None:
        self.invocations.append(
            Invocation(
                path=self.path,
                line=node.lineno,
                function=self.function_stack[-1] if self.function_stack else "<module>",
                operation=operation,
                column=getattr(node, "col_offset", 0) if column is None else column,
            )
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self.visit_FunctionDef(node)

    def visit_Call(self, node: ast.Call) -> None:
        function = node.func
        modal_sdk_lookup = False
        if isinstance(function, ast.Attribute) and function.attr == "from_name":
            owner = function.value
            if (
                isinstance(owner, ast.Attribute)
                and owner.attr == "Function"
                and isinstance(owner.value, ast.Name)
                and owner.value.id in self.modal_module_aliases
            ):
                modal_sdk_lookup = True
            elif isinstance(owner, ast.Name) and owner.id in self.modal_function_aliases:
                modal_sdk_lookup = True
        if modal_sdk_lookup:
            self._record(node, "sdk_function_from_name")
        if (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and (function.value.id, function.attr) in SHELL_EXECUTION_FUNCTIONS
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            for operation, offset in _shell_operations(node.args[0].value):
                self._record(node, operation, column=getattr(node, "col_offset", 0) + offset)
        function_name = ""
        if isinstance(function, ast.Name):
            function_name = function.id
        elif isinstance(function, ast.Attribute):
            function_name = function.attr
        if function_name in MODAL_COMMAND_BUILDERS:
            strings = [
                argument.value
                for argument in node.args
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str)
            ]
            operation = _operation(["modal", *strings])
            if operation:
                self._record(node, operation)
        self.generic_visit(node)

    def visit_List(self, node: ast.List) -> None:
        strings = [
            element.value
            for element in node.elts
            if isinstance(element, ast.Constant) and isinstance(element.value, str)
        ]
        operation = _operation(strings)
        if operation:
            self._record(node, operation)
        self.generic_visit(node)

    def visit_Tuple(self, node: ast.Tuple) -> None:  # noqa: N802
        self.visit_List(node)


def _relative(path: Path, workspace_root: Path) -> str:
    return path.resolve().relative_to(workspace_root.resolve()).as_posix()


def _scan_python(path: Path, workspace_root: Path) -> list[Invocation]:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    modal_module_aliases: set[str] = set()
    modal_function_aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                if imported.name == "modal":
                    modal_module_aliases.add(imported.asname or imported.name)
        elif isinstance(node, ast.ImportFrom) and node.module == "modal":
            for imported in node.names:
                if imported.name == "Function":
                    modal_function_aliases.add(imported.asname or imported.name)
    visitor = _PythonInvocationVisitor(
        _relative(path, workspace_root),
        modal_module_aliases=modal_module_aliases,
        modal_function_aliases=modal_function_aliases,
    )
    visitor.visit(tree)
    return visitor.invocations


def _scan_shell(path: Path, workspace_root: Path) -> list[Invocation]:
    invocations: list[Invocation] = []
    relative = _relative(path, workspace_root)
    continued = ""
    continued_line = 0
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    for line_number, line in enumerate(lines, start=1):
        if not continued:
            continued_line = line_number
        stripped = line.rstrip()
        if stripped.endswith("\\"):
            continued += stripped[:-1] + " "
            continue
        logical_line = continued + line
        continued = ""
        if logical_line.lstrip().startswith("#"):
            continue
        for operation, column in _shell_operations(logical_line):
            invocations.append(Invocation(relative, continued_line, "<shell>", operation, column))
    if continued:
        for operation, column in _shell_operations(continued):
            invocations.append(Invocation(relative, continued_line, "<shell>", operation, column))
    return invocations


def scan_invocations(workspace_root: Path = WORKSPACE_ROOT) -> list[Invocation]:
    candidates: set[Path] = set()
    for relative_root in SCAN_ROOTS:
        root = workspace_root / relative_root
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or any(part in EXCLUDED_PARTS for part in path.parts):
                continue
            if path.suffix in {".py", ".sh", ".bash", ".zsh", ".mk"} or path.name == "Makefile":
                candidates.add(path)
    for makefile in (
        workspace_root / "Makefile",
        workspace_root / "TRR-Backend" / "Makefile",
        workspace_root / "TRR-APP" / "Makefile",
    ):
        if makefile.is_file():
            candidates.add(makefile)

    invocations: list[Invocation] = []
    for path in sorted(candidates):
        if path.suffix == ".py":
            invocations.extend(_scan_python(path, workspace_root))
        else:
            invocations.extend(_scan_shell(path, workspace_root))
    return sorted(set(invocations))


def _load_allowlist(path: Path) -> dict[tuple[str, str, str], int]:
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"unable to load Modal invocation allowlist {path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("version") != 1 or not isinstance(payload.get("allowed"), list):
        raise RuntimeError("Modal invocation allowlist must be a version 1 object with an allowed array")
    allowed: dict[tuple[str, str, str], int] = {}
    for row in payload["allowed"]:
        if not isinstance(row, dict):
            raise RuntimeError("Modal invocation allowlist rows must be objects")
        key = (str(row.get("path") or ""), str(row.get("function") or ""), str(row.get("operation") or ""))
        count = row.get("max_occurrences")
        if not all(key) or not isinstance(count, int) or count < 1 or not str(row.get("reason") or "").strip():
            raise RuntimeError(f"invalid Modal invocation allowlist row: {row!r}")
        if key in allowed:
            raise RuntimeError(f"duplicate Modal invocation allowlist row: {key}")
        allowed[key] = count
    return allowed


def find_unapproved_invocations(
    *,
    workspace_root: Path = WORKSPACE_ROOT,
    allowlist_path: Path = DEFAULT_ALLOWLIST,
) -> list[GuardViolation]:
    invocations = scan_invocations(workspace_root)
    allowed = _load_allowlist(allowlist_path)
    observed = Counter(invocation.approval_key for invocation in invocations)
    first_observation = {invocation.approval_key: invocation for invocation in invocations}
    violations: list[GuardViolation] = []
    for key in sorted(set(observed) | set(allowed)):
        observed_count = observed.get(key, 0)
        allowed_count = allowed.get(key, 0)
        if observed_count == allowed_count and key in allowed:
            continue
        if key not in allowed:
            kind = "unapproved"
        elif observed_count == 0:
            kind = "stale_allowlist"
        elif observed_count > allowed_count:
            kind = "over_count"
        else:
            kind = "under_count"
        observation = first_observation.get(key)
        path, function, operation = key
        violations.append(
            GuardViolation(
                path=path,
                line=observation.line if observation is not None else 0,
                function=function,
                operation=operation,
                kind=kind,
                observed_count=observed_count,
                allowed_count=allowed_count,
            )
        )
    return violations


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", type=Path, default=WORKSPACE_ROOT)
    parser.add_argument("--allowlist", type=Path, default=DEFAULT_ALLOWLIST)
    parser.add_argument("--list", action="store_true", help="List observed mutation invocations without checking.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list:
        print(json.dumps([asdict(item) for item in scan_invocations(args.workspace_root)], indent=2, sort_keys=True))
        return 0
    try:
        violations = find_unapproved_invocations(
            workspace_root=args.workspace_root,
            allowlist_path=args.allowlist,
        )
    except (OSError, SyntaxError, RuntimeError) as exc:
        print(f"modal-invocation-guard: ERROR: {exc}", file=sys.stderr)
        return 1
    if violations:
        print("modal-invocation-guard: ERROR: observed invocations do not exactly match allowlist:", file=sys.stderr)
        for item in violations:
            print(
                f"  {item.path}:{item.line} function={item.function} operation={item.operation} "
                f"kind={item.kind} observed={item.observed_count} allowed={item.allowed_count}",
                file=sys.stderr,
            )
        return 1
    print("modal-invocation-guard: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
