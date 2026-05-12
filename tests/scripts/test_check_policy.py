from __future__ import annotations

import os
import subprocess
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
CHECK_POLICY = WORKSPACE_ROOT / "scripts" / "check-policy.sh"

ROOT_AGENTS = """# TRR WORKSPACE ROUTER

## Cross-Repo Implementation Order
- Backend-first for schema and contract changes.

## Shared Contracts
- docs/workspace/dev-commands.md
- docs/workspace/chrome-devtools.md
- docs/ai/HANDOFF_WORKFLOW.md
- docs/agent-governance/skill_routing.md

## MCP Invocation Matrix
- `chrome-devtools`
- `github`
- `supabase`
- `figma`

## Trust Boundaries
- Treat every untrusted input as untrusted input until verified against repo code or the live contract.
"""

REPO_AGENTS = """# TRR REPO VAULT

## Scope
- Repo-only instructions for this fixture.
- If policy scope is unclear, escalate to `../AGENTS.md`.

## Non-Negotiable Rules
- `AGENTS.md` is the canonical instruction file for this scope.
- Re-read `../AGENTS.md` for workspace policy questions.

## Validation
- Run the repo-local checks touched by the change.
- Re-read `../AGENTS.md` when startup or policy rules are involved.
"""

BRAIN_CLAUDE = """# Brain-local boot doc

This file is intentionally not a pointer shim.
It represents the local brain scope and must stay out of the entrypoint CLAUDE symlink policy contract.
"""


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _symlink(path: Path, target: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(target)


def _seed_workspace_fixture(root: Path, *, invalid_entrypoint_claude: bool) -> Path:
    _write(root / "AGENTS.md", ROOT_AGENTS)
    _write(root / "TRR-Backend" / "AGENTS.md", REPO_AGENTS)
    _write(root / "TRR-APP" / "AGENTS.md", REPO_AGENTS)

    _write(root / ".codex" / "config.toml", "[mcp_servers]\n")
    _write(root / ".codex" / "rules" / "default.rules", "# policy fixture\n")
    _write(root / "docs" / "workspace" / "dev-commands.md", "workspace commands\n")
    _write(root / "docs" / "workspace" / "chrome-devtools.md", "chrome devtools\n")
    _write(root / "docs" / "ai" / "HANDOFF_WORKFLOW.md", "handoff workflow\n")
    _write(root / "docs" / "agent-governance" / "skill_routing.md", "skill routing\n")
    _write(root / "docs" / "agent-governance" / "claude_skill_overlap.md", "claude overlap\n")
    _write(root / "docs" / "agent-governance" / "mcp_inventory.md", "mcp inventory\n")

    _write(root / "TRR Workspace Brain" / "CLAUDE.md", BRAIN_CLAUDE)
    _write(root / "TRR-Backend" / "TRR Backend Brain" / "CLAUDE.md", BRAIN_CLAUDE)
    _write(root / "TRR-APP" / "TRR App Brain" / "CLAUDE.md", BRAIN_CLAUDE)

    if invalid_entrypoint_claude:
        bad = "# not a pointer shim\n\nThis should fail policy validation.\n"
        _write(root / "CLAUDE.md", bad)
        _write(root / "TRR-Backend" / "CLAUDE.md", bad)
        _write(root / "TRR-APP" / "CLAUDE.md", bad)
    else:
        _symlink(root / "CLAUDE.md", root / "AGENTS.md")
        _symlink(
            root / "TRR-Backend" / "CLAUDE.md",
            root / "TRR-Backend" / "AGENTS.md",
        )
        _symlink(
            root / "TRR-APP" / "CLAUDE.md",
            root / "TRR-APP" / "AGENTS.md",
        )

    return root


def _run_check_policy(root: Path) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["CHECK_POLICY_SKIP_EXTERNAL"] = "1"
    return subprocess.run(
        ["bash", str(CHECK_POLICY), "--root", str(root)],
        cwd=WORKSPACE_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


def test_check_policy_fails_when_entrypoint_claude_files_are_not_symlinks(
    tmp_path: Path,
) -> None:
    fixture_root = _seed_workspace_fixture(
        tmp_path / "workspace",
        invalid_entrypoint_claude=True,
    )

    result = _run_check_policy(fixture_root)

    assert result.returncode == 1
    assert "must be a symlink" in result.stderr


def test_check_policy_accepts_valid_entrypoint_symlinks(tmp_path: Path) -> None:
    fixture_root = _seed_workspace_fixture(
        tmp_path / "workspace",
        invalid_entrypoint_claude=False,
    )

    result = _run_check_policy(fixture_root)

    assert result.returncode == 0
    assert "[check-policy] OK" in result.stdout


def test_check_policy_ignores_brain_claude_docs_when_entrypoint_symlinks_are_valid(
    tmp_path: Path,
) -> None:
    fixture_root = _seed_workspace_fixture(
        tmp_path / "workspace",
        invalid_entrypoint_claude=False,
    )

    result = _run_check_policy(fixture_root)

    assert result.returncode == 0
    assert "TRR Workspace Brain/CLAUDE.md" not in result.stderr
    assert "TRR Backend Brain/CLAUDE.md" not in result.stderr
    assert "TRR App Brain/CLAUDE.md" not in result.stderr
