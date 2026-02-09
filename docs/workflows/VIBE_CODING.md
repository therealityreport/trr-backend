# Vibe Coding Workflow Guide

This repo does not prescribe a branching/worktree strategy.

## Defaults

- Default is work on `main`.
- Only create/use a branch or worktree if explicitly asked.
- Never force-push to `main`.

## Loop

1. Write acceptance criteria (what "done" means).
2. Make the smallest change that moves the needle.
3. Run fast checks.
4. Commit a coherent slice.
5. Repeat.

## Fast Checks

```bash
ruff check .
ruff format --check .
pytest
```

## Medium Checks (When Applicable)

```bash
make schema-docs-check
make repo-map-check
```

## Running Locally

```bash
source .venv/bin/activate
cp .env.example .env
./start-api.sh
```

## Notes

- If your collaboration flow requires a PR, create a branch (no naming conventions are enforced here).
- If you need parallel workspaces, a git worktree is fine, but optional.

