# Contributing

## Default Git Workflow

This repo does not prescribe a branching/worktree strategy.

- Default is work on `main`.
- Only create/use a branch or worktree if explicitly asked (or if your collaboration flow requires a PR).
- No branch naming conventions are enforced by this repo.

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run

```bash
./start-api.sh
```

## Validate

```bash
ruff check .
ruff format --check .
pytest
```

## Pre-commit hooks

Hooks run `ruff`, `gitleaks`, and basic hygiene checks on staged files before
each commit. They only touch files you actually change, so the existing
lint/format backlog will not block unrelated commits.

```bash
uv tool install pre-commit   # or: pipx install pre-commit
pre-commit install
# optional: lint the whole tree
pre-commit run --all-files
```

CI gates new code forward-only: the `lint` job runs `ruff` on the Python files
changed in each PR (pinned to ruff 0.14.4), so legacy debt does not need to be
fixed all at once. `pyright`, the full test suite, and Modal lockfile freshness
run as non-blocking signal jobs until their backlogs are burned down.

## Security

- Never commit `.env` or anything under `keys/`.
- Never force-push to `main`.

