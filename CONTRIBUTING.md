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

## Security

- Never commit `.env` or anything under `keys/`.
- Never force-push to `main`.

