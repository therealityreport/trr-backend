# TRR Backend Coding Guide (Codex/Claude)

## Default Git Workflow
- Default is work on `main`; only create/use a branch or worktree if explicitly asked.
- If asked to create a branch or worktree, do it exactly as requested (no extra conventions).
- Never force-push to `main`.

## Essential Commands

### Environment Setup
```bash
python3.11 -m venv .venv              # If needed
pip install -r requirements.txt       # Install deps (in venv!)
cp .env.example .env                  # First-time setup
source .venv/bin/activate             # Activate venv
```

### Run The API (Dev)
```bash
./start-api.sh                        # Uvicorn on :8000 with --reload (override with TRR_BACKEND_PORT)
```

### Testing & Validation (Fast - Pre-Commit)
```bash
ruff check .                          # Linting
ruff format .                         # Auto-format
pytest                                # Unit tests
```

### Testing & Validation (Medium - Pre-PR)
```bash
make schema-docs-check                # If schema changed (requires fresh DB)
make schema-docs-reset-check          # Reset DB + verify schema docs (convenience)
make repo-map-check                   # If structure changed
```

### Testing & Validation (Slow - CI Simulation)
```bash
make ci-local                         # Full CI (Docker/Supabase - rarely needed locally)
```

### Pre-Commit Fast Check
```bash
ruff check . && ruff format --check . && pytest
```

### Database Operations
```bash
supabase start                        # Local Supabase
supabase db reset                     # Reset + migrations
PYTHONPATH=. python scripts/sync_shows_all.py --all --verbose
```

### PostgREST Schema Cache (After Adding/Modifying Functions)
```bash
./scripts/reload_postgrest_schema.sh  # Reload PostgREST schema cache
# Or manually: psql "$SUPABASE_DB_URL" -c "NOTIFY pgrst, 'reload schema';"
```

**When to reload**: After applying migrations that add/modify database functions, PostgREST needs to reload its schema cache or it will return PGRST202 errors ("function not found").

**Hosted Supabase**: Verify `core` schema is exposed in Dashboard → Settings → API → Exposed schemas

## Slash Commands (Project-Specific)
- `/trr-spec` - Write specification document
- `/trr-plan` - Design implementation plan
- `/trr-impl` - Execute implementation
- `/trr-validate` - Run validation suite (auto-detects what to run)
- `/trr-pr` - Create pull request

## Safety Rules
- NEVER commit .env or keys/ (use .env.example as template)
- NEVER force push to main
- ALWAYS activate venv before pip install
- ALWAYS run fast checks (ruff + pytest) before committing
- Check git status before destructive operations
- Branch protection (CI checks like `test` + `gitleaks`) may be enforced on PRs (see docs/SECURITY.md#branch-protection)

## Documentation
- **Workflow Guide:** [docs/workflows/VIBE_CODING.md](docs/workflows/VIBE_CODING.md) (analogies, end-to-end loop)
- **Architecture:** [docs/architecture.md](docs/architecture.md)
- **DB Schema:** [docs/db/schema.md](docs/db/schema.md)
- **Git Workflow:** [docs/Repository/diagrams/git_workflow.md](docs/Repository/diagrams/git_workflow.md)

## Cross-Repo Collaboration (TRR Workspace)

Workspace canonical rules:
- `/Users/thomashulihan/Projects/TRR/AGENTS.md`
- `/Users/thomashulihan/Projects/TRR/CLAUDE.md`

Implementation order:
1. TRR-Backend schema + endpoints
2. screenalytics (if impacted)
3. TRR-APP consumers/UI

Notes:
- This repo owns port `:8000` in workspace mode (`make dev`).
- Service-to-service and admin endpoints must be allowlist-only (`ADMIN_EMAIL_ALLOWLIST`).
- Update `docs/ai/HANDOFF.md` before ending a session if you touched this repo.
