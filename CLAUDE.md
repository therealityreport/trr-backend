# Claude Code - TRR Backend Vibe Coding Guide

## Vibe Coding Rules (Non-Negotiable)
- Never work on main branch; always use feature branch + worktree
- One feature = one worktree. No mixing.
- Spec-first: write acceptance criteria before editing code
- Commit early, push often. No invisible work.
- Before asking for next instructions: must have commit SHA + pushed branch

## Worktrunk Quickstart (Recommended)
```bash
# Create isolated branch + worktree
wt new feature/my-feature
cd ../trr-backend-my-feature

# Work, commit, push...
git add . && git commit -m "feat: description"
git push -u origin feature/my-feature

# Create PR
/trr-pr

# Clean up when done
wt delete feature/my-feature
```

### Fallback: Git worktrees (if wt not available)
```bash
git worktree add ../trr-backend-my-feature feature/my-feature
cd ../trr-backend-my-feature
# Work, then cleanup:
git worktree remove ../trr-backend-my-feature
```

## Essential Commands

### Environment Setup
```bash
pip install -r requirements.txt       # Install deps (in venv!)
cp .env.example .env                  # First-time setup
source .venv/bin/activate             # Activate venv
```

### Testing & Validation (Fast - Pre-Commit)
```bash
ruff check .                          # Linting
ruff format .                         # Auto-format
pytest                                # Unit tests
```

### Testing & Validation (Medium - Pre-PR)
```bash
make schema-docs-check                # If schema changed
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

## Slash Commands (Project-Specific)
- `/trr-spec` - Write specification document
- `/trr-plan` - Design implementation plan
- `/trr-impl` - Execute implementation
- `/trr-validate` - Run validation suite (auto-detects what to run)
- `/trr-pr` - Create pull request
- `/trr-wt-new` - Create new Worktrunk branch

## Safety Rules
- NEVER commit .env or keys/ (use .env.example as template)
- NEVER force push to main
- ALWAYS activate venv before pip install
- ALWAYS run fast checks (ruff + pytest) before committing
- Check git status before destructive operations

## Documentation
- **Workflow Guide:** [docs/workflows/VIBE_CODING.md](docs/workflows/VIBE_CODING.md) (analogies, end-to-end loop)
- **Architecture:** [docs/architecture.md](docs/architecture.md)
- **DB Schema:** [docs/db/schema.md](docs/db/schema.md)
- **Git Workflow:** [docs/Repository/diagrams/git_workflow.md](docs/Repository/diagrams/git_workflow.md)
