# /trr-validate - Run Validation Suite

Run appropriate validation checks for current changes. Auto-detect what's needed.

## Fast Validation (Pre-Commit) - ~30 seconds
Run before every commit:
```bash
echo "🔍 Running fast validation..."
ruff check .
ruff format --check .
pytest
git status
```

## Medium Validation (Pre-PR) - ~1-2 minutes
Run before opening PR (includes fast validation):
```bash
echo "🔍 Running medium validation..."
ruff check . && ruff format --check . && pytest

# If schema files modified
if git diff --name-only | grep -q "supabase/migrations\|docs/db/"; then
    make schema-docs-check
fi

# If Python structure changed
if git diff --name-only | grep -q "trr_backend/.*\.py\|scripts/.*\.py"; then
    make repo-map-check
fi
```

## Full Validation (CI Simulation) - ~5-10 minutes
ONLY if debugging CI failures (requires Docker):
```bash
make ci-local  # Runs pytest + Supabase validation
```

## What to Run When
| Situation | Validation Level |
|-----------|------------------|
| Modified Python code | Fast |
| Added new module | Fast + repo-map-check |
| Changed DB schema | Fast + schema-docs-check |
| Before opening PR | Medium |
| Debugging CI failure | Full (optional) |

## Output
Report validation results with clear pass/fail status and next steps.
