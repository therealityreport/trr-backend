# /trr-pr - Create Pull Request

Create a well-formed pull request with proper context and validation.

## Pre-Flight Checks
1. Run /trr-validate to ensure all checks pass
2. Review git diff for unintended changes (secrets, debug code)
3. Confirm no .env or keys/ in changeset
4. Verify all tests pass

## PR Creation Process
1. Review all commits since branch diverged from main:
```bash
git log main..HEAD --oneline
git diff main...HEAD --stat
```

2. Generate PR description with:
   - Summary of changes (bullet points)
   - Motivation/context (why this change?)
   - Testing performed (validation results)
   - Breaking changes (if any)
   - Related issues (use "Closes #123" if applicable)

3. Use gh CLI to create PR:
```bash
gh pr create --title "type: brief description" --body "$(cat <<'EOF'
## Summary
- Key change 1
- Key change 2

## Testing
✅ ruff check: PASSED
✅ pytest: PASSED
✅ make schema-docs-check: PASSED (if applicable)

## Notes
- Any caveats or follow-up work needed
EOF
)"
```

## TRR-Backend PR Conventions
- **Title format:** `type: brief description`
  - Types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`
- **Base branch:** main (verify with --base main)
- **Link issues:** "Closes #123" in body if fixing issue
- **Request review:** Tag @reviewers if known

## After PR Creation
- Monitor CI checks (should start automatically)
- Respond to review feedback promptly
- Keep branch updated if main advances (rebase or merge)
