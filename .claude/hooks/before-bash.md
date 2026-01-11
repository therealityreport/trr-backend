# Risky Bash Command Blocker Hook

**Trigger:** Before executing any Bash command via Claude Code

**Purpose:** Block dangerous operations that could leak secrets or cause data loss

## Blocked Patterns

### Critical Dangers (ALWAYS BLOCK)
- `rm -rf /` or `rm -rf ~` - Filesystem destruction
- `git push --force origin main` or `git push --force origin master` - Force push to protected branch
- `echo "..." > .env` or `cat > .env` - Overwriting secrets file
- `git add .env` or `git add keys/` - Staging secrets for commit
- `pip install` without venv check (`$VIRTUAL_ENV` unset) - Global package pollution

### AWS Destructive Operations (BLOCK without confirmation)
- `aws s3 rm` - S3 deletion
- `aws rds delete-*` - Database deletion
- `aws ec2 terminate-*` - Instance termination
- Any AWS command with `--force` flag

### Supabase Destructive Operations (WARN, require --dry-run first)
- `supabase db push` without prior `--dry-run` - Schema push without preview
- `supabase projects delete` - Project deletion

## Allowed Operations (Safe List)
- Git read operations: `git status`, `git diff`, `git log`, `git show`
- Test runners: `pytest`, `python -m pytest`
- Linters: `ruff check`, `ruff format`
- Makefile targets: `make schema-docs-check`, `make repo-map-check`
- AWS read operations: `aws s3 ls`, `aws iam list-*`, `aws sts get-caller-identity`
- Worktrunk operations: `wt new`, `wt list`, `wt delete` (has built-in safety)

## Response on Block
When blocking a command:
1. **Explain why** the command is risky
2. **Suggest safer alternative** (if available)
3. **Require explicit user confirmation** to proceed (with understanding of risk)

Example:
```
⛔ BLOCKED: git push --force origin main

Why: Force pushing to main can overwrite team members' work and break production.

Safer alternatives:
- Create a new branch: git checkout -b fix/force-push-alternative
- Use regular push: git push origin main (will fail if out of sync - this is good!)
- If you really need to force push, use --force-with-lease (safer)

Confirm to proceed anyway? (y/N)
```
