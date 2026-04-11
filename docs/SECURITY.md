# Security

This document describes the security measures in place to protect sensitive data.

## Secret Management

### What We Protect

**Never commit to git:**
- `.env` files (all variants: `.env`, `.env.local`, `.env.production`, etc.)
- API keys and tokens (FIRECRAWL_API_KEY, GEMINI_API_KEY, TMDB_BEARER_TOKEN)
- Private keys and certificates (`*.pem`, `*.key`)
- Database credentials (PGPASSWORD, connection strings)
- Backup files that may contain secrets (`worktree-backups*`, `*.patch`)
- Local scrape/session artifacts (for example `data/tiktok_cookies.json`)
- Operational evidence captures under `docs/ai/evidence/`

**Safe to commit:**
- `.env.example` with placeholder values
- Environment variable names (references only)
- Documentation with `REDACTED` or masked values
- A tracked `docs/ai/evidence/README.md` that documents the local-only evidence policy

### Protection Layers

#### 1. `.gitignore` (Prevention)
Excludes sensitive files from being tracked:
```
.env
.env.*
!.env.example
keys/
*.pem
worktree-backups*
```

#### 2. Claude Code Hooks (Interactive Prevention)
`.claude/hooks/before-bash.md` blocks dangerous commands before execution:
- `git add .env` → Blocked (staging secrets)
- `git add keys/` → Blocked (staging credentials)
- `git add **/*.pem` → Blocked (staging private keys)
- `git commit -a` → Warning (review `git status` first)
- `echo $API_KEY` → Blocked (exposing secrets in logs)

#### 3. GitHub Actions (CI Detection)
`.github/workflows/secret-scan.yml` scans all commits with Gitleaks:
- Runs on: Pull requests + pushes to main
- Detects: Hardcoded API keys, tokens, credentials
- Fails build: If secrets found (prevents merge)

#### 4. Gitleaks Configuration
`.gitleaks.toml` defines custom rules and allowlists:
- Custom patterns for FIRECRAWL, GEMINI, TMDB keys
- Allowlist for .env.example placeholders
- Allowlist for environment variable references in code

## Key Rotation

If a secret is accidentally committed:

### 1. Assess Exposure
```bash
# Check if key was pushed to remote
git log --all -S "suspected-key-pattern" --oneline

# Check current remotes
git remote -v
```

### 2. Rotate Immediately
- **FIRECRAWL**: Regenerate at https://firecrawl.dev/account
- **Gemini**: Regenerate at https://aistudio.google.com/app/apikey
- **TMDB**: Regenerate at https://www.themoviedb.org/settings/api

### 3. Clean History (if pushed)
```bash
# Use BFG Repo-Cleaner or git-filter-repo (not git-filter-branch)
# See: https://rtyley.github.io/bfg-repo-cleaner/
```

### 4. Force Push (Last Resort)
```bash
# Only if absolutely necessary and coordinated with team
git push --force-with-lease origin main
```

## Backup Security

Worktree and patch backups may contain secrets:

```bash
# Secure existing backups
chmod 700 ~/worktree-backups-PRIVATE
chmod 600 ~/worktree-backups-PRIVATE/*

# Review before archiving
rg -n "API_KEY|SECRET|TOKEN" ~/worktree-backups-PRIVATE/

# Delete if no longer needed
rm -rf ~/worktree-backups-PRIVATE/
```

## Incident Response

If a secret leak is detected:

1. **Do NOT panic push fixes** - This can make things worse
2. **Rotate the exposed key immediately** (see Key Rotation above)
3. **Verify exposure scope**: Local only vs. pushed to GitHub
4. **If pushed**: Clean history before rotating (rotating first alerts attackers)
5. **Document the incident**: What happened, what was exposed, how fixed

## Best Practices

### Development
- Always use `.env` for local secrets (never commit)
- Use `.env.example` for documentation (placeholder values only)
- Run `git status` before `git commit -a`
- Review diffs before pushing: `git diff origin/main..HEAD`

### Code Reviews
- Check for hardcoded credentials in PR diffs
- Verify `.env` is not in changed files
- Watch for backup files being added (`.patch`, `.tar.gz` with secrets)

### CI/CD
- Use GitHub Secrets for sensitive values in workflows
- Never `echo` secret values in CI logs
- Use masked environment variables: `${{ secrets.API_KEY }}`

## Branch Protection

### Configuration
GitHub branch protection for `main` enforces:
- **Required status checks**: `test`, `gitleaks`
- **Strict branch updates**: PRs must be up-to-date with base branch
- **Admin enforcement**: DISABLED (admins can bypass for emergency fixes)

### Check Behavior

**Always Required (Blocking)**:
- `test` (ci.yml) - Unit tests, schema validation, linting
- `gitleaks` (secret-scan.yml) - Secret scanning (incremental on PRs, full history on push/schedule)

**Conditional (Not Required)**:
- `generate-repo-map` (repo_map.yml) - Runs only when Python files, migrations, or repo config changes
  - Auto-commits documentation updates back to PR when triggered
  - Not blocking because it's a documentation enhancement, not a safety gate

### Managing Protection Rules

**View current settings:**
```bash
gh api repos/therealityreport/trr-backend/branches/main/protection | jq '.required_status_checks'
```

**Update required checks:**
```bash
gh api -X PATCH repos/therealityreport/trr-backend/branches/main/protection/required_status_checks \
  --input - <<'EOF'
{
  "strict": true,
  "contexts": ["test", "gitleaks"]
}
EOF
```

**Admin enforcement (current: disabled):**
```bash
# Enable (stricter - admins must follow rules)
gh api -X PUT repos/therealityreport/trr-backend/branches/main/protection/enforce_admins

# Disable (current - allow emergency bypass)
gh api -X DELETE repos/therealityreport/trr-backend/branches/main/protection/enforce_admins
```

### Troubleshooting

**"Required status check missing":**
- Ensure check name exactly matches job name in workflow YAML
- Only `test` and `gitleaks` are required (they run on all PRs)
- `generate-repo-map` is optional and only runs when relevant paths change

**"PR blocked but all checks passed":**
- `strict: true` requires PR branch to be up-to-date with main
- Solution: `git pull origin main` and push again

**"Check never runs on PR":**
- `test` and `gitleaks` should always run on all PRs
- `generate-repo-map` only runs when Python/migration/repo-config files change (expected behavior)

## References

- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [Gitleaks Documentation](https://github.com/gitleaks/gitleaks)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [GitHub Branch Protection Rules](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)
