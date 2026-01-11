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

**Safe to commit:**
- `.env.example` with placeholder values
- Environment variable names (references only)
- Documentation with `REDACTED` or masked values

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

## References

- [GitHub Secret Scanning](https://docs.github.com/en/code-security/secret-scanning)
- [Gitleaks Documentation](https://github.com/gitleaks/gitleaks)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
