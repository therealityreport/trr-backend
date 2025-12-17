# Git History Purge Guide

## Why This Is Needed

After merging PR #1 (repo hygiene), `.env` and `.venv/` are removed from tracking but still exist in git history. This means:
- The repo remains bloated (`.venv/` was ~800K lines)
- Secrets from `.env` are still retrievable from old commits

## When To Do This

**After PR #1 is merged to main.** This is a destructive operation that rewrites history.

## Prerequisites

1. **Rotate all secrets first** - assume anything in the old `.env` is compromised:
   - TMDB_API_KEY
   - TMDB_BEARER
   - GEMINI_API_KEY / GOOGLE_GEMINI_API_KEY
   - Any database passwords
   - Firebase/Google service account keys

2. **Coordinate with team** - force-push will require all collaborators to re-clone or reset their local repos

3. **Install git-filter-repo**:
   ```bash
   # macOS
   brew install git-filter-repo

   # pip
   pip install git-filter-repo
   ```

## Purge Steps

```bash
# 1. Fresh clone (required for git-filter-repo)
git clone https://github.com/therealityreport/trr-backend-2025.git trr-backend-purge
cd trr-backend-purge

# 2. Verify what will be removed
git log --all --full-history -- .env .venv/ | head -20

# 3. Remove .env and .venv/ from all history
git filter-repo --invert-paths --path .env --path .venv/

# 4. Also remove .DS_Store if present
git filter-repo --invert-paths --path .DS_Store

# 5. Verify removal
git log --all --full-history -- .env .venv/
# Should return nothing

# 6. Check new repo size
du -sh .git

# 7. Force push (DESTRUCTIVE - rewrites all history)
git remote add origin https://github.com/therealityreport/trr-backend-2025.git
git push origin --force --all
git push origin --force --tags
```

## Post-Purge

1. **All collaborators must re-clone** or run:
   ```bash
   git fetch origin
   git reset --hard origin/main
   ```

2. **Update any CI/CD** that caches the repo

3. **GitHub Actions cache** may need clearing

## Alternative: BFG Repo-Cleaner

If git-filter-repo isn't available:
```bash
# Download BFG
wget https://repo1.maven.org/maven2/com/madgag/bfg/1.14.0/bfg-1.14.0.jar

# Remove files
java -jar bfg-1.14.0.jar --delete-files .env
java -jar bfg-1.14.0.jar --delete-folders .venv

# Clean up
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# Force push
git push --force
```

## Verification

After purge, verify secrets are gone:
```bash
# Should return nothing
git log --all -p -- .env | grep -i "api_key\|password\|secret"
```
