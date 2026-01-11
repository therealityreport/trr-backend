# /trr-wt-new - Create Worktrunk Branch

Create a new isolated branch using Worktrunk for safe parallel development.

## Process
1. **Ask user for branch name** (or suggest based on task description)
   - Suggest format: `feature/`, `fix/`, `docs/`, `chore/`
   - Example: `feature/add-user-ratings`, `fix/auth-token-expiry`

2. **Validate branch name follows conventions:**
   - feature/* for new features
   - fix/* for bug fixes
   - docs/* for documentation only
   - chore/* for maintenance/tooling

3. **Create Worktrunk worktree:**
```bash
wt new <branch-name>
```

4. **Navigate to new worktree** (Worktrunk auto-creates directory):
```bash
cd ../trr-backend-<branch-name-suffix>
```

5. **Verify environment setup** (post-create hook should handle this):
```bash
ls -la .env       # Should exist (copied by hook)
git status        # Should be clean
source .venv/bin/activate  # Activate venv
```

## Fallback: Git Worktree (if wt command fails)
```bash
git worktree add ../trr-backend-<branch-name> <branch-name>
cd ../trr-backend-<branch-name>

# Manual setup (Worktrunk does this automatically):
cp ../.env .env || cp .env.example .env
source .venv/bin/activate
```

## After Branch Creation
- **Print workspace location:** Show user where they are now
- **Remind of next steps:**
  1. Verify venv activated
  2. Run pytest to confirm environment works
  3. Start implementation with /trr-spec or /trr-impl

## Cleanup Later
When feature is merged:
```bash
wt delete <branch-name>  # Safely removes worktree + checks for uncommitted work
```
