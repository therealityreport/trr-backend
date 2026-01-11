# Stop Reminder Hook

**Trigger:** When user stops or exits Claude Code session

**Purpose:** Remind user to commit work and check git status before leaving

## Actions on Stop

1. **Run git status** (read-only, safe):
```bash
git status --short
git branch --show-current
```

2. **Check for uncommitted changes:**
   - Modified files?
   - Untracked files?
   - Staged but not committed?

3. **Check for unpushed commits:**
```bash
git log --branches --not --remotes --oneline
```

4. **Display reminder message:**

```
⏸️  Session ending. Workspace status check:

📍 Current branch: feature/my-feature
📂 Location: ~/Projects/trr-backend-my-feature

📝 Uncommitted changes:
 M api/routers/shows.py
 M tests/test_shows.py
?? new_script.py

💾 Unpushed commits:
abc1234 feat: add user ratings endpoint
def5678 test: add ratings tests

⚠️  Reminders before you go:
- Uncommitted work? → git add . && git commit -m "description"
- Ready to share? → git push
- Done with feature? → wt delete feature/my-feature (cleanup)
- Switch branches? → wt list (see all worktrees)

✅ Changes are isolated in this worktree - main branch is untouched.
```

## Safety Notes
- This hook is INFORMATIONAL only (doesn't block)
- Encourages good habits (commit often, push regularly)
- Reminds user of worktree isolation benefits
