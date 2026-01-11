# Vibe Coding Workflow Guide

## 60-Second Mental Model

**TL;DR:** Safe, isolated development with automatic guardrails.

- **Branch** = A parallel version of the repo (your safe sandbox to experiment)
- **Worktree** = A separate folder checkout of that branch (no file collisions with main)
- **Worktrunk** = Tool that automates creating/managing worktrees + branches
- **Commit** = Local checkpoint (saved on your machine only)
- **Push** = Share your commits to GitHub (team can see, CI runs)
- **Hooks** = Guardrails that block unsafe merges/deletes (prevents foot-guns)

**Example flow (30 seconds):**
```bash
wt new feature/add-ratings      # Create isolated worktree
cd ../trr-backend-add-ratings   # Navigate to new folder
# Edit code, then:
ruff check . && pytest          # Fast validation (~10s)
git add . && git commit -m "feat: add ratings endpoint"
git push -u origin feature/add-ratings
/trr-pr                         # Create PR via Claude Code
# After merge:
wt delete feature/add-ratings   # Cleanup
```

**Why this matters:** Main branch stays pristine. You can work on 5 features in parallel without conflicts. Hooks catch bugs before they reach CI.

---

## Table of Contents
1. [Core Concepts](#core-concepts)
2. [Workflow Loop](#workflow-loop)
3. [Branch Isolation](#branch-isolation)
4. [Commit vs Push](#commit-vs-push)
5. [Slash Commands](#slash-commands)
6. [Validation Levels](#validation-levels)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## Core Concepts

### Branches: Parallel Realities
Think of git branches as parallel universes for your code:
- **main**: The stable, production-ready universe
- **feature/X**: An experimental universe where you try new ideas
- Changes in your feature branch don't affect main until you merge

### Worktrees: Physical Locations
Worktrees are separate directories on your filesystem, each checked out to a different branch:
- **Traditional**: `git checkout` switches branches IN PLACE (risky - can lose uncommitted work)
- **Worktrees**: Each branch has its own directory (safe - changes are isolated)

Example:
```
~/Projects/
├── trr-backend/              # Main worktree (main branch)
└── trr-backend-my-feature/   # Feature worktree (feature/my-feature branch)
```

### Worktrunk: Worktree Manager
Worktrunk (`wt`) automates worktree creation and management:
- `wt new feature/my-feature` → Creates worktree + runs setup hooks
- `wt delete feature/my-feature` → Cleans up worktree safely
- `wt list` → Shows all your active worktrees

---

## Workflow Loop

### The Complete Cycle

1. **Create Isolated Branch**
   ```bash
   wt new feature/add-new-endpoint
   cd ../trr-backend-add-new-endpoint
   ```

2. **Develop with Claude Code**
   - Use slash commands: `/trr-spec`, `/trr-plan`, `/trr-impl`
   - Make changes incrementally
   - Run validation frequently

3. **Commit Frequently**
   ```bash
   git add .
   git commit -m "feat: add new endpoint for show ratings"
   ```

   Commits are LOCAL checkpoints. You can make mistakes - just commit more fixes.

4. **Push to Share**
   ```bash
   git push -u origin feature/add-new-endpoint
   ```

   Push sends commits to GitHub. Now others can see your work (and CI runs).

5. **Create Pull Request**
   ```bash
   /trr-pr  # Claude Code helps generate PR description
   ```

   PR is a request to merge your feature branch into main.

6. **Clean Up After Merge**
   ```bash
   wt delete feature/add-new-endpoint
   ```

---

## Branch Isolation

### Why Isolate?

**Problem**: Working on multiple features simultaneously
- Feature A: Half-done changes
- Feature B: Urgent bug fix needed
- Switching branches with `git checkout` mixes uncommitted changes

**Solution**: Use worktrees
- Feature A: `/trr-backend-feature-a/` directory
- Feature B: `/trr-backend-bug-fix/` directory
- Changes are physically separate, no mixing

### Worktrunk Workflow

```bash
# Create new branch + worktree
wt new feature/advanced-filtering
cd ../trr-backend-advanced-filtering

# Work on feature...
git commit -m "Add filtering logic"

# Context switch to urgent fix
cd ../trr-backend  # Main worktree
wt new fix/critical-bug
cd ../trr-backend-critical-bug

# Fix bug...
git commit -m "Fix critical bug"
git push
/trr-pr

# Return to feature work
cd ../trr-backend-advanced-filtering
# Your feature changes are intact!
```

### Git Worktree Fallback

Without Worktrunk:
```bash
# Create worktree
git worktree add ../trr-backend-feature-x feature/feature-x
cd ../trr-backend-feature-x

# Set up manually (Worktrunk does this automatically)
cp ../.env .env
source .venv/bin/activate

# Work, commit, push...

# Clean up
cd ..
git worktree remove trr-backend-feature-x
```

---

## Commit vs Push

### Commit: Local Checkpoint
- **What**: Saves changes to your LOCAL git history
- **Where**: Only on your machine
- **When**: Frequently (after each logical change)
- **Undo**: Easy with `git reset` or `git revert`

```bash
git add .
git commit -m "Add test for new endpoint"
# ✅ Changes saved locally, not visible to others
```

### Push: Share with Team
- **What**: Uploads commits to remote (GitHub)
- **Where**: Remote repository (visible to team)
- **When**: When ready to share or backup work
- **Undo**: Harder (requires force push, affects others)

```bash
git push
# ✅ Commits now on GitHub, CI runs, team can review
```

### Typical Pattern

```bash
# Work session 1
git commit -m "Add endpoint skeleton"
git commit -m "Implement business logic"
git commit -m "Fix typing error"
# Still local, iterating quickly

# End of day / ready for review
git push
# Share all 3 commits with team
```

---

## Slash Commands

### /trr-spec - Specification
**When:** Starting new feature or major change
```
/trr-spec add support for user ratings and reviews
```
Claude Code will:
- Ask clarifying questions
- Research existing patterns
- Draft specification document
- Save to docs/

### /trr-plan - Planning
**When:** Have spec, need implementation roadmap
```
/trr-plan implement user ratings feature (based on spec)
```
Claude Code will:
- Read specification
- Explore codebase
- Design step-by-step plan
- Identify risks and dependencies

### /trr-impl - Implementation
**When:** Have plan, ready to code
```
/trr-impl execute ratings feature plan
```
Claude Code will:
- Follow plan incrementally
- Run validation after changes
- Create commits with descriptive messages

### /trr-validate - Validation
**When:** Before committing or creating PR
```
/trr-validate
```
Runs appropriate validation:
- Fast: ruff + pytest (pre-commit)
- Medium: + schema/repo-map checks (pre-PR)
- Full: + Supabase/Docker CI (rarely needed)

### /trr-pr - Pull Request
**When:** Ready to merge feature
```
/trr-pr
```
Claude Code will:
- Run pre-flight validation
- Review all commits
- Generate PR description
- Create PR via gh CLI
- Return PR URL

### /trr-wt-new - New Branch
**When:** Starting new work
```
/trr-wt-new
```
Claude Code will:
- Ask for branch name
- Suggest name based on task
- Create worktree with setup
- Navigate to new workspace

---

## Validation Levels

### Fast (Pre-Commit) - 10-30 seconds
Run before every commit:
```bash
ruff check .           # Linting
ruff format --check .  # Format check
pytest                 # Unit tests
```

### Medium (Pre-PR) - 1-2 minutes
Run before opening PR:
```bash
# Fast validation +
make schema-docs-check   # If schema changed
make repo-map-check      # If structure changed
```

### Full (CI Simulation) - 5-10 minutes
Rarely needed locally (CI runs this):
```bash
make ci-local  # Requires Docker + Supabase
```

### What to Run When
| Situation | Validation Level |
|-----------|-----------------|
| Modified Python code | Fast |
| Added new module | Fast + repo-map-check |
| Changed database schema | Fast + schema-docs-check |
| Before PR | Medium |
| Debugging CI failure | Full (optional) |

---

## Troubleshooting

### "Tests failing after branch switch"
**Cause**: Dependencies changed between branches
**Solution**:
```bash
pip install -r requirements.txt  # Update deps
pytest  # Re-run tests
```

### "Environment variables missing"
**Cause**: .env not copied to worktree
**Solution**:
```bash
cp ../trr-backend/.env .env  # Copy from main
# Or regenerate from template:
cp .env.example .env
# Edit .env with your credentials
```

### "Worktrunk command not found"
**Cause**: Worktrunk not installed
**Solution**: Use git worktree fallback (see Branch Isolation section)

### "Pre-merge hook failing"
**Cause**: Code doesn't pass validation
**Solution**:
```bash
ruff check .  # See specific errors
ruff format .  # Auto-fix formatting
pytest -v     # See test failures
```

### "Can't delete worktree - uncommitted changes"
**Cause**: Safety check preventing data loss
**Solution**:
```bash
git status  # Review changes
# Option 1: Commit changes
git add . && git commit -m "WIP: save progress"
# Option 2: Stash changes
git stash
# Option 3: Discard changes (careful!)
git reset --hard
```

---

## Best Practices

1. **Commit Often**: Small, atomic commits are easy to review and debug
2. **Push Regularly**: Backup your work and enable collaboration
3. **Run Fast Validation**: Before every commit (10 seconds well spent)
4. **Use Worktrees**: Avoid branch switching headaches
5. **Write Descriptive Commits**: Future you will thank present you
6. **Read CI Failures**: GitHub Actions output shows exactly what failed

---

## Reference

- **Git Workflow**: [docs/Repository/diagrams/git_workflow.md](../Repository/diagrams/git_workflow.md)
- **Architecture**: [docs/architecture.md](../architecture.md)
- **Database**: [docs/db/schema.md](../db/schema.md)
- **Claude Code**: [CLAUDE.md](../../CLAUDE.md)
