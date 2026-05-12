# TRR-BACKEND INSTRUCTIONS

## Startup
- Start from this file, ../AGENTS.md, the active user request, and live backend files.
- Do not read saved notes, wiki pages, sessions, handoffs, patterns, or decisions on boot.
- Treat old plans and saved notes as stale until revalidated against current repo state, branch, tests, and user intent.

## Scope
- Backend-only instructions for /Users/thomashulihan/Projects/TRR/TRR-Backend.
- If routing, ownership, or policy scope is unclear, use ../AGENTS.md as the workspace authority.

## Cross-Repo Work
- If a change crosses the app boundary, update the current shared contract docs under /Users/thomashulihan/Projects/TRR/docs/ as needed.
- Coordinate app follow-through in the same session when backend API/schema behavior changes.

## Non-Negotiable Rules
- Keep schema, API, auth, and worker-contract changes aligned with ../AGENTS.md.
- Validate live-runtime assumptions against code, tests, or current runtime state before treating them as fixed.

## Debugging Discipline
- If the same command or workflow fails twice with the same substantive error, stop retrying blindly. Capture the exact command, full error, relevant stack trace/logs, and recent changes.
- Inspect local evidence first: source code, config, lockfiles, versions, tests, runtime state, and existing project patterns.
- If the cause is still unclear, or the failure involves third-party tooling, APIs, packages, browser/runtime behavior, or time-sensitive docs, research current primary sources such as official docs, release notes, and issue trackers. Identify 3-5 plausible causes or fixes before choosing one.
- Apply the smallest evidence-backed fix one change at a time, then rerun the failing command or workflow to verify. If the best fix is risky or ambiguous, explain the options before editing.

## Validation
- Run backend-local validation or tests touched by the change.
- Re-read ../AGENTS.md when workspace startup, MCP routing, or cross-repo policy is involved.
