# SAVED NOTES ARCHIVE

This directory is an on-demand archive. It is not a bootloader for Codex or Claude sessions.

## Startup Boundary
- Agents must not read this directory on boot.
- Agents must not treat saved pages, old plans, sessions, handoffs, patterns, or decisions as implementation authority.
- Current user request, current workspace instructions, live repo files, branch state, tests, and runtime evidence override anything stored here.

## On Demand Only
Read files in this directory only when the user explicitly asks for saved memory, prior notes, wiki, archive, or source ingestion work.

## Stale Plan Rule
- Old plans are stale by default.
- Failed, superseded, incomplete, or already-implemented plans must not be resumed without current repo verification and explicit user intent.
- If a saved note conflicts with current code or docs, current code and docs win.

## Debugging Discipline
- If the same command or workflow fails twice with the same substantive error, stop retrying blindly. Capture the exact command, full error, relevant stack trace/logs, and recent changes.
- Inspect local evidence first: source code, config, lockfiles, versions, tests, runtime state, and existing project patterns.
- If the cause is still unclear, or the failure involves third-party tooling, APIs, packages, browser/runtime behavior, or time-sensitive docs, research current primary sources such as official docs, release notes, and issue trackers. Identify 3-5 plausible causes or fixes before choosing one.
- Apply the smallest evidence-backed fix one change at a time, then rerun the failing command or workflow to verify. If the best fix is risky or ambiguous, explain the options before editing.
