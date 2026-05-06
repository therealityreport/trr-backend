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
