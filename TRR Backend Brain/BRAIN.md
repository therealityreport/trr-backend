# TRR-BACKEND REPO VAULT

Inherits: /Users/thomashulihan/brain/BRAIN.md

## Scope
- Backend-only instructions for `/Users/thomashulihan/Projects/TRR/TRR-Backend`.
- If routing, ownership, or policy scope is unclear, escalate to `../AGENTS.md`.

## On boot read ONLY
- this file
- /Users/thomashulihan/Projects/TRR/TRR-Backend/TRR Backend Brain/README.md

## On demand
- /Users/thomashulihan/Projects/TRR/TRR-Backend/TRR Backend Brain/architecture.md
- /Users/thomashulihan/Projects/TRR/TRR Workspace Brain/api-contract.md
- /Users/thomashulihan/Projects/TRR/TRR-Backend/TRR Backend Brain/handoffs/
- /Users/thomashulihan/Projects/TRR/TRR-Backend/TRR Backend Brain/sessions/ (most recent only)

## Non-Negotiable Rules
- `AGENTS.md` is the primary project-facing entrypoint for Codex and Claude session work.
- If this would still matter without `/Users/thomashulihan/Projects/TRR/TRR-APP`, keep it in `TRR Backend Brain/`; otherwise move it to `/Users/thomashulihan/Projects/TRR/TRR Workspace Brain/`.
- Check `TRR Backend Brain/handoffs/` before editing.
- If a change crosses the app boundary, update `/Users/thomashulihan/Projects/TRR/TRR Workspace Brain/api-contract.md`.
- Drop a letter in `/Users/thomashulihan/Projects/TRR/TRR Workspace Brain/handoffs/TRR-Backend-to-TRR-APP.md`.

## Validation
- Run the backend-local validation or tests touched by the change.
- Re-read `../AGENTS.md` when workspace startup, MCP routing, or cross-repo policy is involved.
