# TRR-BACKEND REPO VAULT

Inherits: /Users/thomashulihan/brain/BRAIN.md

## On boot read ONLY
- this file
- /Users/thomashulihan/Projects/TRR/TRR-Backend/trr-backend-brain/README.md

## On demand
- /Users/thomashulihan/Projects/TRR/TRR-Backend/trr-backend-brain/architecture.md
- /Users/thomashulihan/Projects/TRR/trr-workspace-brain/api-contract.md
- /Users/thomashulihan/Projects/TRR/TRR-Backend/trr-backend-brain/handoffs/
- /Users/thomashulihan/Projects/TRR/TRR-Backend/trr-backend-brain/sessions/ (most recent only)

## Boundary rule
Would this still be true if `/Users/thomashulihan/Projects/TRR/TRR-APP` disappeared?
- Yes: keep it in this repo's own `trr-backend-brain/`
- No: move it to `/Users/thomashulihan/Projects/TRR/trr-workspace-brain/`

## Cross-repo handoff rule
- `AGENTS.md` is the primary project-facing entrypoint for Codex and Claude session work.
- Check `trr-backend-brain/handoffs/` before editing.
- If a change crosses the app boundary, update `/Users/thomashulihan/Projects/TRR/trr-workspace-brain/api-contract.md`.
- Drop a letter in `/Users/thomashulihan/Projects/TRR/trr-workspace-brain/handoffs/TRR-Backend-to-TRR-APP.md`.
