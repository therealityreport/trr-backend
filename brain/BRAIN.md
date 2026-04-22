# TRR-BACKEND REPO VAULT

Inherits: /Users/thomashulihan/brain/BRAIN.md

## On boot read ONLY
- this file
- /Users/thomashulihan/Projects/TRR/TRR-Backend/brain/README.md

## On demand
- /Users/thomashulihan/Projects/TRR/TRR-Backend/brain/architecture.md
- /Users/thomashulihan/Projects/TRR/brain/api-contract.md
- /Users/thomashulihan/Projects/TRR/TRR-Backend/brain/handoffs/
- /Users/thomashulihan/Projects/TRR/TRR-Backend/brain/sessions/ (most recent only)

## Boundary rule
Would this still be true if `/Users/thomashulihan/Projects/TRR/TRR-APP` disappeared?
- Yes: keep it in this repo's own `brain/`
- No: move it to `/Users/thomashulihan/Projects/TRR/brain/`

## Cross-repo handoff rule
- `AGENTS.md` is the primary project-facing entrypoint for Codex and Claude session work.
- Check `brain/handoffs/` before editing.
- If a change crosses the app boundary, update `/Users/thomashulihan/Projects/TRR/brain/api-contract.md`.
- Drop a letter in `/Users/thomashulihan/Projects/TRR/brain/handoffs/TRR-Backend-to-TRR-APP.md`.
