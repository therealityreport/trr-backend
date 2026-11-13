# Render deployment retired

Render is no longer a TRR production target. Vercel serves the application;
Modal hosts the API and jobs. Follow [the Modal deployment guide](modal.md).

Render service `trr-backend-api` was suspended on **2026-10-31**, according to
its provider event history. The repository hosting contracts were updated on
**2026-11-09**. These dates do not establish the first production traffic cutover.

The obsolete `render.yaml` blueprint and workspace deployment wrappers have
been retired. Do not recreate or resume the service through these old links.
Historical release receipts remain evidence of their original releases.

The workspace retirement record is
`docs/workspace/hosting-retirement-2026-11-09.md`.
