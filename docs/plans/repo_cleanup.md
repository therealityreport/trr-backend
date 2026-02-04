# Repo Cleanup Spec

## Goal
Organize legacy documentation and scripts into dedicated legacy locations while preserving compatibility entrypoints. Update documentation and regenerate repository maps.

## Acceptance Criteria
- All legacy Google Sheets documentation lives under `docs/legacy/`, including `SHEET_EDIT_MAPPING.md`.
- `test_connection.py` remains runnable at the repo root but sources from `scripts/legacy/test_connection.py`.
- `scripts/README.md` clearly documents legacy script location and wrapper purpose.
- `make repo-map-check` passes with regenerated artifacts committed.
- Fast checks (`ruff` + `pytest`) pass, or any skips are explicitly justified.
