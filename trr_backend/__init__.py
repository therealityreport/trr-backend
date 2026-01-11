"""
Shared TRR backend library code.

This package is intended to hold code that is reused across:
- the FastAPI app in `api/`
- pipeline scripts in `scripts/`

App entrypoints (FastAPI routers, CLI scripts) should live outside this package and
import from `trr_backend` rather than the other way around.
"""
