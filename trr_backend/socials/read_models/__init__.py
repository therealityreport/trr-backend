"""Backend-owned social read models.

Read-model modules may query persisted state and compose response-ready domain
payloads. They must not claim jobs, launch scraper work, dispatch workers, or
own API route parsing.
"""

from __future__ import annotations
