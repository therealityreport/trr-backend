"""Season ingest pipeline orchestration.

This package owns season-level ingest scheduling and pipeline assembly. It must
preserve existing stage names, queue metadata, and worker lane semantics.
"""

from __future__ import annotations
