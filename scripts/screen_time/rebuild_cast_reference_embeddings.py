#!/usr/bin/env python3
"""Rebuild gallery-derived cast reference embeddings with the screen-time model."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trr_backend.repositories import face_references  # noqa: E402
from trr_backend.services import face_reference_embeddings  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--person-id", default="", help="Optional person id to rebuild.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum references to rebuild.")
    parser.add_argument("--dry-run", action="store_true", help="List references without writing embeddings.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    rows = face_references.list_gallery_face_reference_images_for_rebuild(
        person_id=args.person_id or None,
        limit=args.limit,
    )
    results: list[dict[str, object]] = []
    for row in rows:
        reference_id = str(row.get("id") or "").strip()
        image_source = str(row.get("hosted_url") or row.get("source_url") or "").strip()
        if not reference_id or not image_source:
            results.append({"reference_image_id": reference_id, "status": "skipped", "reason": "missing_source"})
            continue
        if args.dry_run:
            results.append({"reference_image_id": reference_id, "status": "dry_run"})
            continue
        result = face_reference_embeddings.register_reference_image(
            reference_image_id=reference_id,
            image_source=image_source,
            assigned_person_id=str(row.get("person_id") or "") or None,
        )
        results.append(
            {
                "reference_image_id": reference_id,
                "embedding_status": result.get("embedding_status"),
                "error_message": result.get("error_message"),
            }
        )
    print(json.dumps({"count": len(results), "results": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
