#!/usr/bin/env python3
"""Export canonical Reddit schemas for the app's existing OpenAPI type generator."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trr_backend.repositories import reddit_contract

models = [
    reddit_contract.RedditRunCaptureContract,
    reddit_contract.RedditWorkTarget,
    reddit_contract.RedditCaptureEvidence,
    reddit_contract.RedditPostCapture,
]
schemas = {}
for model in models:
    schema = model.model_json_schema(ref_template="#/components/schemas/{model}")
    schemas.update(schema.pop("$defs", {}))
    schemas[model.__name__] = schema
payload = {
    "openapi": "3.1.0",
    "info": {"title": "Reddit Capture", "version": "2"},
    "paths": {},
    "components": {"schemas": schemas},
}
print(json.dumps(payload, indent=2, sort_keys=True))
