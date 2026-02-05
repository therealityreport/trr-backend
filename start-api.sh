#!/bin/bash
# Start TRR Backend API
# Run this once, leave it running for all frontends (TRR-APP, SCREENALYTICS, etc.)

cd "$(dirname "$0")"
source .venv/bin/activate
uvicorn api.main:app --reload --port 8000
