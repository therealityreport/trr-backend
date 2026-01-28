# Cloud Setup Guide (Supabase Pipeline)

This guide covers running TRR sync jobs on a cloud VM and writing data to Supabase.

## Option 1: AWS EC2

### 1. Launch Instance
- Instance type: t3.medium (2 vCPU, 4GB RAM)
- OS: Ubuntu 22.04 LTS
- Storage: 20GB
- Security group: Allow SSH (port 22)

### 2. Connect and Setup
```bash
ssh -i your-key.pem ubuntu@your-ec2-ip
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-venv python3-pip git
```

### 3. Clone + Configure
```bash
git clone https://github.com/therealityreport/trr-backend.git
cd trr-backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TMDB_API_KEY, IMDB_API_KEY
```

### 4. Run with Screen (Persistent Session)
```bash
screen -S trr-sync
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose
# Detach: Ctrl+A then D
```

## Option 2: DigitalOcean / GCP

Use the same steps as EC2. Any Ubuntu VM with Python 3.11+ works.

## Suggested Job Patterns

- **On-demand import:** `scripts/import/run_show_import_job.py` for list ingestion
- **Scheduled enrichment:** `scripts/sync/sync_shows_all.py`, `scripts/sync/sync_seasons_episodes.py`
- **Media sync:** `scripts/sync/sync_show_images.py`, `scripts/sync/sync_cast_photos.py`

## Environment Variables

Minimum required:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `TMDB_API_KEY`
- `IMDB_API_KEY`

Optional for media:
- AWS credentials (`AWS_REGION`, `AWS_S3_BUCKET`, `AWS_CDN_BASE_URL`)

## Artifacts

Store runtime artifacts outside the repo root (e.g. `../artifacts/trr-backend/`) and symlink `logs`, `debug_html`, `out`, `.cache` in the repo root to that external directory.
