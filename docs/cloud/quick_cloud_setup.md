# Quick Cloud Setup - 15 Minute Guide (Supabase Pipeline)

This guide is for running TRR sync scripts on a small VM or container and writing to Supabase.

## Step 1: Create a VM (3 minutes)
- Ubuntu 22.04 (or similar)
- 2 vCPU / 4GB RAM is sufficient for most sync jobs
- Allow SSH access

## Step 2: Install Dependencies (5 minutes)
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git
```

Ensure Python 3.11+ is installed (use `python3.11` from deadsnakes if needed).

## Step 3: Clone + Configure (4 minutes)
```bash
git clone https://github.com/therealityreport/trr-backend.git
cd trr-backend
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, TMDB_API_KEY, IMDB_API_KEY
```

## Step 4: Run a Sync Job (3 minutes)
```bash
# Example: import shows from lists
PYTHONPATH=. python scripts/import/run_show_import_job.py --imdb-list ... --tmdb-list ...

# Example: run all TMDb enrichment jobs
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose
```

✅ Done! Use `screen`, `tmux`, or `systemd` if you want the process to run long-term.
