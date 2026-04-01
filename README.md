# The Reality Report Backend Data Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status: Production](https://img.shields.io/badge/status-production-green.svg)](https://github.com/therealityreport/trr-backend)

**The Reality Report** backend data pipeline for reality TV show and cast information. This system automatically collects, enriches, and curates comprehensive data about reality TV shows and their cast members from multiple sources.

## 🎯 Overview

The TRR Backend Data Pipeline is a Supabase-first data processing system that transforms raw data from APIs and web sources into a structured, production-ready dataset for The Reality Report platform. It supports both direct sync scripts and a resumable pipeline orchestrator.

### Key Features
- **Supabase-first storage**: Normalized core schema in Postgres
- **Multi-Source Data Collection**: TMDb, IMDb, Fandom Wikis, Famous Birthdays
- **AI-Powered Enrichment**: Gemini AI for text analysis and gap filling
- **Resumable orchestration**: Pipeline run tracking + stage-level resume
- **Scalable Processing**: Handles 10,000+ cast members and 1,000+ shows

## 🚀 Quick Start

### Prerequisites
- Python 3.11 or higher
- Supabase project URL + Service Role key
- API keys for TMDb, IMDb, and Gemini AI
- Optional: AWS credentials for S3 media mirroring

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/therealityreport/trr-backend.git
   cd trr-backend
   ```

2. **Install dependencies**
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Set up environment**
   ```bash
   # Copy example environment file
   cp .env.example .env
   
   # Edit .env with your API keys and credentials
   nano .env
   ```

4. **Add credentials**
  - Set the runtime Postgres URL in `.env`: `TRR_DB_URL` (Supavisor session mode on `pooler.supabase.com:5432`)
  - Optional local-only troubleshooting lane: set `TRR_DB_FALLBACK_URL` to an operator-supplied direct-host DSN when you intentionally bypass session-mode saturation in dev; keep the derived direct-fallback toggle unset
  - Set auth/runtime secrets in `.env`: `SUPABASE_JWT_SECRET`, `TRR_INTERNAL_ADMIN_SHARED_SECRET`, `SCREENALYTICS_SERVICE_TOKEN`
  - Set API keys in `.env`: `TMDB_BEARER_TOKEN` (or `TMDB_API_KEY`), `TVDB_API_KEY`, `IMDB_API_KEY`, `GEMINI_API_KEY`
  - Optional: `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_REGION`, `OBJECT_STORAGE_PUBLIC_BASE_URL` for media mirroring

5. **Verify environment**
   ```bash
   # Check that all dependencies are correctly installed
   make doctor
   ```

## 🧰 DB Sync Scripts

These scripts read the list of shows from `core.shows` (Supabase) and update tables directly. They load `.env` from the repo root.

```bash
# Shows (metadata + entities + watch providers)
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose

# Seasons + episodes
PYTHONPATH=. python scripts/sync/sync_seasons_episodes.py --all --verbose

# People + cast/credits
PYTHONPATH=. python scripts/sync/sync_people.py --all --verbose

# Show images
PYTHONPATH=. python scripts/sync/sync_show_images.py --all --verbose

# Season/episode images
PYTHONPATH=. python scripts/sync/sync_season_episode_images.py --all --verbose

# People photos (multi-source)
PYTHONPATH=. python scripts/sync/sync_cast_photos.py --imdb-person-id nm11883948 --verbose

# TMDb resolution + backfill (shows)
PYTHONPATH=. python scripts/sync/resolve_tmdb_ids_via_find.py --all --verbose
PYTHONPATH=. python scripts/backfill/backfill_tmdb_show_details.py --all --verbose

# TMDb entities (networks, production companies) + object-storage logo mirroring
PYTHONPATH=. python scripts/sync/sync_tmdb_show_entities.py --all --verbose

# TMDb watch providers + object-storage logo mirroring
PYTHONPATH=. python scripts/sync/sync_tmdb_watch_providers.py --all --verbose
```

Legacy composite runner:

```bash
python -m scripts.sync_all_tables --all
python -m scripts.sync_all_tables --tables shows,episodes,episode_appearances --imdb-id tt1234567
```

Common filters: `--show-id`, `--tmdb-id`, `--imdb-id`, `--limit`, `--dry-run`, `--verbose`.

Media mirroring requires `OBJECT_STORAGE_BUCKET`, `OBJECT_STORAGE_REGION`, and `OBJECT_STORAGE_PUBLIC_BASE_URL` (must start with `https://` and not contain placeholders like `dxxxx`). Optional: `OBJECT_STORAGE_PROFILE`.
Runtime DB access uses `TRR_DB_URL` and optional `TRR_DB_FALLBACK_URL`. Tooling can still accept legacy DB envs where explicitly documented.

TMDb backfill flow: resolve missing `tmdb_id` via `/find` using IMDb ids, then backfill `/tv/{id}` details into `core.shows` (typed columns + `tmdb_meta`). Both scripts are idempotent; omit `--all` for incremental updates. See `docs/architecture.md` for the full TMDb enrichment pipeline documentation.

Incremental/resume flags: `--incremental/--no-incremental`, `--resume/--no-resume`, `--force`, `--since`.
Incremental mode uses `core.sync_state` + `shows.most_recent_episode` to skip unchanged shows and retry failures.
After seasons/episodes sync, `shows.show_total_seasons` is normalized to the count of seasons with `season_number > 0`.
Per-show progress is stored in `core.sync_state` (one row per show + table).

## 🧭 Pipeline Orchestrator (Resumable)

The pipeline orchestrator records runs and stages in the `pipeline` schema and supports resume-by-hash.

```bash
python -m trr_backend.cli pipeline run --all --verbose
python -m trr_backend.cli pipeline list
python -m trr_backend.cli pipeline status <run-id>
```

See `docs/architecture/pipeline.md` for details.

## 🔐 Security

Never commit API keys, AWS credentials, or private keys. Rotate any exposed credentials immediately.

## 📦 Repo Layout

- `api/`: FastAPI app (Supabase-backed API + WebSockets)
- `trr_backend/`: Shared library code (reused by API + pipeline)
  - `trr_backend/integrations/`: External metadata clients (IMDb/TMDb/etc.)
  - `trr_backend/pipeline/`: Pipeline orchestration logic
  - `trr_backend/cli/`: CLI entrypoints (Typer)
- `scripts/`: Data sync scripts and utilities
- `supabase/`: Database schema, migrations, and seeds
- `docs/`: Architecture and operating docs

For detailed repository structure, module dependency graphs, and architecture diagrams, see [docs/Repository/README.md](docs/Repository/README.md).

## 📁 Architecture

The backend uses a Supabase database with data sync scripts that fetch from external APIs and populate the database.

**Current Architecture:**
- Data stored in Supabase PostgreSQL (`core.*` schema)
- Sync scripts in `scripts/` fetch from TMDb, IMDb, Fandom wikis
- Resumable pipeline orchestrator in `trr_backend/pipeline/`
- FastAPI app in `api/` serves data to the frontend

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

> **Note:** The legacy numbered pipeline (`1-ShowInfo/`, `2-CastInfo/`, etc.) has been removed.
> Git history preserves these files if needed for reference.
> Current data ingestion uses the DB Sync Scripts documented above.

## 📊 Data Sources

| Source | Purpose | Data Type | Rate Limits |
|--------|---------|-----------|-------------|
| **TMDb API** | Primary show and cast metadata | Shows, Cast, Episodes | 40 requests/10s |
| **IMDb API** | Episode details and additional cast info | Episodes, Credits | 1000 requests/day |
| **Fandom Wikis** | Reality show-specific cast details | Cast bios, Show info | Respectful scraping |
| **Famous Birthdays** | Biographical data | Birthdays, Zodiac signs | Rate limited |
| **Gemini AI** | Text analysis and gap filling | Guest names, Descriptions | 1000 requests/minute |

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` (never commit `.env`):

```bash
# API Keys
TMDB_BEARER_TOKEN=your_tmdb_bearer_token
TMDB_API_KEY=your_tmdb_api_key
TVDB_API_KEY=your_tvdb_api_key
IMDB_API_KEY=your_imdb_api_key  
GEMINI_API_KEY=your_gemini_api_key

# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
TRR_DB_URL=postgresql://postgres.<project>:password@aws-1-us-east-1.pooler.supabase.com:5432/postgres
TRR_DB_FALLBACK_URL=
# Local-only troubleshooting lane only. Supply a direct-host DSN here when needed.
# Leave TRR_DB_ENABLE_DIRECT_FALLBACK unset.

# Optional Configuration
REALITEASE_TMDB_SHOW_LIMIT=5
GEMINI_MODEL=gemini-2.5-flash
GEMINI_MODEL_FAST=gemini-2.5-flash
GEMINI_MODEL_PRO=gemini-2.5-pro
GOOGLE_GEMINI_MODEL=gemini-2.5-flash
# Deprecated fallback alias (temporary)
GEMINI-MODEL=gemini-2.5-flash

# Legacy Google Sheets (archived)
SPREADSHEET_NAME=Realitease2025Data
GOOGLE_APPLICATION_CREDENTIALS=keys/service-account.json
```

### Supabase Structure

For the authoritative schema, see `docs/db/schema.md` and `docs/architecture.md`.

## 🛠️ Development

### Running Individual Steps

Each sync stage can be run independently with various options:

```bash
# Import shows from lists
PYTHONPATH=. python scripts/import/import_shows_from_lists.py --imdb-list ... --tmdb-list ...

# Enrich shows (TMDb metadata + entities + providers)
PYTHONPATH=. python scripts/sync/sync_shows_all.py --all --verbose

# Seasons + episodes
PYTHONPATH=. python scripts/sync/sync_seasons_episodes.py --all --verbose

# People + cast
PYTHONPATH=. python scripts/sync/sync_people.py --all --verbose
```

### Monitoring Progress

- **Logs**: Check `../artifacts/trr-backend/logs/` (or the `logs` symlink) for execution logs and results
- **Dry Run**: Use `--dry-run` flag to preview changes before writing
- **Limited Processing**: Use `--limit` to test with smaller datasets
- **Progress Tracking**: Most scripts provide detailed progress output

### Caching

The system includes comprehensive caching for efficiency:

- **Gemini Responses**: Cached in `../artifacts/trr-backend/.cache/` (or the `.cache` symlink)
- **TMDb API**: Per-session caching to respect rate limits
- **IMDb Data**: Local caching for repeated requests
- **Custom Cache**: Use `--cache-file` to specify custom locations

## 📈 Data Flow

```
External APIs → Ingestion Scripts → Supabase → API/Exports
     ↓                 ↓             ↓          ↓
TMDb/IMDb/Fandom   import/sync     core.*     FastAPI + S3
```

## 🔍 Quality Assurance

### Data Validation
- **Type Checking**: Validates data types and formats
- **Required Fields**: Ensures all required fields are present
- **Consistency Checks**: Verifies data consistency across sources
- **Conflict Resolution**: Handles data conflicts intelligently

### Error Handling
- **Comprehensive Logging**: Detailed logs for all operations
- **Graceful Failures**: Handles API failures without data loss
- **Retry Logic**: Automatic retry for transient failures
- **Recovery Mechanisms**: Data recovery capabilities

### Performance Monitoring
- **Processing Speed**: 1,000+ records per hour
- **Error Rates**: < 1% processing errors
- **API Compliance**: 100% rate limit compliance
- **Resource Usage**: Efficient memory and CPU usage

## 📂 Project Structure

```
TRR-Backend/
├── api/                       # FastAPI application
├── trr_backend/               # Shared library code
│   ├── db/                    # Database utilities
│   ├── integrations/          # External API clients (IMDb, TMDb, etc.)
│   ├── ingestion/             # Data ingestion modules
│   └── repositories/          # Database access layer
├── scripts/                   # Data sync and utility scripts
│   ├── dev/                   # Development tools (doctor.py)
│   ├── db/                    # Database SQL scripts
│   └── supabase/              # Supabase-specific utilities
├── supabase/                  # Database schema and migrations
│   ├── migrations/            # SQL migrations
│   └── schema_docs/           # Auto-generated schema documentation
├── tests/                     # Test suite
├── docs/                      # Documentation
├── requirements.txt           # Python dependencies
└── Makefile                   # Common development tasks
```

## 🚀 Deployment

### Local Development
```bash
# Install dependencies
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your credentials

# Verify setup
make doctor

# Run tests
pytest
```

### Cloud Deployment
See `docs/cloud/` for detailed cloud deployment guides.

## 🔐 Security

- **Rotate Secrets**: This repo previously tracked a `.env` file. Assume any keys in it are compromised and rotate them.
- **Never Commit `.env`**: Local `.env` files are gitignored; use `.env.example` as the template.
- **Credentials**: Keep service account JSONs under `keys/` (gitignored) or inject via CI secrets.
- **Generated Output**: Runtime artifacts live outside the repo root in `../artifacts/trr-backend/` (e.g. `logs/`, `.cache/`, `debug_html/`, `out/`). Use symlinks in the repo root for convenience.
- **Optional History Purge**: If you need to remove leaked secrets from git history, rotate keys first, then use a history-rewrite tool and force-push.

## 📝 Documentation

- **PRD**: See `PRD.md` for comprehensive product requirements
- **Architecture**: See `docs/architecture.md` for a high-level system overview
- **Pipeline Orchestration**: See `docs/architecture/pipeline.md` for staged runs and resume logic
- **DB Schema**: See `docs/db/schema.md` for core tables and views
- **Setup Guides**: See `docs/cloud/` for deployment documentation
- **Local Development**: See `docs/README_local.md` for additional setup notes
- **API Docs UI**: See `docs/api/run.md` for `/docs`, `/redoc`, and `/openapi.json`
- **Legacy Google Sheets Pipeline**: See `docs/legacy/google_sheets_pipeline.md`

## 🤝 Contributing

See `CONTRIBUTING.md`.

1. Fork the repository
2. Make your changes (default: `main`; this repo does not enforce a branch naming convention)
3. Commit your changes (`git commit -m 'Describe your change'`)
4. Push your changes
5. Open a Pull Request

### Development Guidelines
- Follow Python PEP 8 style guidelines
- Add comprehensive error handling
- Include detailed logging
- Test with dry-run mode first
- Update documentation for new features

## 📊 Performance Metrics

- **Data Volume**: 10,000+ cast members, 1,000+ shows
- **Processing Speed**: 1,000+ records per hour
- **Accuracy**: 98%+ accuracy compared to source APIs
- **Uptime**: 99%+ availability
- **Error Rate**: < 1% processing errors

## 🐛 Troubleshooting

### Common Issues

**Supabase Connectivity**
```bash
# Verify environment and connectivity
make doctor
```

**Schema Cache Issues**
```bash
# Reload PostgREST schema cache
bash scripts/reload_postgrest_schema.sh
```

**Data Parity Checks**
```bash
# Validate credits/media parity
PYTHONPATH=. python scripts/verify/verify_credits_parity.py
PYTHONPATH=. python scripts/verify/verify_media_unification.py
```

### Getting Help

- **Issues**: Create an issue on GitHub
- **Documentation**: Check `docs/` directory
- **Logs**: Review `logs/` directory for error details
- **Dry Run**: Use `--dry-run` flag to test changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **TMDb**: For comprehensive movie and TV database
- **IMDb**: For detailed episode and cast information
- **Google**: For Gemini AI
- **Fandom**: For reality TV show wikis and community data

---

**The Reality Report Backend Data Pipeline** - Transforming reality TV data into actionable insights.

*For questions or support, please open an issue or contact the development team.*
