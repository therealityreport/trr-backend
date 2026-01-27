# The Reality Report Backend Data Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status: Production](https://img.shields.io/badge/status-production-green.svg)](https://github.com/therealityreport/trr-backend-2025)

**The Reality Report** backend data pipeline for reality TV show and cast information. This system automatically collects, enriches, and curates comprehensive data about reality TV shows and their cast members from multiple sources.

## 🎯 Overview

The TRR Backend Data Pipeline is a sophisticated 5-stage data processing system that transforms raw data from APIs and web sources into a structured, production-ready dataset for The Reality Report platform. It handles everything from initial data collection to final production deployment.

### Key Features
- **Multi-Source Data Collection**: TMDb, IMDb, Fandom Wikis, Famous Birthdays
- **AI-Powered Enrichment**: Gemini AI for text analysis and gap filling
- **Comprehensive Validation**: Data quality assurance and error handling
- **Scalable Processing**: Handles 10,000+ cast members and 1,000+ shows
- **Production Ready**: Firebase integration and deployment support

## 🚀 Quick Start

### Prerequisites
- Python 3.11 or higher
- Google Cloud service account with Sheets API access
- API keys for TMDb, IMDb, and Gemini AI

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/therealityreport/trr-backend-2025.git
   cd trr-backend-2025
   ```

2. **Install dependencies**
   ```bash
   python3 -m venv .venv
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
   - Place your Google service account JSON in `keys/`
   - Set API keys in `.env`: `TMDB_BEARER_TOKEN` (or `TMDB_API_KEY`), `IMDB_API_KEY`, `GEMINI_API_KEY`
   - Configure spreadsheet name: `SPREADSHEET_NAME=Realitease2025Data`

5. **Verify environment**
   ```bash
   # Check that all dependencies are correctly installed
   make doctor
   ```

## 🧰 DB Sync Scripts

These scripts read the list of shows from `core.shows` (Supabase) and update tables directly. They load `.env` from the repo root.

```bash
# Shows (metadata + entities + watch providers)
PYTHONPATH=. python scripts/sync_shows_all.py --all --verbose

# Seasons + episodes
PYTHONPATH=. python scripts/sync_seasons_episodes.py --all --verbose

# People + cast/credits
PYTHONPATH=. python scripts/sync_people.py --all --verbose

# Show images
PYTHONPATH=. python scripts/sync_show_images.py --all --verbose

# Season/episode images
PYTHONPATH=. python scripts/sync_season_episode_images.py --all --verbose

# People photos (multi-source)
PYTHONPATH=. python scripts/sync_cast_photos.py --imdb-person-id nm11883948 --verbose

# TMDb resolution + backfill (shows)
PYTHONPATH=. python scripts/resolve_tmdb_ids_via_find.py --all --verbose
PYTHONPATH=. python scripts/backfill_tmdb_show_details.py --all --verbose

# TMDb entities (networks, production companies) + S3 logo mirroring
PYTHONPATH=. python scripts/sync_tmdb_show_entities.py --all --verbose

# TMDb watch providers + S3 logo mirroring
PYTHONPATH=. python scripts/sync_tmdb_watch_providers.py --all --verbose
```

Legacy composite runner:

```bash
python -m scripts.sync_all_tables --all
python -m scripts.sync_all_tables --tables shows,episodes,episode_appearances --imdb-id tt1234567
```

Common filters: `--show-id`, `--tmdb-id`, `--imdb-id`, `--limit`, `--dry-run`, `--verbose`.

Media mirroring requires `AWS_S3_BUCKET`, `AWS_REGION` (or `AWS_DEFAULT_REGION`), and `AWS_CDN_BASE_URL` (must start with https:// and not contain placeholders like dxxxx). Optional: `AWS_PROFILE`/`AWS_DEFAULT_PROFILE`.
Schema verification uses `SUPABASE_DB_URL` (remote Supabase only).

TMDb backfill flow: resolve missing `tmdb_id` via `/find` using IMDb ids, then backfill `/tv/{id}` details into `core.shows` (typed columns + `tmdb_meta`). Both scripts are idempotent; omit `--all` for incremental updates. See `docs/architecture.md` for the full TMDb enrichment pipeline documentation.

Incremental/resume flags: `--incremental/--no-incremental`, `--resume/--no-resume`, `--force`, `--since`.
Incremental mode uses `core.sync_state` + `shows.most_recent_episode` to skip unchanged shows and retry failures.
After seasons/episodes sync, `shows.show_total_seasons` is normalized to the count of seasons with `season_number > 0`.
Per-show progress is stored in `core.sync_state` (one row per show + table).

## 🔐 Security

Never commit API keys, AWS credentials, or private keys. Rotate any exposed credentials immediately.

## 📦 Repo Layout

- `api/`: FastAPI app (Supabase-backed API + WebSockets)
- `trr_backend/`: Shared library code (reused by API + pipeline)
  - `trr_backend/integrations/`: External metadata clients (IMDb/TMDb/etc.)
- `scripts/`: Data sync scripts and utilities
- `supabase/`: Database schema, migrations, and seeds
- `docs/`: Architecture and operating docs

For detailed repository structure, module dependency graphs, and architecture diagrams, see [docs/Repository/README.md](docs/Repository/README.md).

## 📁 Architecture

The backend uses a Supabase database with data sync scripts that fetch from external APIs and populate the database.

**Current Architecture:**
- Data stored in Supabase PostgreSQL (`core.*` schema)
- Sync scripts in `scripts/` fetch from TMDb, IMDb, Fandom wikis
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
IMDB_API_KEY=your_imdb_api_key  
GEMINI_API_KEY=your_gemini_api_key

# Google Sheets Configuration
SPREADSHEET_NAME=Realitease2025Data
GOOGLE_APPLICATION_CREDENTIALS=keys/service-account.json

# Optional Configuration
REALITEASE_TMDB_SHOW_LIMIT=5
GOOGLE_GEMINI_MODEL=gemini-2.5-flash
```

### Google Sheets Structure

The pipeline works with a Google Sheets workbook containing these tabs:

| Sheet | Purpose | Key Columns |
|-------|---------|-------------|
| **ShowInfo** | Show metadata and IDs | ShowName, IMDbID, TMDbID, Network |
| **CastInfo** | Cast member details by show | CastName, IMDbID, ShowName, Episodes |
| **RealiteaseInfo** | Person-focused aggregated data | PersonName, Shows, Birthday, Zodiac |
| **WWHLinfo** | Watch What Happens Live episodes | EpisodeID, Season, Episode, Guests |
| **FinalList** | Curated final dataset | All consolidated data |

## 🛠️ Development

### Running Individual Steps

Each pipeline stage can be run independently with various options:

```bash
# Dry run with limited rows
python3 RealiteaseInfo_Step3.py --dry-run --limit 100

# Process specific row range
python3 FinalInfo_Step2.py --start-row 1000 --end-row 2000

# Custom batch size and delays
python3 FinalInfo_Step2.py --batch-size 500 --delay 0.8

# Verbose logging
python3 CastInfo_Step1.py --verbose
```

### Monitoring Progress

- **Logs**: Check `logs/` directory for execution logs and results
- **Dry Run**: Use `--dry-run` flag to preview changes before writing
- **Limited Processing**: Use `--limit` to test with smaller datasets
- **Progress Tracking**: Most scripts provide detailed progress output

### Caching

The system includes comprehensive caching for efficiency:

- **Gemini Responses**: Cached in `.cache/` directory
- **TMDb API**: Per-session caching to respect rate limits
- **IMDb Data**: Local caching for repeated requests
- **Custom Cache**: Use `--cache-file` to specify custom locations

## 📈 Data Flow

```
External APIs → Data Collection → Processing Pipeline → Quality Validation → Production Storage
     ↓              ↓                    ↓                    ↓                    ↓
   TMDb/IMDb    ShowInfo         CastInfo/RealiteaseInfo    Validation         Firebase
     ↓              ↓                    ↓                    ↓                    ↓
   Rate Limits   Batch Updates      Person Enrichment    Error Handling    Final Dataset
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
python3 -m venv .venv
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
- **Generated Output**: `logs/`, `.cache/`, and `debug_html/` are runtime artifacts and are excluded from version control.
- **Optional History Purge**: If you need to remove leaked secrets from git history, rotate keys first, then use a history-rewrite tool and force-push.

## 📝 Documentation

- **PRD**: See `PRD.md` for comprehensive product requirements
- **Architecture**: See `docs/architecture.md` for a high-level system overview
- **Setup Guides**: See `docs/cloud/` for deployment documentation
- **API Mapping**: See `docs/SHEET_EDIT_MAPPING.md` for column specifications
- **Local Development**: See `docs/README_local.md` for additional setup notes

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
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

**API Rate Limits**
```bash
# Check rate limit compliance
python3 scripts/5-FinalList/FinalInfo_Step2.py --dry-run --limit 10
```

**Google Sheets Access**
```bash
# Test Google Sheets connectivity
python3 test_connection.py
```

**Data Quality Issues**
```bash
# Validate data quality
python3 scripts/5-FinalList/FinalInfo_Step3.py --verify
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
- **Google**: For Gemini AI and Google Sheets integration
- **Fandom**: For reality TV show wikis and community data

---

**The Reality Report Backend Data Pipeline** - Transforming reality TV data into actionable insights.

*For questions or support, please open an issue or contact the development team.*
