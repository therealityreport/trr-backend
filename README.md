# TRR Backend 2025

**The Reality Report** backend data pipeline for reality TV show and cast information.

## 🚀 Quick Start

1. **Setup Environment**
   ```bash
   # Clone and enter directory
   git clone https://github.com/therealityreport/trr-backend-2025.git
   cd trr-backend-2025
   
   # Install dependencies
   pip install -r logs/requirements.txt
   
   # Copy example environment file
   cp .env.example .env
   # Edit .env with your API keys and credentials
   ```

2. **Add Credentials**
   - Place your Google service account JSON in `keys/`
   - Set API keys in `.env`: `TMDB_API_KEY`, `IMDB_API_KEY`, `GEMINI_API_KEY`
   - Configure spreadsheet name in `.env`: `SPREADSHEET_NAME=Realitease2025Data`

## 📁 Pipeline Structure

The data pipeline is organized into 5 sequential steps:

### 1️⃣ ShowInfo (`scripts/1-ShowInfo/`)
Populates show metadata from TMDb and IMDb APIs.
```bash
cd scripts/1-ShowInfo
python3 showinfo_step1.py
```

### 2️⃣ CastInfo (`scripts/2-CastInfo/`)
Extracts and enriches cast member data for each show.
```bash
cd scripts/2-CastInfo
python3 CastInfo_Step1.py    # Update cast columns A-F
python3 CastInfo_Step2.py    # Extract seasons/episodes to G/H
```

### 3️⃣ RealiteaseInfo (`scripts/3-RealiteaseInfo/`)
Builds person-focused database with biographical enrichment.
```bash
cd scripts/3-RealiteaseInfo
python3 RealiteaseInfo_Step1.py   # Build base from CastInfo
python3 RealiteaseInfo_Step2.py   # Aggregate show counts/links
python3 RealiteaseInfo_Step3.py   # Enrich: gender, birthday, zodiac
python3 RealiteaseInfo_Step4.py   # Backfill TMDb cast IDs
```

### 4️⃣ WWHLInfo (`scripts/4-WWHLInfo/`)
Watch What Happens Live episode and guest data pipeline.
```bash
cd scripts/4-WWHLInfo
python3 WWHLInfo_TMDb_Step1.py    # Show data from TMDb
python3 WWHLInfo_IMDb_Step2.py    # Episode data from IMDb
python3 WWHLInfo_Gemini_Step3.py  # Fill gaps with Gemini AI
python3 WWHLInfo_Checker_Step4.py # Validate and clean data
```

### 5️⃣ FinalList (`scripts/5-FinalList/`)
Generates the final curated dataset for the Realitease platform.
```bash
cd scripts/5-FinalList
python3 FinalInfo_Step1.py   # Build final consolidated list
python3 FinalInfo_Step2.py   # Enrich with IMDb data + retries
python3 FinalInfo_Step3.py   # Normalize and clean final data
```

## 📊 Data Sources

- **TMDb**: Show metadata, cast information, person details
- **IMDb**: Episode data, additional cast information
- **Fandom Wikis**: Reality show-specific cast details
- **Famous Birthdays**: Biographical data
- **Gemini AI**: Text analysis and data gap filling

## 🔧 Configuration

### Environment Variables
```bash
# API Keys
TMDB_API_KEY=your_tmdb_api_key
IMDB_API_KEY=your_imdb_api_key  
GEMINI_API_KEY=your_gemini_api_key

# Google Sheets
SPREADSHEET_NAME=Realitease2025Data
GOOGLE_APPLICATION_CREDENTIALS=keys/trr-backend-df2c438612e1.json

# Optional
REALITEASE_TMDB_SHOW_LIMIT=5
GOOGLE_GEMINI_MODEL=gemini-2.5-flash
```

### Google Sheets Structure
The pipeline works with a Google Sheets workbook containing these tabs:
- **ShowInfo**: Show metadata and IDs
- **CastInfo**: Cast member details by show
- **RealiteaseInfo**: Person-focused aggregated data
- **WWHLinfo**: Watch What Happens Live episodes
- **FinalList**: Curated final dataset

## 📂 Additional Directories

- **`docs/`**: Documentation, setup guides, and images
- **`logs/`**: Pipeline execution logs and results
- **`.cache/`**: API response caches (Gemini, etc.)
- **`keys/`**: Google service account credentials _(gitignored)_
- **`scripts/archived_scripts/`**: Legacy/experimental scripts
- **`scripts/archives/`**: Additional utilities and tools

## 🛠️ Development

### Running Individual Steps
Each step can be run independently with various options:

```bash
# Dry run with limited rows
python3 RealiteaseInfo_Step3.py --dry-run --limit 100

# Process specific row range
python3 FinalInfo_Step2.py --start-row 1000 --end-row 2000

# Custom batch size and delays
python3 FinalInfo_Step2.py --batch-size 500 --delay 0.8
```

### Monitoring Progress
- Check `logs/` for execution logs and results
- Use `--dry-run` flag to preview changes before writing
- Most scripts support `--limit` to test with smaller datasets

### Caching
- Gemini responses cached in `.cache/` for efficiency
- TMDb API responses cached per-session to respect rate limits
- Use `--cache-file` to specify custom cache locations

## 📈 Pipeline Flow

```
TMDb/IMDb APIs → ShowInfo → CastInfo → RealiteaseInfo → WWHLInfo → FinalList
                     ↓          ↓           ↓            ↓          ↓
                Sheet Tab   Sheet Tab   Sheet Tab   Sheet Tab   Sheet Tab
```

Each step enriches and refines the data, culminating in a production-ready dataset for The Reality Report platform.

## 📝 Documentation

- **Setup Guides**: See `docs/cloud/` for deployment documentation
- **API Mapping**: See `docs/SHEET_EDIT_MAPPING.md` for column specifications  
- **Local Development**: See `docs/README_local.md` for additional setup notes

## 🔐 Security

- All API keys and credentials are gitignored
- Service account keys stored locally in `keys/` directory
- Environment variables used for sensitive configuration
- Cache files excluded from version control