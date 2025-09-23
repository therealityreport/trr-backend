# 1-ShowInfo Scripts

This folder contains the production-ready scripts for managing the ShowInfo sheet.

## Current Working Script

### `showinfo_step1.py`
- **Status**: ✅ PRODUCTION READY - ENHANCED
- **Purpose**: Complete ShowInfo sheet management with optimized IMDb extraction
- **Enhanced Features**:
  - **All 164 IMDb list items** extracted using JSON-LD structured data
  - **Single-request efficiency** - no pagination needed
  - **TMDb ID primary keys** in Column A for consistency
  - Fetches shows from TMDb list (8301263) and IMDb list (ls4106677119)
  - Preserves existing OVERRIDE data and manual flags
  - Adds new shows to bottom of sheet (append-only for safety)
  - Updates existing shows with missing data
  - Handles external IDs (TMDb, IMDb, TheTVDB, Wikidata)
  - Safe data preservation - never overwrites existing values

## Usage

```bash
cd "/Users/thomashulihan/Projects/TRR-Backend/scripts/1-ShowInfo"
python3 showinfo_step1.py
```

## Environment Requirements

The script requires these environment variables in `.env`:
- `GOOGLE_APPLICATION_CREDENTIALS`
- `SPREADSHEET_ID`
- `TMDB_API_KEY`
- `TMDB_BEARER`
- `TMDB_LIST_ID` (currently: 8301263)
- `IMDB_LIST_URL` (currently: https://www.imdb.com/list/ls4106677119/)
- `THETVDB_API_KEY`

## Output

The script updates the ShowInfo sheet with:
- **Column A**: Show ID (TMDb ID or imdb_ttxxxxxx)
- **Column B**: Show Name
- **Column C**: Network
- **Column D**: Total Seasons
- **Column E**: Total Episodes
- **Column F**: IMDb Series ID
- **Column G**: TMDb ID
- **Column H**: TheTVDB ID
- **Column I**: Most Recent Episode Date
- **Column J**: OVERRIDE (preserved from existing data)
- **Column K**: Wikidata ID

## Data Safety Features

- **Append-Only**: New shows are added to the bottom, existing data is never deleted
- **OVERRIDE Preservation**: Manually set OVERRIDE values (like "Y" for RHOA) are never touched
- **Backfill Only**: Missing data is filled in, but existing values are never overwritten
- **Targeted Updates**: Only refreshes "Most Recent Episode" dates for existing shows

## Last Updated
September 2025 - Enhanced with JSON-LD IMDb extraction, optimized for 164+ shows, TMDb ID primary keys
