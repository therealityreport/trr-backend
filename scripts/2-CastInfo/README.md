# CastInfo Scripts

This folder contains the current CastInfo processing scripts for the TRR-Backend project.

## Current Scripts in This Folder

### Main Processing Scripts
- **`CastInfo_Step1.py`** - Enhanced CastInfo updater (adds/cleans A–F; thresholds, SKIP removal)
- **`CastInfo_Step2.py`** - Latest parallel season/episode extractor (fills G/H per show, v7 combined)
- **`CastInfo_ArchiveStep.py`** - Previous extractor (v6 optimized page reuse)

### Key Features of v6/v7
- Multi-threaded processing with 4-6 browsers
- Show-based batching for efficient Google Sheets updates  
- Precise XPath selectors for modern IMDb structure
- Automatic fallback to legacy selectors for compatibility
- Enhanced error handling and session recovery
- "**" markers for missing data (batched, not individual writes)
- Direct cast section navigation with anchor URLs
- Reality show contestant detection capabilities

## Historical Versions
- **v2** versions are preserved in `../archived_scripts/` and `../archives/`
- **v3-v5** versions have been removed as they're superseded by v6/v7

## Usage
Run from the CastInfo directory:
```bash
cd scripts/2-CastInfo
# Step 1: Update/add cast A–F
python3 CastInfo_Step1.py --dry-run   # preview
python3 CastInfo_Step1.py             # apply

# Step 2: Extract episodes/seasons (G/H)
python3 CastInfo_Step2.py
```

## Sheet Purpose
The CastInfo sheet contains detailed cast member information including:
- Cast names and IMDb IDs
- Show associations and IMDb IDs
- Episode counts (Column G) 
- Season counts (Column H)
- Cast member roles and show participation data

## Data Flow
The CastInfo scripts extract episode and season data from IMDb and update the Google Sheets CastInfo tab. This data serves as a source for other processing scripts in the project.
