# TRR-Backend-2025

Reality TV data extraction and processing scripts for Realitease 2025.

## Features

- **Cast Information Processing**: Automated IMDb scraping for episode counts and season information
- **Parallel Processing**: Multi-browser support for efficient data extraction  
- **Cloud-Ready**: Optimized for GitHub Codespaces with headless browser operation
- **Error Recovery**: Comprehensive error handling and failed item tracking
- **Google Sheets Integration**: Automatic batch updates to spreadsheets

## Quick Setup for GitHub Codespaces

### Prerequisites
- GitHub Pro account (for Codespaces access)
- Google Sheets API credentials

### Setup Steps

1. **Clone and Open Codespace**
   ```bash
   # Create Codespace from this repository
   # Click "Code" > "Codespaces" > "Create codespace on main"
   ```

2. **Install Dependencies**
   ```bash
   # Install Chrome and Python dependencies
   sudo apt update
   sudo apt install -y wget gnupg
   wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
   sudo sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
   sudo apt update
   sudo apt install -y google-chrome-stable python3-pip
   pip3 install selenium gspread
   ```

3. **Configure Service Account**
   ```bash
   # Upload your Google Sheets API JSON credentials to keys/
   # Or set environment variable: GSPREAD_SERVICE_ACCOUNT
   ```

4. **Run Cast Information Processor**
   ```bash
   cd scripts/2-CastInfo
   python3 v2UniversalSeasonExtractorCastInfo_Parallel_Full.py
   ```

## Scripts Overview

### CastInfo Processing
- `v2UniversalSeasonExtractorCastInfo_Parallel_Full.py` - Main parallel cast processor (8 browsers)
- Features: Headless operation, stale element recovery, failed cast member tracking

### Configuration
- **Browsers**: 8 parallel Chrome instances
- **Timeouts**: Optimized for cloud stability (8s cast member timeout)
- **Batch Size**: 50 Google Sheets updates per batch
- **Error Handling**: Comprehensive logging and recovery

## Performance
- Processes ~15,000+ cast members across 190+ shows
- Average rate: ~60+ cast members per minute
- Cloud deployment ready with 180 hours/month Codespaces quota

## Development

The project includes scripts for:
- Show information extraction
- Person details enhancement  
- Viable cast filtering
- Update information management
- WWHL (Watch What Happens Live) data processing
