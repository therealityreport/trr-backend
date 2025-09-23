# WWHLinfo Sheet Scripts

This folder contains scripts that work with the **WWHLinfo** sheet in the Realitease2025Data workbook.

## Working Scripts (September 3, 2025)

### WWHL Data Extraction
- **`fetch_WWHL_info.py`** - Watch What Happens Live episode and guest data extractor
  - Uses TMDb API to fetch WWHL episode information
  - Extracts guest appearances and episode details
  - Creates and manages WWHLinfo sheet
  - Sources data from ViableCast sheet

## Sheet Purpose
The WWHLinfo sheet contains Watch What Happens Live specific data including:
- Episode information
- Guest appearances
- Air dates and episode numbers
- Host and guest details

## Data Source
Reads from ViableCast sheet to identify WWHL-related cast members and builds detailed episode information.
