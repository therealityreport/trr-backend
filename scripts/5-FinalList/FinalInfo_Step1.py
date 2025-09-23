#!/usr/bin/env python3
"""
FinalInfo Step 1 - FinalList Builder

Creates the final consolidated FinalList sheet with the following logic:
- Name, IMDbCastID, CorrectDate, AlternativeNames, IMDbSeriesIDs columns
- Include rows with 6+ total episodes of 1 show OR listed in WWHLInfo CastIDs
- Exclude rows missing data in columns D-G of RealiteaseInfo
- Consolidate duplicates with complex merging rules

Requirements:
1. Must have 6+ episodes of 1 show OR be in WWHLInfo
2. Must have complete data in RealiteaseInfo columns D-G (ShowNames, ShowIMDbIDs, ShowTMDbIDs, TotalShows)
3. Handle duplicate consolidation by name and IMDbCastID
"""

# Re-export by importing and running main() from the builder for consistency
from FinalList_Builder import main

if __name__ == "__main__":
    main()
