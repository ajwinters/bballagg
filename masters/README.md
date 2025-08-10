# Masters System - NBA Data Collection

## Purpose
Collects and organizes fundamental NBA data across multiple leagues with proper formatting.

## Key Features
- Multi-league support (NBA, WNBA, G-League)
- Proper season formatting per league
- Automatic league separation
- Data integrity validation

## Quick Start
```python
from collectors.league_separated_collection import LeagueSeparatedMasterCollector

collector = LeagueSeparatedMasterCollector()
results = collector.run_league_separated_collection(test_mode=True)
```

## Data Structure
- `data/comprehensive/` - All leagues combined
- `data/leagues/` - League-separated tables

## Testing
- Run `tests/multi_league_test.py` for comprehensive validation
- Run `tests/season_formats_test.py` to validate season formatting
