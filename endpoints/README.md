# Endpoints System - NBA API Processing

## Purpose
Processes specific NBA API endpoints using master data as reference for systematic data collection.

## Key Features
- Uses master data for reference
- League-specific processing
- Systematic endpoint testing
- Production data collection

## Quick Start
```python
from collectors.comprehensive_collector import FinalDataCollector

collector = FinalDataCollector()
results = collector.collect_with_all_fixes()
```

## Data Flow
1. Uses master data from `../masters/data/`
2. Processes NBA API endpoints systematically
3. Saves results to `data/` and `results/`

## Testing
- Run `tests/systematic_tester.py` for endpoint validation
- Run `tests/endpoint_tests.py` for specific endpoint testing
