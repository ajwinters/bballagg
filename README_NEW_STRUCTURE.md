# NBA Data Collection System - Reorganized Structure

## New Project Structure

This project has been reorganized to separate master data collection from endpoint processing:

### Masters System - Master Data Collection
- **Purpose**: Collect and organize fundamental NBA data (games, players, teams, seasons)
- **Key Features**: Multi-league support (NBA, WNBA, G-League), proper season formatting
- **Entry Point**: `masters/collectors/league_separated_collection.py`

### Endpoints System - Endpoint Data Processing
- **Purpose**: Process specific NBA API endpoints using master data as reference
- **Key Features**: Systematic endpoint testing, production data collection
- **Entry Point**: `endpoints/collectors/comprehensive_collector.py`

### Shared System - Shared Utilities
- **Purpose**: Common utilities and configurations used by both systems
- **Contents**: Database utilities, shared scripts, project configuration

### Archive - Original Structure Backup
- **Purpose**: Backup of original file structure for reference/rollback
- **Contents**: Complete copy of original src/, data/, config/ directories

## Getting Started

1. **Master Data Collection**: Start with `masters/collectors/league_separated_collection.py`
2. **Endpoint Processing**: Use `endpoints/collectors/comprehensive_collector.py`
3. **Testing**: Run tests in respective `/tests/` directories

## Directory Details

Each system directory contains:
- `collectors/` - Main collection scripts
- `config/` - Configuration files  
- `data/` - Data files (masters) or results (endpoints)
- `tests/` - Test scripts
- `scripts/` - Utility scripts
