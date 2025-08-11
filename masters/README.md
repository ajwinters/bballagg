<<<<<<< HEAD
# NBA Data Collection System

Simple, automated NBA data collection using Windows Task Scheduler.

## Files Overview

### Core Collection System
- **`daily_collection_clean.py`** - Main collection script (run this daily)
- **`run_daily_collection.bat`** - Windows batch wrapper for Task Scheduler
- **`TASK_SCHEDULER_SETUP.md`** - Complete setup instructions

### Data Collectors
- **`games_collector.py`** - Handles game data collection
- **`players_collector.py`** - Handles player data collection  
- **`teams_collector.py`** - Handles team data collection
- **`database_manager.py`** - Database connection and table management

### Configuration & Logs
- **`config/`** - Database connection settings
- **`logs/`** - Daily collection logs (auto-created)

## Quick Start

1. **Setup**: Follow instructions in `TASK_SCHEDULER_SETUP.md`
2. **Schedule**: Set Windows Task Scheduler to run `run_daily_collection.bat` daily at 7 AM
3. **Monitor**: Check logs in `logs/` directory

## What Gets Collected

- **Daily**: Recent games for active leagues (currently WNBA)
- **Weekly** (Sundays): Player data updates
- **Monthly** (1st): Team data updates

The system automatically detects which leagues are active and collects accordingly.

## Current Status (August 2025)
- ✅ WNBA season active - collecting daily games
- ⏸️ NBA season inactive - will resume in October
- ⏸️ G-League season inactive - will resume in November
=======
>>>>>>> 19d8ffac5b5d40dbee755e1410861659a1445302
