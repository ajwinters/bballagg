# Windows Task Scheduler Setup Guide

## Quick Setup for Daily NBA Data Collection

### Option 1: Simple Batch File Approach (Recommended)

1. **Use the provided batch file**:
   ```
   C:\Users\ajwin\Projects\Personal\NBA\thebigone\masters\run_daily_collection.bat
   ```

2. **Open Windows Task Scheduler**:
   - Press `Win + R`, type `taskschd.msc`, press Enter
   - Or search "Task Scheduler" in Start menu

3. **Create Basic Task**:
   - Click "Create Basic Task..." in right panel
   - Name: `NBA Data Collection`
   - Description: `Daily collection of NBA game data`
   - Click Next

4. **Set Trigger**:
   - Select "Daily"
   - Start date: Today
   - Start time: `7:00 AM` (recommended after games finish)
   - Recur every: `1` days
   - Click Next

5. **Set Action**:
   - Select "Start a program"
   - Program/script: Browse to `run_daily_collection.bat`
   - Or paste: `C:\Users\ajwin\Projects\Personal\NBA\thebigone\masters\run_daily_collection.bat`
   - Click Next

6. **Finish**:
   - Review settings
   - Check "Open Properties dialog when I click Finish"
   - Click Finish

7. **Configure Advanced Settings** (in Properties dialog):
   - General tab:
     - Check "Run whether user is logged on or not"
     - Check "Run with highest privileges"
   - Settings tab:
     - Check "Allow task to be run on demand"
     - Check "Run task as soon as possible after a scheduled start is missed"
     - If task fails, restart every: `5 minutes`
     - Attempt to restart up to: `3` times

### Option 2: Direct Python Script

If you prefer to run the Python script directly:

1. **Program/script**: `C:/Users/ajwin/Projects/Personal/NBA/thebigone/.venv/Scripts/python.exe`
2. **Arguments**: `daily_collection_clean.py`
3. **Start in**: `C:\Users\ajwin\Projects\Personal\NBA\thebigone\masters`

## What Gets Collected

### Daily (every run):
- **Recent Games**: Last 3 days for active leagues
- Currently collecting WNBA games (season is active)
- NBA and G-League will be skipped until their seasons start

### Weekly (Sundays only):
- **Players Data**: All players across all leagues
- Updates player information, stats, team affiliations

### Monthly (1st of each month):
- **Teams Data**: All teams across all leagues  
- Updates team information, rosters, standings

## Log Files

Check collection results in:
```
C:\Users\ajwin\Projects\Personal\NBA\thebigone\masters\logs\daily_collection_YYYYMMDD.log
```

## Testing the Setup

1. **Test manually**:
   - Right-click your task in Task Scheduler
   - Select "Run"
   - Check the log file for results

2. **Expected output** (August 2025):
   - ✅ WNBA games collected (season active)
   - ⏩ NBA games skipped (off-season)
   - ⏩ G-League games skipped (off-season)
   - ⏩ Players skipped (not Sunday)
   - ⏩ Teams skipped (not 1st of month)

## Troubleshooting

### Task Failed to Start
- Ensure the batch file path is correct
- Check "Run with highest privileges" is enabled
- Verify the virtual environment path in batch file

### No Data Collected
- Check the log file in `masters/logs/`
- Verify database connection in `config/db_config.py`
- Test manually: run the batch file directly

### Python Environment Issues
- Ensure the virtual environment is activated in batch file
- Check Python path: `C:/Users/ajwin/Projects/Personal/NBA/thebigone/.venv/Scripts/python.exe`

## Seasonal Schedule

### NBA Season (October - June):
- Daily games collection during regular season and playoffs
- Reduced activity during off-season

### WNBA Season (May - October):
- Currently active! Daily games being collected
- Peak activity during regular season

### G-League Season (November - March):
- Daily games collection during active season
- Currently off-season

## Monitoring

The system will automatically:
- ✅ Detect active seasons
- ⏩ Skip inactive leagues
- 🔄 Handle API rate limits
- 📝 Log all activities
- 🛡️ Retry failed operations

Your database will stay up-to-date with minimal manual intervention!
