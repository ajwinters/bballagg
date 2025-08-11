# NBA Master Tables Database System

This system automatically collects and maintains NBA, WNBA, and G-League data in PostgreSQL master tables with different update schedules.

## 📊 Database Schema

The system creates and maintains 9 master tables:

### Games Tables (Updated Daily)
- `master_games_nba` - NBA games data
- `master_games_wnba` - WNBA games data  
- `master_games_g-league` - G-League games data

### Players Tables (Updated Weekly)
- `master_players_nba` - NBA players data
- `master_players_wnba` - WNBA players data
- `master_players_g-league` - G-League players data

### Teams Tables (Updated Yearly)
- `master_teams_nba` - NBA teams data
- `master_teams_wnba` - WNBA teams data
- `master_teams_g-league` - G-League teams data

## 🔄 Update Schedules

- **Games**: Daily at 6:00 AM (captures previous day's games)
- **Players**: Weekly on Sunday at 2:00 AM (roster changes)  
- **Teams**: Yearly on October 1st at 1:00 AM (new season setup)

## 🚀 Getting Started

### 1. Initial Setup

```bash
# Navigate to the masters directory
cd c:\Users\ajwin\Projects\thebigone\masters

# Run initial backfill (test mode first)
python database_manager.py
# Choose option 1: Run full backfill (test mode)

# After testing, run production backfill
python database_manager.py  
# Choose option 2: Run full backfill (production mode)
```

### 2. Manual Operations

#### Database Manager (database_manager.py)
```bash
python database_manager.py
```

Interactive menu options:
1. 🚀 Run full backfill (test mode) - Limited data for testing
2. 🏭 Run full backfill (production mode) - Complete historical data
3. 🎯 Update games only (daily) - Force games update
4. 👥 Update players only (weekly) - Force players update
5. 🏟️ Update teams only (yearly) - Force teams update
6. 📋 Show database summary - View table statistics
7. 🚪 Exit

#### Scheduler (scheduler.py)
```bash
python scheduler.py
```

Interactive menu options:
1. Run scheduled check - Check and run due updates
2. Force games update - Manual games update
3. Force players update - Manual players update
4. Force teams update - Manual teams update
5. Show status - View last run times and schedules
6. Show Windows Task Scheduler setup - Get automation commands
7. Exit

### 3. Command Line Usage

```bash
# Check and run scheduled updates
python scheduler.py --run

# Force specific updates
python scheduler.py --games
python scheduler.py --players  
python scheduler.py --teams

# Check status
python scheduler.py --status

# Get Windows Task Scheduler commands
python scheduler.py --setup-windows
```

## ⚙️ Automated Scheduling

### Windows Task Scheduler Setup

1. Open Command Prompt as Administrator
2. Run the setup command to get the task creation commands:
   ```bash
   python scheduler.py --setup-windows
   ```
3. Execute the displayed commands to create scheduled tasks

### Example Task Setup
```cmd
# Daily check task (runs at 6:00 AM daily)
schtasks /create /tn "NBA_Masters_Daily" /tr "C:\Python\python.exe C:\Users\ajwin\Projects\thebigone\masters\scheduler.py --run" /sc daily /st 06:00 /f
```

## 📁 File Structure

```
masters/
├── database_manager.py      # Core database operations
├── scheduler.py            # Automated scheduling system
├── scheduler_config.json   # Scheduler configuration
├── masters_README.md       # This file
└── logs/                  # Execution logs
    └── scheduler_YYYYMMDD.log
```

## 🛠️ Configuration

Edit `scheduler_config.json` to customize:

- **Schedules**: Change update frequencies and times
- **Database**: Connection timeouts, retry attempts
- **Collection**: Test mode, season limits, data types
- **Notifications**: Email alerts, Slack webhooks
- **Monitoring**: Runtime limits, resource thresholds

## 📊 Key Features

### Incremental Updates
- Only collects new data since last successful run
- Uses timestamp tracking to avoid duplicate work
- Handles API rate limits with intelligent delays

### Database Schema Management  
- Automatic table creation with proper indexes
- Column type mapping from pandas to PostgreSQL
- UPSERT operations to handle duplicate records
- Foreign key relationships and constraints

### Error Handling
- Comprehensive retry logic for API failures
- Database connection recovery
- Detailed logging for troubleshooting
- Graceful degradation on partial failures

### Monitoring
- Execution logs with timestamps
- Database table statistics
- Last successful run tracking
- Performance metrics

## 🏀 Data Sources

All data collected from NBA API (`nba_api` package):

- **Games**: `LeagueGameFinder` endpoint with league separation
- **Players**: `LeagueDashPlayerBioStats` for comprehensive player data
- **Teams**: Static teams data with league assignments

### Supported Leagues
- **NBA** (League ID: 00) - Since 1946
- **WNBA** (League ID: 10) - Since 1997  
- **G-League** (League ID: 20) - Since 2001

### Season Types
- Regular Season
- Playoffs
- Pre Season
- In-Season Tournament (IST)

## 🔍 Monitoring and Logs

### Log Files
- Location: `masters/logs/`
- Format: `scheduler_YYYYMMDD.log`
- Rotation: Daily files
- Retention: 30 days (configurable)

### Log Contents
- Start/end times for each process
- Record counts processed
- API response times
- Database operation results
- Error details and stack traces

### Database Summary
```bash
python database_manager.py
# Choose option 6: Show database summary
```

Shows for each table:
- Total record count
- Last update timestamp
- Table size and indexes

## ⚠️ Important Notes

### API Rate Limits
- NBA API has rate limits (~600ms between requests)
- System includes automatic delays
- Full backfill can take several hours

### Database Resources
- Master tables can grow large (millions of records)
- Ensure sufficient disk space
- Monitor connection pool usage

### Scheduling Considerations
- Games update: Run after games typically finish (6 AM)
- Players update: Weekend timing for roster stability
- Teams update: October aligns with NBA season start

## 🆘 Troubleshooting

### Common Issues

1. **Database Connection Failures**
   - Check RDS instance availability
   - Verify network connectivity
   - Confirm credentials in database_manager.py

2. **API Rate Limit Errors**
   - Increase delays in configuration
   - Run updates during off-peak hours
   - Use test mode for development

3. **Memory Issues**
   - Process smaller date ranges
   - Increase system memory
   - Monitor pandas DataFrame sizes

### Debug Steps
1. Check recent logs in `logs/` directory
2. Run database summary to verify table states
3. Test individual processes manually
4. Verify API connectivity with test mode

## 📈 Performance Tips

1. **Optimize Update Frequency**
   - Games: Daily is optimal for current data
   - Players: Weekly captures roster changes
   - Teams: Yearly is sufficient (teams rarely change)

2. **Database Performance**
   - Indexes are created automatically
   - Consider partitioning for large tables
   - Regular VACUUM and ANALYZE operations

3. **System Resources**
   - Run during off-peak hours
   - Monitor disk space growth
   - Set memory limits in configuration

## 🔄 Maintenance

### Regular Tasks
- Review logs for errors
- Monitor disk space usage
- Verify scheduled tasks are running
- Update configuration as needed

### Seasonal Tasks
- October: Verify team updates for new season
- June: Check playoff data collection
- Year-end: Archive old logs

### System Updates
- Keep nba_api package updated
- Monitor PostgreSQL version compatibility
- Update Python dependencies regularly

---

## 🎯 Quick Start Checklist

- [ ] Verify database connectivity
- [ ] Run test mode backfill
- [ ] Check table creation successful
- [ ] Set up Windows scheduled tasks
- [ ] Monitor first few automated runs
- [ ] Configure notifications if needed

The system is designed to be hands-off once properly configured. Daily monitoring of logs is recommended for the first week, then weekly reviews should be sufficient.
