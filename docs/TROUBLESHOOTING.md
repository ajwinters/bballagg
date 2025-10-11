# NBA Data Collection System - Troubleshooting Guide

## 🚨 **Common Issues & Solutions**

### **1. Column Name Mapping Failures**

**Symptoms**:
- Jobs failing with "column not found" errors
- Database insert failures 
- KeyError exceptions for `game_id`, `player_id`, or `team_id`

**Root Cause**: NBA API uses inconsistent column names across endpoints

**Solution**: Dynamic column mapping system handles this automatically
```bash
# Test column mapping
python tests/test_column_mapping.py

# Verify master table columns
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    config = json.load(f)
db = RDSConnectionManager(config)
columns = db.get_table_columns('master_nba_games')
print('Master games columns:', columns)
"
```

**Prevention**: System automatically detects `gameid`/`game_id`, `personid`/`player_id`, `teamid`/`team_id` variations

---

### **2. Missing Master Table Dependencies**

**Symptoms**:
- Endpoints requiring player/team/game IDs fail
- "Master table not found" errors
- Empty parameter sets for dependent endpoints

**Root Cause**: Master tables not created before dependent endpoints run

**Solution**: Ensure master endpoints run first
```bash
# Check master table status
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    db_config = json.load(f)
db = RDSConnectionManager(db_config)
masters = ['master_nba_games', 'master_nba_players', 'master_nba_teams']
for table in masters:
    exists = db.table_exists(table)
    count = db.execute_query(f'SELECT COUNT(*) FROM {table}')[0][0] if exists else 0
    print(f'{table}: {\"✅\" if exists else \"❌\"} ({count} rows)')
"

# Manually run master endpoints if needed
python src/nba_data_processor.py --endpoint LeagueGameFinder --test-mode
python src/nba_data_processor.py --endpoint CommonAllPlayers --test-mode  
python src/nba_data_processor.py --endpoint CommonTeamYears --test-mode
```

**Prevention**: Use distributed job submission which handles dependencies automatically

---

### **3. SLURM Job Failures**

**Symptoms**:
- Jobs stuck in PENDING state
- OUT_OF_MEMORY errors
- TIME_LIMIT exceeded
- Job arrays not submitting

**Common Solutions**:

**Memory Issues**:
```bash
# Increase memory allocation
#SBATCH --mem=8GB  # Instead of 4GB

# Or increase per-CPU memory
#SBATCH --mem-per-cpu=4GB
```

**Time Limit Issues**:
```bash
# Increase time limit for comprehensive runs
#SBATCH --time=48:00:00  # 48 hours for full datasets

# Or reduce scope for testing
python src/nba_data_processor.py --endpoint PlayerGameLogs --test-mode --max-items 10
```

**Job Submission Issues**:
```bash
# Check SLURM status
sinfo
squeue -u $(whoami)

# Verify job script syntax
bash -n batching/single_endpoint.sh

# Test job submission
sbatch --test-only batching/single_endpoint.sh test PlayerGameLogs
```

---

### **4. Database Connection Issues**

**Symptoms**:
- "Connection refused" errors
- "Authentication failed" messages
- Timeouts during data insertion

**Solutions**:

**Connection Test**:
```bash
# Test database connectivity
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    config = json.load(f)
try:
    db = RDSConnectionManager(config)
    print('✅ Database connection successful')
except Exception as e:
    print(f'❌ Connection failed: {e}')
"
```

**Common Fixes**:
1. **Credentials**: Check `DB_PASSWORD` environment variable
2. **Network**: Verify VPN/network access to RDS
3. **Security Groups**: Ensure RDS allows connections from compute nodes
4. **SSL**: Check SSL requirements in connection string

**Environment Setup**:
```bash
# Set database password
export DB_PASSWORD="your_actual_password"

# Verify environment
echo $DB_PASSWORD
```

---

### **5. API Rate Limiting Issues**

**Symptoms**:
- "Too Many Requests" (429) errors
- Slow processing speed
- API call failures

**Solutions**:

**Adjust Rate Limiting**:
```json
// In config/run_config.json
"slurm_config": {
  "rate_limit": 0.3  // Increase from 0.2 to slow down requests
}
```

**Monitor API Usage**:
```bash
# Check recent API calls in logs
grep -i "rate limit\|429\|too many" logs/nba_*.out

# Adjust rate limiting based on failures
```

**Performance Tuning**:
- **Conservative**: 0.5 seconds (120 calls/minute)
- **Balanced**: 0.2 seconds (300 calls/minute)
- **Aggressive**: 0.1 seconds (600 calls/minute)

---

### **6. Endpoint Version Conflicts**

**Symptoms**:
- Duplicate data from V2 and V3 endpoints
- Inconsistent data structures
- Longer processing times

**Solution**: Latest version filtering (automatically enabled)
```bash
# Verify version filtering is working
python batching/scripts/analyze_versions.py

# Check what endpoints will be processed
python batching/scripts/get_endpoints.py high_priority

# Should only show V3 endpoints, not V2 duplicates
```

**Manual Override** (if needed):
```json
// In config/run_config.json - only if you need old versions
"legacy_profile": {
  "filter": "priority:high",
  "latest_only": false  // Includes both V2 and V3
}
```

---

### **7. Parameter Generation Issues**

**Symptoms**:
- "No parameter combinations found"
- Endpoints processing only current season
- Missing historical data

**Solution**: Verify comprehensive parameter system
```bash
# Test parameter generation
python tests/test_comprehensive_params.py

# Check season type coverage
python tests/test_season_types.py

# Verify all 6 season types are included
python -c "
import sys
sys.path.append('src')
from nba_data_processor import NBADataProcessor
processor = NBADataProcessor()
season_types = processor._get_all_season_types()
print(f'Season types ({len(season_types)}): {season_types}')
assert 'IST' in season_types, 'IST missing!'
print('✅ All season types present')
"
```

---

### **8. Job Naming and Monitoring Issues**

**Symptoms**:
- Can't identify which endpoint is running
- Generic job names in queue
- Difficult to track job progress

**Solution**: Enhanced job naming (automatically enabled)
```bash
# Verify job naming is working
squeue -u $(whoami) --format="%.10i %.15P %.30j %.8u %.8T %.10M"

# Should show names like: nba_high_priority_PlayerGameLog

# Monitor specific endpoints
squeue -u $(whoami) | grep PlayerGameLog

# Cancel specific endpoints
scancel -u $(whoami) -n "*PlayerGameLog*"
```

---

## 🔧 **Diagnostic Commands**

### **System Health Check**
```bash
# Complete system validation
echo "=== NBA Data Collection System Health Check ==="

# 1. Database connectivity
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    config = json.load(f)
try:
    db = RDSConnectionManager(config)
    print('✅ Database: Connected')
except Exception as e:
    print(f'❌ Database: {e}')
"

# 2. Master tables
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    config = json.load(f)
db = RDSConnectionManager(config)
for table in ['master_nba_games', 'master_nba_players', 'master_nba_teams']:
    exists = db.table_exists(table)
    print(f'{'✅' if exists else '❌'} Master Table: {table}')
"

# 3. Configuration validation
python batching/scripts/analyze_versions.py

# 4. Parameter system
python tests/test_comprehensive_params.py

echo "=== Health Check Complete ==="
```

### **Quick Test Run**
```bash
# Test single endpoint locally
python src/nba_data_processor.py --endpoint BoxScoreAdvancedV3 --test-mode --max-items 1 --log-level DEBUG

# Check if it completes successfully
echo "Exit code: $?"
```

### **Log Analysis**
```bash
# Find recent errors
find logs/ -name "nba_*.out" -mtime -1 -exec grep -l -i "error\|failed\|exception" {} \;

# Show recent job completions
find logs/ -name "nba_*.out" -mtime -1 -exec grep -l "completed" {} \;

# Monitor active job logs
tail -f logs/nba_*_$(date +%Y%m%d)*.out
```

---

## 📋 **Recovery Procedures**

### **Complete System Reset**
```bash
# 1. Cancel all jobs
scancel -u $(whoami) -n "*nba*"

# 2. Clean database (if needed)
python database_cleanup.py --cleanup --confirm

# 3. Test single endpoint
python src/nba_data_processor.py --endpoint CommonAllPlayers --test-mode --max-items 5

# 4. Run master tables
sbatch batching/nba_masters.sh high_priority

# 5. Submit distributed jobs
./batching/submit_distributed_nba_jobs.sh high_priority
```

### **Partial Recovery**
```bash
# 1. Identify failed endpoints
squeue -u $(whoami) -t FAILED

# 2. Resubmit individual endpoints
sbatch batching/single_endpoint.sh high_priority PlayerGameLogs

# 3. Monitor recovery
squeue -u $(whoami) | grep PlayerGameLog
```

This troubleshooting guide covers the most common issues encountered in the NBA data collection system and provides step-by-step solutions for quick resolution.