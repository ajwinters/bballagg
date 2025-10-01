#!/bin/bash
# NBA Regular Endpoints Job - Phase 2
# This job runs AFTER master tables are complete (using SLURM dependencies)

#SBATCH --job-name=nba_endpoints
#SBATCH --output=logs/nba_endpoints_%j.out
#SBATCH --error=logs/nba_endpoints_%j.err
#SBATCH --time=03:00:00
#SBATCH --mem=6GB
#SBATCH --cpus-per-task=2

# This job should be submitted with dependency on masters job:
# sbatch --dependency=afterok:$MASTERS_JOB_ID nba_endpoints.sh <profile>

# Check if profile argument provided
if [ -z "$1" ]; then
    echo "Error: No profile specified"
    echo "Usage: sbatch --dependency=afterok:<masters_job_id> $0 <profile_name>"
    exit 1
fi

PROFILE=$1
echo "Starting NBA REGULAR ENDPOINTS collection with profile: $PROFILE"
echo "Job ID: $SLURM_JOB_ID"
echo "Date: $(date)"

# Navigate to project root
PROJECT_ROOT="$(cd .. && pwd)"
cd "$PROJECT_ROOT"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated"
else
    echo "❌ Error: Virtual environment not found"
    exit 1
fi

# Verify master tables exist (safety check)
echo "🔍 Verifying master tables exist..."
python -c "
import sys
from pathlib import Path
import json

# Add project root to path
project_root = Path.cwd()
sys.path.insert(0, str(project_root))

from src.rds_connection_manager import RDSConnectionManager

# Load database configuration
config_path = project_root / 'config' / 'database_config.json'
with open(config_path, 'r') as f:
    db_config = json.load(f)

db_config_dict = {
    'host': db_config['host'],
    'database': db_config['name'], 
    'user': db_config['user'],
    'password': db_config['password'],
    'port': int(db_config['port']),
    'sslmode': db_config.get('ssl_mode', 'require'),
    'connect_timeout': 60
}

db_manager = RDSConnectionManager(db_config_dict)

try:
    with db_manager.get_cursor() as cursor:
        # Check for master tables
        cursor.execute('''
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND (tablename LIKE '%commonallplayers%' 
                 OR tablename LIKE '%leaguegamefinder%' 
                 OR tablename LIKE '%leaguegamelog%')
        ''')
        
        master_tables = cursor.fetchall()
        
        if len(master_tables) >= 2:  # Need at least players and games
            print(f'✅ Found {len(master_tables)} master tables - ready to proceed')
            for table in master_tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table[0]}')
                count = cursor.fetchone()[0]
                print(f'   • {table[0]}: {count:,} records')
            sys.exit(0)
        else:
            print(f'❌ Only found {len(master_tables)} master tables - need at least 2')
            print('Master tables job may have failed!')
            sys.exit(1)
            
except Exception as e:
    print(f'❌ Error checking master tables: {e}')
    sys.exit(1)
"

MASTERS_CHECK=$?
if [ $MASTERS_CHECK -ne 0 ]; then
    echo "❌ Master tables verification failed - cannot proceed"
    exit 1
fi

# Determine test mode
if [ "$PROFILE" = "test" ]; then
    TEST_MODE_FLAG="--test-mode"
    MAX_ITEMS="--max-items 5"
    echo "🧪 ENDPOINTS: Running in TEST MODE"
else
    TEST_MODE_FLAG=""
    MAX_ITEMS=""
    echo "🏭 ENDPOINTS: Running in PRODUCTION MODE"
fi

echo ""
echo "🏀 PROCESSING GAME DATA (Regular Endpoints)"
echo "==========================================="

# Run regular endpoints (which will use master tables for dependencies)
python src/nba_data_processor.py \
    $TEST_MODE_FLAG \
    $MAX_ITEMS \
    --log-level INFO \
    --run-full

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ REGULAR ENDPOINTS COMPLETED SUCCESSFULLY!"
    
    # Generate final summary
    python -c "
import sys
from pathlib import Path
import json

project_root = Path.cwd()
sys.path.insert(0, str(project_root))

from src.rds_connection_manager import RDSConnectionManager

config_path = project_root / 'config' / 'database_config.json'
with open(config_path, 'r') as f:
    db_config = json.load(f)

db_config_dict = {
    'host': db_config['host'],
    'database': db_config['name'],
    'user': db_config['user'], 
    'password': db_config['password'],
    'port': int(db_config['port']),
    'sslmode': db_config.get('ssl_mode', 'require'),
    'connect_timeout': 60
}

db_manager = RDSConnectionManager(db_config_dict)

print()
print('🏁 FINAL NBA DATA COLLECTION SUMMARY')
print('=' * 50)

try:
    with db_manager.get_cursor() as cursor:
        cursor.execute('SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',))
        
        table_names = [row[0] for row in cursor.fetchall()]
        
        master_count = 0
        endpoint_count = 0
        total_rows = 0
        
        for table_name in table_names:
            cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
            row_count = cursor.fetchone()[0]
            total_rows += row_count
            
            if any(table_name.endswith(suffix) for suffix in ['_commonallplayers', '_leaguegamelog', '_leaguegamefinderresults']):
                master_count += 1
                print(f'📊 MASTER: {table_name:<45} {row_count:>8,} rows')
            else:
                endpoint_count += 1  
                print(f'🏀 ENDPOINT: {table_name:<43} {row_count:>8,} rows')
        
        print('=' * 50)
        print(f'MASTER TABLES: {master_count}')
        print(f'ENDPOINT TABLES: {endpoint_count}')
        print(f'TOTAL ROWS: {total_rows:,}')
        print('✅ NBA DATA COLLECTION COMPLETE!')

except Exception as e:
    print(f'❌ Error generating summary: {e}')
"
else
    echo ""
    echo "❌ REGULAR ENDPOINTS FAILED!"
fi

echo "Endpoints job completion time: $(date)"
exit $EXIT_CODE