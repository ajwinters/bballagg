#!/bin/bash
# Single Endpoint Job Script
# This script processes ONE specific NBA endpoint
# Usage: sbatch --dependency=afterok:$MASTERS_JOB_ID single_endpoint.sh <profile> <endpoint_name>

#SBATCH --job-name=nba_endpoint
#SBATCH --output=logs/nba_%x_%j.out
#SBATCH --error=logs/nba_%x_%j.err
#SBATCH --time=02:00:00
#SBATCH --mem=4GB
#SBATCH --cpus-per-task=1

# Check if arguments provided
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Error: Missing arguments"
    echo "Usage: sbatch --dependency=afterok:<masters_job_id> $0 <profile> <endpoint_name>"
    exit 1
fi

PROFILE=$1
ENDPOINT_NAME=$2

echo "🏀 SINGLE ENDPOINT JOB"
echo "====================="
echo "Profile: $PROFILE"
echo "Endpoint: $ENDPOINT_NAME"
echo "Job ID: $SLURM_JOB_ID" 
echo "Date: $(date)"

# Update job name to include endpoint name (if scontrol is available)
if command -v scontrol &> /dev/null && [ -n "$SLURM_JOB_ID" ]; then
    scontrol update JobId=$SLURM_JOB_ID JobName="nba_${PROFILE}_${ENDPOINT_NAME}" 2>/dev/null || true
    echo "📝 Updated job name to: nba_${PROFILE}_${ENDPOINT_NAME}"
fi

# Navigate to project root (handle both batch/ subdir and direct execution)
if [ -d "../src" ]; then
    PROJECT_ROOT="$(cd .. && pwd)"
    cd "$PROJECT_ROOT"
else
    PROJECT_ROOT="$(pwd)"
fi

echo "🗂️  Project root: $PROJECT_ROOT"

# Activate virtual environment
VENV_ACTIVATE=""
if [ -f ".venv/bin/activate" ]; then
    VENV_ACTIVATE=".venv/bin/activate"
elif [ -f "venv/bin/activate" ]; then
    VENV_ACTIVATE="venv/bin/activate"
elif [ -f ".env/bin/activate" ]; then
    VENV_ACTIVATE=".env/bin/activate"
fi

if [ -n "$VENV_ACTIVATE" ]; then
    source "$VENV_ACTIVATE"
    echo "✅ Virtual environment activated: $VENV_ACTIVATE"
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

try:
    with db_manager.get_cursor() as cursor:
        # Get all tables for debugging
        cursor.execute('SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',))
        all_tables = [row[0] for row in cursor.fetchall()]
        print(f'📋 Total tables in database: {len(all_tables)}')
        
        # Check for standardized master tables
        cursor.execute('''
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND (tablename LIKE 'master_%_players'
                 OR tablename LIKE 'master_%_games'
                 OR tablename LIKE 'master_%_teams'
                 OR tablename LIKE '%commonallplayers%' 
                 OR tablename LIKE '%leaguegamefinder%' 
                 OR tablename LIKE '%leaguegamelog%')
            ORDER BY tablename
        ''')
        
        master_tables = cursor.fetchall()
        
        if len(master_tables) >= 1:
            print(f'✅ Found {len(master_tables)} potential master tables:')
            for table in master_tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table[0]}')
                count = cursor.fetchone()[0]
                print(f'   • {table[0]}: {count:,} records')
            
            # Check if we have data in master tables
            has_data = False
            for table in master_tables:
                cursor.execute(f'SELECT COUNT(*) FROM {table[0]}')
                if cursor.fetchone()[0] > 0:
                    has_data = True
                    break
            
            if has_data:
                print('✅ Master tables have data - ready to proceed')
                sys.exit(0)
            else:
                print('⚠️  Master tables exist but are empty - may still be populating')
                sys.exit(0)
        else:
            print(f'❌ No master tables found!')
            print('Available tables (first 10):')
            for i, table in enumerate(all_tables[:10]):
                print(f'   {i+1:2d}. {table}')
            if len(all_tables) > 10:
                print(f'   ... and {len(all_tables) - 10} more')
            print('Master tables job may have failed!')
            sys.exit(1)
            
except Exception as e:
    print(f'❌ Error checking master tables: {e}')
    sys.exit(1)
"

if [ $? -ne 0 ]; then
    echo "❌ Master tables verification failed - cannot proceed"
    exit 1
fi

# Determine test mode parameters
if [ "$PROFILE" = "test" ]; then
    TEST_MODE_FLAG="--test-mode"
    MAX_ITEMS="--max-items 5" 
    echo "🧪 Running in TEST MODE"
else
    TEST_MODE_FLAG=""
    MAX_ITEMS=""
    echo "🏭 Running in PRODUCTION MODE"
fi

echo ""
echo "🎯 PROCESSING SINGLE ENDPOINT: $ENDPOINT_NAME"
echo "=============================================="

# Run single endpoint with connection retry
python src/nba_data_processor.py \
    $TEST_MODE_FLAG \
    $MAX_ITEMS \
    --log-level INFO \
    --single-endpoint "$ENDPOINT_NAME" \
    --connection-timeout 120 \
    --retry-attempts 3

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ENDPOINT '$ENDPOINT_NAME' COMPLETED SUCCESSFULLY!" 
    
    # Report final stats for this endpoint
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

endpoint_name = '$ENDPOINT_NAME'.lower()

try:
    with db_manager.get_cursor() as cursor:
        cursor.execute('SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename LIKE %s', 
                      ('public', f'%{endpoint_name}%'))
        
        tables = cursor.fetchall()
        
        if tables:
            print(f'📊 ENDPOINT RESULTS FOR {endpoint_name.upper()}:')
            total_rows = 0
            for table in tables:
                table_name = table[0]
                cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
                row_count = cursor.fetchone()[0]
                total_rows += row_count
                print(f'   • {table_name}: {row_count:,} rows')
            print(f'   TOTAL: {total_rows:,} rows')
        else:
            print(f'❌ No tables found for endpoint {endpoint_name}')

except Exception as e:
    print(f'❌ Error getting endpoint stats: {e}')
"
else
    echo "❌ ENDPOINT '$ENDPOINT_NAME' FAILED!"
    echo "Check logs for connection timeout or other errors"
fi

echo "Endpoint job completion time: $(date)"
exit $EXIT_CODE