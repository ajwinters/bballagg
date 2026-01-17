#!/bin/bash
# NBA Data Collection Batch Workflow for HPC
# This script handles the proper sequencing of master tables and regular endpoints

#SBATCH --job-name=nba_workflow
#SBATCH --output=logs/nba_workflow_%j.out
#SBATCH --error=logs/nba_workflow_%j.err
#SBATCH --time=04:00:00
#SBATCH --mem=8GB
#SBATCH --cpus-per-task=2

# Check if profile argument provided
if [ -z "$1" ]; then
    echo "Error: No profile specified"
    echo "Usage: sbatch $0 <profile_name>"
    echo "Available profiles: test, high_priority, full"
    exit 1
fi

PROFILE=$1
echo "Starting NBA data collection workflow with profile: $PROFILE"
echo "Job ID: $SLURM_JOB_ID"
echo "Date: $(date)"

# Navigate to project root
PROJECT_ROOT="/storage/home/ajw5296/work/thebigone"
cd "$PROJECT_ROOT"
echo "Working directory: $(pwd)"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated"
else
    echo "❌ Error: Virtual environment not found"
    exit 1
fi

# Verify environment
echo "Python version: $(python --version)"
echo "NBA data processor: $(ls -la src/nba_data_processor.py)"

# Determine test mode based on profile
if [ "$PROFILE" = "test" ]; then
    TEST_MODE_FLAG="--test-mode"
    MAX_ITEMS="--max-items 5"
    echo "🧪 Running in TEST MODE with limited data"
else
    TEST_MODE_FLAG=""
    MAX_ITEMS=""
    echo "🏭 Running in PRODUCTION MODE with full data"
fi

echo ""
echo "========================================"
echo "PHASE 1: MASTER TABLES (Foundation Data)"
echo "========================================"

# Run master endpoints first - these create the foundation tables
echo "Step 1a: Processing Master Endpoints..."
python src/nba_data_processor.py \
    $TEST_MODE_FLAG \
    $MAX_ITEMS \
    --log-level INFO \
    --masters-only

MASTER_EXIT_CODE=$?
if [ $MASTER_EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: Master endpoints failed with exit code $MASTER_EXIT_CODE"
    exit $MASTER_EXIT_CODE
fi

echo "✅ Master endpoints completed successfully"
echo ""

echo "========================================"
echo "PHASE 2: REGULAR ENDPOINTS (Game Data)"  
echo "========================================"

# Now run regular endpoints that depend on master tables
echo "Step 2a: Processing Regular Endpoints..."
python src/nba_data_processor.py \
    $TEST_MODE_FLAG \
    $MAX_ITEMS \
    --log-level INFO \
    --run-full

REGULAR_EXIT_CODE=$?
if [ $REGULAR_EXIT_CODE -ne 0 ]; then
    echo "❌ ERROR: Regular endpoints failed with exit code $REGULAR_EXIT_CODE"
    exit $REGULAR_EXIT_CODE
fi

echo "✅ Regular endpoints completed successfully"
echo ""

echo "========================================"
echo "PHASE 3: VALIDATION & SUMMARY"
echo "========================================"

# Get final statistics
echo "Step 3a: Collecting final statistics..."
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

# Create database connection
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

print('FINAL NBA DATA COLLECTION SUMMARY')
print('=' * 50)

try:
    with db_manager.get_cursor() as cursor:
        cursor.execute('SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename', ('public',))
        
        table_names = [row[0] for row in cursor.fetchall()]
        
        print(f'TOTAL TABLES CREATED: {len(table_names)}')
        print('-' * 50)
        
        master_count = 0
        endpoint_count = 0
        total_rows = 0
        
        for table_name in table_names:
            cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
            row_count = cursor.fetchone()[0]
            total_rows += row_count
            
            if any(table_name.endswith(suffix) for suffix in ['_commonallplayers', '_leaguegamelog', '_leaguegamefinderresults']):
                master_count += 1
                print(f'  📊 MASTER: {table_name:<45} {row_count:>8,} rows')
            else:
                endpoint_count += 1
                print(f'  🏀 ENDPOINT: {table_name:<43} {row_count:>8,} rows')
        
        print('-' * 50)
        print(f'MASTER TABLES: {master_count}')
        print(f'ENDPOINT TABLES: {endpoint_count}')
        print(f'TOTAL ROWS: {total_rows:,}')
        print('✅ NBA DATA COLLECTION WORKFLOW COMPLETE!')

except Exception as e:
    print(f'❌ Error generating summary: {e}')
"

echo ""
echo "🏁 NBA Data Collection Workflow Completed Successfully!"
echo "Profile: $PROFILE"
echo "Job ID: $SLURM_JOB_ID" 
echo "Completion time: $(date)"

exit 0