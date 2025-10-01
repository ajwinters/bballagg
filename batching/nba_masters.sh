#!/bin/bash
# NBA Master Tables Job - Phase 1
# This job MUST complete before regular endpoints can start

#SBATCH --job-name=nba_masters
#SBATCH --output=logs/nba_masters_%j.out
#SBATCH --error=logs/nba_masters_%j.err
#SBATCH --time=02:00:00
#SBATCH --mem=4GB
#SBATCH --cpus-per-task=1

# Check if profile argument provided
if [ -z "$1" ]; then
    echo "Error: No profile specified"
    echo "Usage: sbatch $0 <profile_name>"
    exit 1
fi

PROFILE=$1
echo "Starting NBA MASTER TABLES collection with profile: $PROFILE"
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

# Determine test mode
if [ "$PROFILE" = "test" ]; then
    TEST_MODE_FLAG="--test-mode"
    MAX_ITEMS="--max-items 10"
    echo "🧪 MASTERS: Running in TEST MODE"
else
    TEST_MODE_FLAG=""
    MAX_ITEMS=""
    echo "🏭 MASTERS: Running in PRODUCTION MODE"
fi

echo ""
echo "🔧 PROCESSING FOUNDATION DATA (Master Tables)"
echo "=============================================="

# Run ONLY master endpoints
python src/nba_data_processor.py \
    $TEST_MODE_FLAG \
    $MAX_ITEMS \
    --log-level INFO \
    --masters-only

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ MASTER TABLES COMPLETED SUCCESSFULLY!"
    echo "Foundation data is ready for dependent endpoints"
    
    # Create a signal file that dependent jobs can check
    touch "logs/masters_complete_${SLURM_JOB_ID}.signal"
    
else
    echo ""
    echo "❌ MASTER TABLES FAILED!"
    echo "Dependent endpoints will NOT be able to run"
fi

echo "Masters job completion time: $(date)"
exit $EXIT_CODE