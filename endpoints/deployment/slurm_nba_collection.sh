#!/bin/bash
# Simplified NBA Data Collection SLURM Script
# Usage: sbatch slurm_nba_collection.sh <profile_name>
# Profiles: test, high_priority, full

#SBATCH --job-name=nba_collection
#SBATCH --output=logs/nba_%x_%A_%a.out
#SBATCH --error=logs/nba_%x_%A_%a.err
##SBATCH --mail-type=FAIL
##SBATCH --mail-user=ajw5296@psu.edu

# Check if profile argument provided
if [ -z "$1" ]; then
    echo "Error: No profile specified"
    echo "Usage: sbatch $0 <profile_name>"
    echo "Available profiles: test, high_priority, full"
    exit 1
fi

PROFILE=$1
echo "Starting NBA data collection with profile: $PROFILE"

# Get endpoints for this profile
ENDPOINTS_FILE="/tmp/nba_endpoints_${SLURM_JOB_ID}.txt"
python scripts/get_endpoints.py $PROFILE > $ENDPOINTS_FILE

if [ ! -s "$ENDPOINTS_FILE" ]; then
    echo "Error: No endpoints found for profile $PROFILE"
    exit 1
fi

# Count endpoints and set up array job
ENDPOINT_COUNT=$(wc -l < $ENDPOINTS_FILE)
echo "Found $ENDPOINT_COUNT endpoints to process"

# Load configuration for this profile
CONFIG_JSON=$(python -c "
import json
import sys
with open('config/run_config.json', 'r') as f:
    config = json.load(f)
profile = config['collection_profiles']['$PROFILE']
slurm = profile['slurm_config']
print(f'{slurm[\"time\"]}|{slurm[\"mem_per_cpu\"]}|{slurm[\"rate_limit\"]}')
")

IFS='|' read -r TIME_LIMIT MEM_PER_CPU RATE_LIMIT <<< "$CONFIG_JSON"

echo "SLURM Configuration:"
echo "  Time limit: $TIME_LIMIT"  
echo "  Memory per CPU: $MEM_PER_CPU"
echo "  Rate limit: $RATE_LIMIT"

# If this is the first task, resubmit as array job
if [ -z "$SLURM_ARRAY_TASK_ID" ]; then
    echo "Resubmitting as array job with $ENDPOINT_COUNT tasks..."
    
    # Resubmit with proper array configuration
    sbatch \
        --array=1-$ENDPOINT_COUNT \
        --time=$TIME_LIMIT \
        --mem-per-cpu=$MEM_PER_CPU \
        --cpus-per-task=2 \
        --job-name=nba_${PROFILE} \
        $0 $PROFILE
        
    echo "Array job submitted. Check status with: squeue -u \$(whoami)"
    exit 0
fi

# Array job execution
echo "Running array task $SLURM_ARRAY_TASK_ID of $ENDPOINT_COUNT"

# Get the specific endpoint for this task
ENDPOINT=$(sed -n "${SLURM_ARRAY_TASK_ID}p" $ENDPOINTS_FILE)
if [ -z "$ENDPOINT" ]; then
    echo "Error: Could not get endpoint for task $SLURM_ARRAY_TASK_ID"
    exit 1
fi

echo "Processing endpoint: $ENDPOINT"

# Load modules and environment (CUSTOMIZE FOR YOUR CLUSTER)
module load python/3.11.0
source .venv/bin/activate

# Create unique node ID
NODE_ID="${PROFILE}_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"

# Run the endpoint processor
python collectors/endpoint_processor.py \
    --endpoint "$ENDPOINT" \
    --node-id "$NODE_ID" \
    --rate-limit "$RATE_LIMIT" \
    --db-config config/database_config.json \
    --log-level INFO

EXIT_CODE=$?

# Cleanup temp file if this is the last task
if [ "$SLURM_ARRAY_TASK_ID" -eq "$ENDPOINT_COUNT" ]; then
    rm -f "$ENDPOINTS_FILE"
fi

echo "Task $SLURM_ARRAY_TASK_ID completed with exit code: $EXIT_CODE"
exit $EXIT_CODE
