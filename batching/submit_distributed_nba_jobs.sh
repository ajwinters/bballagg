#!/bin/bash
# NBA Distributed Job Submission - One Job Per Endpoint
# Usage: ./submit_distributed_nba_jobs.sh [profile]
#
# Available profiles:
#   high_priority  - High priority endpoints only (DEFAULT)
#   test          - Limited endpoints with small data samples  
#   full          - All available endpoints (comprehensive)
#
# Examples:
#   ./submit_distributed_nba_jobs.sh                    # Uses high_priority
#   ./submit_distributed_nba_jobs.sh high_priority      # High priority endpoints
#   ./submit_distributed_nba_jobs.sh test               # Test mode
#   ./submit_distributed_nba_jobs.sh full               # All endpoints

# Show help if requested
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "NBA Distributed Job Submission"
    echo "=============================="
    echo "Usage: $0 [profile]"
    echo ""
    echo "Available profiles:"
    echo "  high_priority  - High priority endpoints only (DEFAULT)"
    echo "  test          - Limited endpoints with small data samples"
    echo "  full          - All available endpoints (comprehensive)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Uses high_priority (default)"
    echo "  $0 high_priority      # High priority endpoints"
    echo "  $0 test               # Test mode"
    echo "  $0 full               # All endpoints"
    echo ""
    exit 0
fi

PROFILE=${1:-high_priority}

echo "🚀 NBA DISTRIBUTED JOB SUBMISSION"
echo "=================================="
echo "Profile: $PROFILE"
if [ "$PROFILE" = "high_priority" ]; then
    echo "Mode: HIGH PRIORITY ENDPOINTS (default - most important data)"
elif [ "$PROFILE" = "test" ]; then
    echo "Mode: TEST MODE (limited data for testing)"
elif [ "$PROFILE" = "full" ]; then
    echo "Mode: FULL MODE (all endpoints - comprehensive collection)"
else
    echo "Mode: CUSTOM PROFILE"
fi
echo "Date: $(date)"
echo ""

# Navigate to project root
PROJECT_ROOT="/storage/home/ajw5296/work/thebigone"
cd "$PROJECT_ROOT"

# Create logs directory
mkdir -p logs

# Step 1: Submit master tables job
echo "📋 PHASE 1: Submitting MASTER TABLES job..."
MASTERS_JOB_OUTPUT=$(sbatch "${PROJECT_ROOT}/batching/nba_masters.sh" $PROFILE)
MASTERS_JOB_ID=$(echo $MASTERS_JOB_OUTPUT | grep -o '[0-9]\+$')

if [ -z "$MASTERS_JOB_ID" ]; then
    echo "❌ Failed to submit master tables job!"
    exit 1
fi

echo "✅ Master tables job submitted: $MASTERS_JOB_ID"

# Step 2: Get list of endpoints to process
echo ""
echo "📋 PHASE 2: Getting endpoint list..."

# Get endpoints from config based on profile
ENDPOINTS_LIST=$(python -c "
import json
import sys

# Load endpoint config
with open('${PROJECT_ROOT}/config/endpoint_config.json', 'r') as f:
    endpoint_config = json.load(f)

profile = '$PROFILE'
endpoints = []

# Get all endpoints with non-None priority (excluding master endpoints)
for name, config in endpoint_config['endpoints'].items():
    # Skip master endpoints
    if 'master' in config:
        continue

    # Include all endpoints with a non-None priority
    priority = config.get('priority')
    if priority is not None and priority != 'None':
        endpoints.append(name)

# Print endpoints, one per line
for endpoint in sorted(endpoints):
    print(endpoint)
")

if [ -z "$ENDPOINTS_LIST" ]; then
    echo "❌ No endpoints found for profile '$PROFILE'"
    exit 1
fi

# Convert to array
readarray -t ENDPOINTS_ARRAY <<< "$ENDPOINTS_LIST"
ENDPOINT_COUNT=${#ENDPOINTS_ARRAY[@]}

echo "✅ Found $ENDPOINT_COUNT endpoints to process:"
printf '   • %s\n' "${ENDPOINTS_ARRAY[@]}"

# Step 3: Submit one job per endpoint
echo ""
echo "🚀 PHASE 3: Submitting $ENDPOINT_COUNT endpoint jobs..."

ENDPOINT_JOB_IDS=()
FAILED_SUBMISSIONS=0

for endpoint in "${ENDPOINTS_ARRAY[@]}"; do
    # Submit job with dependency on masters completion
    # Job name format: nba_<profile>_<endpoint> (e.g., nba_high_priority_PlayerGameLog)
    JOB_OUTPUT=$(sbatch --dependency=afterok:$MASTERS_JOB_ID \
                        --job-name="nba_${PROFILE}_${endpoint}" \
                        "${PROJECT_ROOT}/batching/single_endpoint.sh" "$PROFILE" "$endpoint")
    
    JOB_ID=$(echo $JOB_OUTPUT | grep -o '[0-9]\+$')
    
    if [ -n "$JOB_ID" ]; then
        ENDPOINT_JOB_IDS+=($JOB_ID)
        echo "✅ $endpoint → Job $JOB_ID (nba_${PROFILE}_${endpoint})"
    else
        echo "❌ Failed to submit $endpoint"
        ((FAILED_SUBMISSIONS++))
    fi
done

# Summary
echo ""
echo "📊 DISTRIBUTED SUBMISSION SUMMARY"
echo "================================="
echo "Master Tables Job:    $MASTERS_JOB_ID"
echo "Endpoint Jobs:        ${#ENDPOINT_JOB_IDS[@]} submitted"
echo "Failed Submissions:   $FAILED_SUBMISSIONS"
echo "Total Endpoints:      $ENDPOINT_COUNT"

if [ $FAILED_SUBMISSIONS -gt 0 ]; then
    echo "⚠️  Some jobs failed to submit!"
fi

echo ""
echo "🔍 MONITORING COMMANDS:"
echo "======================"
echo "View all NBA jobs:"
echo "  squeue -u \$USER --name=nba*"
echo ""
echo "Monitor master job:"
echo "  tail -f logs/nba_masters_$MASTERS_JOB_ID.out"
echo ""
echo "Monitor endpoint jobs:"
echo "  squeue -j $(IFS=,; echo "${ENDPOINT_JOB_IDS[*]}")"
echo ""
echo "Cancel all NBA jobs if needed:"
echo "  scancel -u \$USER --name=nba*"

# Optional: Wait and report final results
if [ "$2" = "wait" ]; then
    echo ""
    echo "⏳ Waiting for all jobs to complete..."
    
    ALL_JOB_IDS=($MASTERS_JOB_ID "${ENDPOINT_JOB_IDS[@]}")
    IFS=','
    JOB_LIST="${ALL_JOB_IDS[*]}"
    
    while squeue -j "$JOB_LIST" 2>/dev/null | grep -q -E "$(echo ${ALL_JOB_IDS[@]} | tr ' ' '|')"; do
        sleep 60
        RUNNING_COUNT=$(squeue -j "$JOB_LIST" 2>/dev/null | grep -c -E "$(echo ${ALL_JOB_IDS[@]} | tr ' ' '|')" || echo "0")
        echo "  $(date): $RUNNING_COUNT jobs still running..."
    done
    
    echo ""
    echo "🏁 All jobs completed!"
    echo "Check individual logs in logs/ directory"
fi

echo ""
echo "✅ Distributed job submission complete!"