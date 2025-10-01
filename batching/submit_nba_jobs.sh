#!/bin/bash
# NBA Parallel Job Manager - Submit Master and Endpoints Jobs with Dependencies
# Usage: ./submit_nba_jobs.sh <profile> [test|production]

# Default profile if none provided
PROFILE=${1:-test}

echo "NBA PARALLEL JOB SUBMISSION"
echo "=========================="
echo "Profile: $PROFILE"
echo "Date: $(date)"

# Navigate to project root
PROJECT_ROOT="$(cd .. && pwd)"
cd "$PROJECT_ROOT"

# Create logs directory if it doesn't exist
mkdir -p logs

echo ""
echo "🚀 PHASE 1: Submitting MASTER TABLES job..."

# Submit master tables job
MASTERS_JOB_OUTPUT=$(sbatch batching/nba_masters.sh $PROFILE)
MASTERS_JOB_ID=$(echo $MASTERS_JOB_OUTPUT | grep -o '[0-9]\+$')

if [ -z "$MASTERS_JOB_ID" ]; then
    echo "❌ Failed to submit master tables job!"
    echo "Output: $MASTERS_JOB_OUTPUT"
    exit 1
fi

echo "✅ Master tables job submitted: $MASTERS_JOB_ID"

echo ""
echo "🚀 PHASE 2: Submitting ENDPOINTS job with dependency..."

# Submit endpoints job with dependency on masters completion
ENDPOINTS_JOB_OUTPUT=$(sbatch --dependency=afterok:$MASTERS_JOB_ID batching/nba_endpoints.sh $PROFILE)
ENDPOINTS_JOB_ID=$(echo $ENDPOINTS_JOB_OUTPUT | grep -o '[0-9]\+$')

if [ -z "$ENDPOINTS_JOB_ID" ]; then
    echo "❌ Failed to submit endpoints job!"
    echo "Output: $ENDPOINTS_JOB_OUTPUT"
    exit 1
fi

echo "✅ Endpoints job submitted: $ENDPOINTS_JOB_ID"
echo "   → Waiting for master job $MASTERS_JOB_ID to complete"

echo ""
echo "📊 JOB SUBMISSION SUMMARY"
echo "========================"
echo "Master Tables Job:  $MASTERS_JOB_ID"
echo "Endpoints Job:      $ENDPOINTS_JOB_ID (depends on $MASTERS_JOB_ID)"
echo ""
echo "To monitor jobs:"
echo "  squeue -u \$USER"
echo "  squeue -j $MASTERS_JOB_ID,$ENDPOINTS_JOB_ID"
echo ""
echo "To view logs:"
echo "  tail -f logs/nba_masters_$MASTERS_JOB_ID.out"
echo "  tail -f logs/nba_endpoints_$ENDPOINTS_JOB_ID.out"
echo ""
echo "To cancel jobs if needed:"
echo "  scancel $MASTERS_JOB_ID $ENDPOINTS_JOB_ID"

# Optional: Wait for completion and report results
if [ "$2" = "wait" ]; then
    echo ""
    echo "⏳ Waiting for jobs to complete..."
    
    # Wait for both jobs to finish
    while squeue -j $MASTERS_JOB_ID,$ENDPOINTS_JOB_ID | grep -q -E "$MASTERS_JOB_ID|$ENDPOINTS_JOB_ID"; do
        sleep 30
        echo "  $(date): Jobs still running..."
    done
    
    echo ""
    echo "🏁 Both jobs have completed!"
    echo "Check logs for results:"
    echo "  Masters: logs/nba_masters_$MASTERS_JOB_ID.out"
    echo "  Endpoints: logs/nba_endpoints_$ENDPOINTS_JOB_ID.out"
fi

echo ""
echo "✅ Job submission complete!"