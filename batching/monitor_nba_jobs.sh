#!/bin/bash
# NBA Job Monitor - Show jobs with endpoint names
# Usage: ./monitor_nba_jobs.sh [profile]

PROFILE=${1:-}

echo "🏀 NBA DATA COLLECTION JOBS"
echo "==========================="

if [ -n "$PROFILE" ]; then
    echo "Filtering for profile: $PROFILE"
    squeue -u $(whoami) --format="%.10i %.15P %.25j %.8u %.8T %.10M %.9l %.6D %R" | head -1
    squeue -u $(whoami) --format="%.10i %.15P %.25j %.8u %.8T %.10M %.9l %.6D %R" | grep "nba_${PROFILE}" || echo "No jobs found for profile: $PROFILE"
else
    echo "Showing all NBA jobs (use './monitor_nba_jobs.sh <profile>' to filter)"
    squeue -u $(whoami) --format="%.10i %.15P %.25j %.8u %.8T %.10M %.9l %.6D %R" | head -1
    squeue -u $(whoami) --format="%.10i %.15P %.25j %.8u %.8T %.10M %.9l %.6D %R" | grep "nba_" || echo "No NBA jobs found"
fi

echo ""
echo "📊 JOB STATUS SUMMARY"
echo "=====================" 

if [ -n "$PROFILE" ]; then
    TOTAL=$(squeue -u $(whoami) --format="%.25j" | grep -c "nba_${PROFILE}" || echo "0")
    RUNNING=$(squeue -u $(whoami) --format="%.25j %.8T" | grep "nba_${PROFILE}" | grep -c "RUNNING" || echo "0")
    PENDING=$(squeue -u $(whoami) --format="%.25j %.8T" | grep "nba_${PROFILE}" | grep -c "PENDING" || echo "0")
else
    TOTAL=$(squeue -u $(whoami) --format="%.25j" | grep -c "nba_" || echo "0")
    RUNNING=$(squeue -u $(whoami) --format="%.25j %.8T" | grep "nba_" | grep -c "RUNNING" || echo "0")
    PENDING=$(squeue -u $(whoami) --format="%.25j %.8T" | grep "nba_" | grep -c "PENDING" || echo "0")
fi

echo "Total NBA Jobs: $TOTAL"
echo "Running: $RUNNING"
echo "Pending: $PENDING"

echo ""
echo "🔍 QUICK COMMANDS"
echo "================="
echo "Monitor specific profile: ./monitor_nba_jobs.sh <profile>"
echo "Cancel all NBA jobs: scancel -u \$(whoami) -n nba_"
echo "Cancel profile jobs: scancel -u \$(whoami) -n nba_<profile>_"
echo "View job details: scontrol show job <job_id>"

# Show recent completed jobs
echo ""
echo "📋 RECENT COMPLETED NBA JOBS (last 10)"
echo "======================================="
sacct -u $(whoami) --format="JobID,JobName%25,State,ExitCode,Start,End" -S $(date -d '1 day ago' +%Y-%m-%d) | grep "nba_" | tail -10 || echo "No recent completed jobs found"