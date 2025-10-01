#!/bin/bash
# Simple NBA Collection Job Manager

show_usage() {
    echo "NBA Collection Job Manager"
    echo "Usage: $0 <command> [arguments]"
    echo ""
    echo "Commands:"
    echo "  submit <profile>  - Submit job with profile (test|high_priority|full)"
    echo "  status           - Show job status and quick stats"
    echo "  monitor          - Run detailed monitoring script"
    echo "  cancel <job_id>  - Cancel specific job"
    echo "  logs <job_id>    - Show logs for job"
    echo "  profiles         - List available profiles"
    echo "  cleanup          - Clean old log files"
    echo ""
}

show_profiles() {
    echo "Available Collection Profiles:"
    echo "=============================="
    python -c "
import json
with open('config/run_config.json', 'r') as f:
    config = json.load(f)
for name, profile in config['collection_profiles'].items():
    print(f'  {name:15} - {profile[\"description\"]}')
"
}

case "$1" in
    "submit")
        if [ -z "$2" ]; then
            echo "Error: Profile required"
            echo "Usage: $0 submit <profile>"
            show_profiles
            exit 1
        fi
        
        echo "Submitting NBA collection workflow with profile: $2"
        echo "Using coordinated workflow (master tables → regular endpoints)"
        sbatch batching/nba_workflow.sh "$2"
        ;;
        
    "status")
        echo "NBA Collection Jobs Status:"
        echo "=========================="
        
        # First try with NBA filter (handles both nba_* and nba_collection patterns)
        NBA_JOBS=$(squeue -u $(whoami) --name=nba* --noheader 2>/dev/null | wc -l)
        
        if [ "$NBA_JOBS" -gt 0 ]; then
            echo "Found $NBA_JOBS NBA jobs:"
            squeue -u $(whoami) --format="%.10i %.15j %.8T %.10M %.5D %.12L" --name=nba*
            echo ""
            echo "Quick Stats:"
            echo "  Total NBA jobs: $NBA_JOBS"
            echo "  Running: $(squeue -u $(whoami) --name=nba* -t RUNNING --noheader 2>/dev/null | wc -l)"
            echo "  Pending: $(squeue -u $(whoami) --name=nba* -t PENDING --noheader 2>/dev/null | wc -l)"
            echo "  Completed: $(squeue -u $(whoami) --name=nba* -t COMPLETED --noheader 2>/dev/null | wc -l)"
        else
            echo "No NBA-named jobs found. Showing all your jobs:"
            ALL_JOBS=$(squeue -u $(whoami) --noheader 2>/dev/null | wc -l)
            if [ "$ALL_JOBS" -gt 0 ]; then
                echo "Found $ALL_JOBS total jobs:"
                squeue -u $(whoami) --format="%.10i %.15j %.8T %.10M %.5D %.12L"
                echo ""
                echo "Job names:"
                squeue -u $(whoami) --format="%.15j" --noheader | sort | uniq -c
            else
                echo "No jobs found for user $(whoami)"
                echo ""
                echo "Checking recent job history:"
                sacct -u $(whoami) --format=JobID,JobName,State,Submit,Start,End -S today --noheader | head -10
            fi
        fi
        ;;
        
    "cancel")
        if [ -z "$2" ]; then
            echo "Error: Job ID required"
            echo "Usage: $0 cancel <job_id>"
            exit 1
        fi
        echo "Cancelling job $2..."
        scancel "$2"
        ;;
        
    "logs")
        if [ -z "$2" ]; then
            echo "Error: Job ID required" 
            echo "Usage: $0 logs <job_id>"
            exit 1
        fi
        
        echo "Recent logs for job $2:"
        find logs -name "*_${2}_*" -type f | head -10 | while read logfile; do
            echo "=== $logfile ==="
            tail -20 "$logfile"
            echo ""
        done
        ;;
        
    "profiles")
        show_profiles
        ;;
        
    "monitor")
        echo "Running detailed monitoring..."
        python scripts/monitor.py
        ;;
        
    "cleanup")
        echo "Cleaning old log files (>7 days)..."
        find logs -name "*.log" -mtime +7 -delete 2>/dev/null || echo "No old logs to clean"
        find logs -name "*.out" -mtime +7 -delete 2>/dev/null || echo "No old output files to clean" 
        find logs -name "*.err" -mtime +7 -delete 2>/dev/null || echo "No old error files to clean"
        echo "Cleanup complete."
        ;;
        
    *)
        show_usage
        exit 1
        ;;
esac
