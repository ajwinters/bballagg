#!/bin/bash
# Simple NBA Collection Job Manager

show_usage() {
    echo "NBA Collection Job Manager"
    echo "Usage: $0 <command> [arguments]"
    echo ""
    echo "Commands:"
    echo "  submit <profile>  - Submit job with profile (test|high_priority|full)"
    echo "  status           - Show job status"  
    echo "  cancel <job_id>  - Cancel specific job"
    echo "  logs <job_id>    - Show logs for job"
    echo "  profiles         - List available profiles"
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
        
        echo "Submitting NBA collection job with profile: $2"
        sbatch deployment/slurm_nba_collection.sh "$2"
        ;;
        
    "status")
        echo "NBA Collection Jobs Status:"
        squeue -u $(whoami) --format="%.10i %.15j %.8T %.10M %.5D %.12L" --name=nba_*
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
        
    *)
        show_usage
        exit 1
        ;;
esac
