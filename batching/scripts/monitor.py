#!/usr/bin/env python3
"""
NBA Job Monitoring and Status Checker
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

def run_command(cmd):
    """Run shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def get_job_status():
    """Get current job status from SLURM"""
    success, stdout, stderr = run_command("squeue -u $(whoami) --format='%.10i %.15j %.8T %.10M %.5D %.12L' --name=nba_*")
    
    if success and stdout.strip():
        lines = stdout.strip().split('\n')
        if len(lines) > 1:  # Skip header
            print("Current NBA Jobs:")
            print("=" * 70)
            print(lines[0])  # Header
            print("-" * 70)
            for line in lines[1:]:
                print(line)
            print()
            return True
        else:
            print("No NBA jobs currently running.")
            return False
    else:
        print("Could not retrieve job status.")
        return False

def get_recent_logs(hours=1):
    """Get recent log files"""
    logs_dir = Path("logs")
    if not logs_dir.exists():
        print("No logs directory found.")
        return
    
    print(f"\nRecent log files (last {hours} hours):")
    print("=" * 50)
    
    # Find log files modified in the last hour
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(hours=hours)
    
    recent_logs = []
    for log_file in logs_dir.glob("nba_*.log"):
        if log_file.stat().st_mtime > cutoff.timestamp():
            recent_logs.append(log_file)
    
    if recent_logs:
        for log_file in sorted(recent_logs, key=lambda x: x.stat().st_mtime, reverse=True):
            mod_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            size_kb = log_file.stat().st_size / 1024
            print(f"  {log_file.name:<40} {mod_time.strftime('%H:%M:%S')} ({size_kb:.1f} KB)")
    else:
        print("  No recent log files found.")

def check_endpoint_progress():
    """Check progress of endpoint processing"""
    print("\nEndpoint Processing Progress:")
    print("=" * 40)
    
    # This would connect to your database to check progress
    # For now, we'll just show a placeholder
    try:
        # You could add database queries here to check:
        # - Which endpoints have been processed
        # - How many records were collected
        # - Which endpoints failed
        
        print("  [Feature not implemented yet]")
        print("  Future: Show completion status per endpoint")
        print("  Future: Show record counts and error rates")
        
    except Exception as e:
        print(f"  Could not check database progress: {e}")

def show_system_resources():
    """Show current system resource usage"""
    print("\nSystem Resources:")
    print("=" * 30)
    
    # Show disk usage
    success, stdout, stderr = run_command("df -h | grep -E '(Filesystem|/$|/home)'")
    if success:
        print("Disk Usage:")
        print(stdout)
    
    # Show memory usage if available
    success, stdout, stderr = run_command("free -h")
    if success:
        print("Memory Usage:")
        lines = stdout.strip().split('\n')
        for line in lines[:2]:  # Just show header and memory line
            print(line)

def main():
    """Main monitoring function"""
    print("NBA Data Collection - Job Monitor")
    print("=" * 50)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Check job status
    has_jobs = get_job_status()
    
    # Show recent logs
    get_recent_logs(hours=2)
    
    # Check progress (placeholder)
    check_endpoint_progress()
    
    # Show resources
    show_system_resources()
    
    if has_jobs:
        print("\n💡 Tips:")
        print("  - Use './nba_jobs.sh status' for quick status")
        print("  - Use './nba_jobs.sh logs <job_id>' for specific job logs")  
        print("  - Use 'tail -f logs/nba_*.log' to follow log updates")
    else:
        print("\n🚀 To start jobs:")
        print("  ./nba_jobs.sh submit test")
        print("  ./nba_jobs.sh submit high_priority") 
        print("  ./nba_jobs.sh submit full")

if __name__ == "__main__":
    main()
