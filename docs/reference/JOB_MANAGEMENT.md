# Job Management Reference

## 🎯 **SLURM Job Management**

### **Job Naming Convention**
- **Master Jobs**: `nba_masters_<profile>` (e.g., `nba_masters_high_priority`)
- **Endpoint Jobs**: `nba_<profile>_<endpoint>` (e.g., `nba_high_priority_PlayerGameLog`)
- **Array Jobs**: Dynamically updated to include endpoint names

### **Job Submission**
```bash
# Submit distributed jobs with dependencies
./batching/submit_distributed_nba_jobs.sh high_priority

# Submit single endpoint test
sbatch batching/single_endpoint.sh test PlayerGameLogs

# Submit array job for multiple endpoints
sbatch batching/slurm_nba_collection.sh high_priority
```

### **Job Monitoring**
```bash
# View all NBA jobs
squeue -u $(whoami) | grep nba_

# View jobs by profile
squeue -u $(whoami) | grep nba_high_priority

# View specific endpoint
squeue -u $(whoami) | grep PlayerGameLog

# Detailed job information
scontrol show job <job_id>
```

### **Job Cancellation**
```bash
# Cancel all NBA jobs
scancel -u $(whoami) -n "*nba*"

# Cancel specific profile
scancel -u $(whoami) -n "*nba_high_priority*"

# Cancel specific endpoint
scancel -u $(whoami) -n "*PlayerGameLog*"

# Cancel by job state
scancel -u $(whoami) -t PENDING -n "*nba*"
```

---

## 📊 **Job Status & Monitoring**

### **Queue Status Commands**
```bash
# Custom format showing job details
squeue -u $(whoami) --format="%.10i %.15P %.25j %.8u %.8T %.10M %.9l %.6D %R"

# Count jobs by status
squeue -u $(whoami) | grep nba | wc -l

# Show recent completed jobs
sacct -u $(whoami) --format="JobID,JobName%25,State,ExitCode,Start,End" -S $(date -d '1 day ago' +%Y-%m-%d) | grep nba
```

### **Log File Management**
```bash
# Log file locations
logs/nba_masters_<job_id>.out                    # Master table jobs
logs/nba_<job_name>_<job_id>.out                 # Individual endpoint jobs  
logs/nba_array_<job_name>_<array_id>_<task_id>.out  # Array jobs

# Monitor real-time logs
tail -f logs/nba_high_priority_PlayerGameLog_*.out

# Search logs for errors
grep -i error logs/nba_*.out
grep -i "failed" logs/nba_*.out
```

---

## 🔧 **Job Configuration**

### **Resource Allocation**
```bash
# Standard endpoint job
#SBATCH --time=24:00:00
#SBATCH --mem=4GB
#SBATCH --cpus-per-task=1

# Master table job  
#SBATCH --time=02:00:00
#SBATCH --mem=4GB
#SBATCH --cpus-per-task=1

# Heavy processing job
#SBATCH --time=24:00:00
#SBATCH --mem=8GB
#SBATCH --cpus-per-task=2
```

### **Job Dependencies**
```bash
# Submit with dependency on masters completion
sbatch --dependency=afterok:$MASTERS_JOB_ID batching/single_endpoint.sh profile endpoint

# Chain multiple jobs
JOB1=$(sbatch job1.sh | grep -o '[0-9]*$')
JOB2=$(sbatch --dependency=afterok:$JOB1 job2.sh | grep -o '[0-9]*$')
```

---

## 🚨 **Troubleshooting**

### **Common Job Failures**
1. **Out of Memory**: Increase `--mem` allocation
2. **Time Limit**: Increase `--time` or reduce data scope
3. **API Rate Limiting**: Adjust `rate_limit` in configuration
4. **Database Connection**: Check connection settings and credentials

### **Debugging Steps**
1. **Check Job Status**: `scontrol show job <job_id>`
2. **Review Logs**: `cat logs/nba_*_<job_id>.out`
3. **Test Locally**: Run same command on login node
4. **Validate Configuration**: Check endpoint and run configs

### **Recovery Procedures**
1. **Cancel Failed Jobs**: `scancel -u $(whoami) -n "*nba*"`
2. **Check Database State**: Verify master tables exist
3. **Resubmit**: Use same commands with fixed configuration
4. **Monitor Progress**: Use monitoring commands to track recovery

---

## 📋 **Job Management Workflows**

### **Standard Batch Processing**
1. **Prepare**: Ensure database is clean and accessible
2. **Submit Masters**: `./batching/submit_distributed_nba_jobs.sh high_priority`
3. **Monitor**: Use `squeue` to track progress
4. **Validate**: Check logs for any failures
5. **Recover**: Resubmit any failed endpoints individually

### **Incremental Updates**
1. **Identify Scope**: Determine which endpoints need updates
2. **Test Single**: Submit one endpoint to verify configuration
3. **Batch Submit**: Submit remaining endpoints with dependencies
4. **Monitor & Adjust**: Track progress and adjust resources as needed

### **Emergency Procedures**
1. **Mass Cancellation**: `scancel -u $(whoami) -n "*nba*"`
2. **System Check**: Verify database connectivity and API access
3. **Diagnostic Run**: Submit single test endpoint
4. **Gradual Recovery**: Resubmit in small batches

This reference provides complete job management procedures for the NBA data collection system.