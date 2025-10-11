# NBA Distributed Job Naming Enhancements

## ✅ **Endpoint Names in Job Names - IMPLEMENTED**

Your NBA distributed jobs now include endpoint names in their job names for easy identification and monitoring.

## Job Naming Convention

### **Master Tables Job**
- **Format**: `nba_masters_<profile>`
- **Example**: `nba_masters_high_priority`
- **Purpose**: Identifies the master tables collection phase

### **Individual Endpoint Jobs**
- **Format**: `nba_<profile>_<endpoint>`
- **Example**: `nba_high_priority_PlayerGameLog`
- **Purpose**: Shows exactly which endpoint and profile is being processed

### **Array Jobs** (if using array mode)
- **Format**: `nba_<profile>_<endpoint>` (dynamically updated)
- **Example**: `nba_test_CommonTeamYears`
- **Purpose**: Array tasks are renamed to show the specific endpoint being processed

## Enhanced Features

### **1. Dynamic Job Name Updates**
- Jobs automatically update their names to include endpoint information
- Uses `scontrol update JobId=<job_id> JobName=<new_name>` when available
- Fallback gracefully if scontrol is not available

### **2. Improved Log File Naming**
- **Single Endpoint**: `logs/nba_<job_name>_<job_id>.out`
- **Array Jobs**: `logs/nba_array_<job_name>_<array_id>_<task_id>.out`
- **Master Tables**: `logs/nba_masters_<job_id>.out`

### **3. Clear Job Submission Output**
```bash
✅ PlayerGameLog → Job 12345 (nba_high_priority_PlayerGameLog)
✅ TeamGameLog → Job 12346 (nba_high_priority_TeamGameLog)
✅ CommonAllPlayers → Job 12347 (nba_high_priority_CommonAllPlayers)
```

## Monitor Your Jobs

### **View All NBA Jobs**
```bash
squeue -u $(whoami) | grep nba_
```

### **View Jobs by Profile**
```bash
squeue -u $(whoami) | grep nba_high_priority
```

### **View Specific Endpoint**
```bash
squeue -u $(whoami) | grep PlayerGameLog
```

### **Cancel Jobs by Endpoint**
```bash
scancel -u $(whoami) -n nba_high_priority_PlayerGameLog
```

### **Cancel All Profile Jobs**
```bash
scancel -u $(whoami) -n nba_high_priority_
```

## Example Job Queue View

```
JOBID   PARTITION  NAME                    USER   ST  TIME  NODES
12340   compute    nba_masters_high_priority ajw5296 R  0:15  1
12341   compute    nba_high_priority_PlayerGameLog ajw5296 PD  0:00  1
12342   compute    nba_high_priority_TeamGameLog   ajw5296 PD  0:00  1
12343   compute    nba_high_priority_LeagueGameFinder ajw5296 PD 0:00 1
```

## Files Modified

- ✅ `submit_distributed_nba_jobs.sh` - Enhanced job name format and output
- ✅ `single_endpoint.sh` - Dynamic job name updates and better logging
- ✅ `nba_masters.sh` - Include profile in master job names
- ✅ `slurm_nba_collection.sh` - Dynamic endpoint names for array tasks
- ✅ `monitor_nba_jobs.sh` - New monitoring script for job status

## Benefits

1. **Easy Identification**: Instantly see which endpoint is running
2. **Profile Separation**: Distinguish between test, high_priority, and full runs
3. **Debugging**: Quickly identify failed endpoints in job queues
4. **Monitoring**: Simplified job management and status tracking
5. **Log Organization**: Clear log file naming for troubleshooting

Your distributed NBA data collection system now provides clear, informative job names that make monitoring and management much easier!