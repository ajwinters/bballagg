# NBA Endpoint Processor - Distributed Processing Guide

This document explains how to use the NBA Endpoint Processor for distributed processing across multiple nodes with different IP addresses to avoid NBA API rate limits.

## Overview

The NBA Endpoint Processor has been enhanced to support parameterized, distributed processing. This allows you to:

1. **Run on multiple nodes/servers** with different IP addresses
2. **Process specific endpoints** (each endpoint returns multiple dataframes via `get_data_frames()`)
3. **Split parameter ranges** across nodes (e.g., different game_ids, player_ids)
4. **Run endpoints in parallel** across different machines
5. **Configure processing** via command line arguments or config files

### **NBA API Understanding:**
- Each **endpoint** (e.g., `BoxScoreAdvancedV3`) handles one API call
- Each API call uses **one parameter** (e.g., `game_id="0022100001"`)
- Each call returns **multiple dataframes** via `get_data_frames()`
- Each dataframe gets inserted into its **own table** in AWS
- **Distribution is per endpoint + parameter range**, not per dataframe

## Key Features for Distribution

- **Parameterized execution**: Specify which endpoints to process
- **Parameter range splitting**: Divide work across nodes
- **Rate limit configuration**: Adjust API call frequency per node
- **Node identification**: Track which node processed what data
- **Resume capability**: Continue from interruptions
- **Configuration files**: Easy deployment across nodes

## Command Line Usage

### Basic Usage

```bash
# Process a single endpoint (OPTIMAL for distribution)
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id node1

# This is the recommended approach - each node runs one endpoint
python collectors/endpoint_processor.py --endpoint PlayerGameLogs --node-id node2
python collectors/endpoint_processor.py --endpoint TeamGameLogs --node-id node3

# Configure rate limiting per node
python collectors/endpoint_processor.py --endpoint BoxScorePlayerTrackV3 --rate-limit 0.3 --node-id node4
```

### Configuration File Usage (Recommended)

```bash
# Each node has its own config with one endpoint
python scripts/distributed_runner.py --config config/node1_config.json --db-config config/database_config.json
python scripts/distributed_runner.py --config config/node2_config.json --db-config config/database_config.json
python scripts/distributed_runner.py --config config/node3_config.json --db-config config/database_config.json

# Dry run to verify
python scripts/distributed_runner.py --config config/node1_config.json --dry-run
```

## Distributed Processing Strategies

### **Optimal Strategy: One Endpoint Per Node**
Each node processes **one complete endpoint** with **all its parameters**:

- **Node 1 (IP: x.x.x.1)**: `BoxScoreAdvancedV3` - All games
  - Processes: ALL game_ids for this endpoint
  - Each call: `BoxScoreAdvancedV3(game_id="X")` → Multiple dataframes → Multiple tables
- **Node 2 (IP: x.x.x.2)**: `BoxScoreFourFactorsV3` - All games
  - Processes: ALL game_ids for this endpoint
  - Each call: `BoxScoreFourFactorsV3(game_id="X")` → Multiple dataframes → Multiple tables  
- **Node 3 (IP: x.x.x.3)**: `PlayerGameLogs` - All players
  - Processes: ALL player_ids for this endpoint
  - Each call: `PlayerGameLogs(player_id="X")` → Multiple dataframes → Multiple tables
- **Node 4 (IP: x.x.x.4)**: `PlayerCareerStats` - All players
  - Processes: ALL player_ids for this endpoint
  - Each call: `PlayerCareerStats(player_id="X")` → Multiple dataframes → Multiple tables

### **Why This Is Optimal:**
1. **Maximum IP distribution**: Each endpoint gets its own IP address
2. **Simple deployment**: One command per node
3. **No overlap**: No coordination needed between nodes
4. **Full rate limit utilization**: Each IP can make calls at full rate
5. **Easy monitoring**: Clear separation of responsibilities

## Configuration Files

### Database Configuration (`config/database_config.json`)
```json
{
  "host": "your-db-host.amazonaws.com",
  "name": "your_database_name",
  "user": "your_username",
  "password": "your_password",
  "port": "5432"
}
```

### Node Configuration Example (`config/node1_config.json`)
```json
{
  "description": "Node 1: High priority game-based endpoints",
  "node_id": "node_1_games_high",
  "endpoints": [
    "BoxScoreAdvancedV3",
    "BoxScoreFourFactorsV3", 
    "BoxScoreMatchupsV3"
  ],
  "rate_limit": 0.5,
  "parameter_config": {
    "start_index": 0,
    "limit": 1000
  }
}
```

## Running Distributed Processing

### Option 1: Easy Batch Commands (Windows)

```cmd
# Show what would be processed (dry run)
run_distributed.bat dry-run

# Run individual nodes
run_distributed.bat node1
run_distributed.bat node2
run_distributed.bat node3

# Run all nodes in parallel (on same machine)
run_distributed.bat all
```

### Option 2: PowerShell Script (Windows)

```powershell
# Dry run
.\scripts\run_distributed.ps1 -DryRun

# Full processing
.\scripts\run_distributed.ps1
```

### Option 3: Direct Python Commands

```bash
# Single endpoint with custom parameters
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --param-start 0 --param-end 500 --rate-limit 0.4 --node-id production_node_1

# Category with priority and limits
python collectors/endpoint_processor.py --category player_based --priority high --param-limit 100 --rate-limit 0.3 --node-id production_node_2
```

## Best Practices for Distributed Processing

### Rate Limiting
- **Different IP addresses**: Use different servers/VPNs for each node
- **Conservative rates**: Start with 0.5-1.0 second delays between calls
- **Monitor logs**: Watch for rate limit errors and adjust accordingly

### Parameter Distribution
- **Non-overlapping ranges**: Ensure parameter ranges don't overlap between nodes
- **Database coordination**: Use the database to track which parameters have been processed
- **Resume capability**: The system can resume from interruptions

### Error Handling
- **Track failures**: Failed calls are automatically tracked and skipped on retry
- **Monitor logs**: Each node logs its progress and errors
- **Gradual scaling**: Start with fewer nodes and scale up based on API response

### Resource Management
- **Connection pooling**: Each node manages its own database connections
- **Memory usage**: Process in batches to avoid memory issues
- **Disk space**: Monitor log files and database growth

## Monitoring and Troubleshooting

### Log Files
- **Endpoint processor log**: `nba_endpoint_processor.log`
- **Per-node identification**: Logs include node ID for tracking
- **Error tracking**: Failed calls are logged with details

### Database Monitoring
```sql
-- Check processing progress
SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'nba_%';

-- Check failed calls
SELECT * FROM nba_endpoint_failed_calls ORDER BY failed_at DESC LIMIT 10;

-- Check recent processing activity
SELECT table_name, COUNT(*) as row_count 
FROM information_schema.tables t
JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE t.table_name LIKE 'nba_%'
GROUP BY table_name
ORDER BY row_count DESC;
```

### Common Issues and Solutions

1. **Rate Limiting**
   - Increase `--rate-limit` value
   - Reduce parallel nodes
   - Use different IP addresses

2. **Connection Timeouts**
   - Check network connectivity
   - Verify database configuration
   - Restart processing (it will resume)

3. **Parameter Errors**
   - Verify parameter ranges don't exceed available data
   - Check master table data availability
   - Use dry-run to validate configuration

## Advanced Usage

### Custom Parameter Lists
```bash
# Process specific games only
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --param-list 0022100001 0022100002 0022100003
```

### Environment Variables
Set environment variables for database connection:
```bash
export DB_HOST=your-host.amazonaws.com
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_PORT=5432
```

### Integration with Task Schedulers
The processor works well with:
- **Windows Task Scheduler**
- **Cron jobs (Linux)**
- **Docker containers**
- **Cloud job schedulers**

## Security Considerations

1. **Database credentials**: Use config files or environment variables, never hardcode
2. **API keys**: Store NBA API credentials securely
3. **Network security**: Use secure connections and VPNs when possible
4. **Access logs**: Monitor who is running what processing

## Performance Optimization

1. **Batch processing**: Use parameter limits to process in manageable chunks
2. **Parallel processing**: Run different categories on different nodes
3. **Resource monitoring**: Monitor CPU, memory, and network usage
4. **Database indexing**: Ensure proper indexes on frequently queried columns

This distributed approach allows you to scale NBA data collection across multiple IP addresses while maintaining data integrity and avoiding API rate limits.
