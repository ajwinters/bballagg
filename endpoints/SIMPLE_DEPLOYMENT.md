# NBA Endpoint Processing - Simplified Distribution

## ✅ FINAL APPROACH: No Config Files Needed!

Each server runs ONE endpoint. All endpoint configurations are already defined in `nba_endpoints_config.py`.

### Perfect Deployment (One Command Per Server):

```bash
# Server 1 (IP: x.x.x.1) - BoxScoreAdvancedV3
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id server1

# Server 2 (IP: x.x.x.2) - BoxScoreFourFactorsV3  
python collectors/endpoint_processor.py --endpoint BoxScoreFourFactorsV3 --node-id server2

# Server 3 (IP: x.x.x.3) - PlayerGameLogs
python collectors/endpoint_processor.py --endpoint PlayerGameLogs --node-id server3

# Server 4 (IP: x.x.x.4) - PlayerCareerStats
python collectors/endpoint_processor.py --endpoint PlayerCareerStats --node-id server4

# Server 5 (IP: x.x.x.5) - TeamGameLogs
python collectors/endpoint_processor.py --endpoint TeamGameLogs --node-id server5
```

### How It Works:

1. **Looks up endpoint**: Finds `BoxScoreAdvancedV3` in `nba_endpoints_config.py`
2. **Gets parameters**: Retrieves all `game_id` values from master tables
3. **Processes completely**: For each game_id, calls API and gets ALL dataframes
4. **Creates tables**: Each dataframe goes into its own table in AWS
5. **Handles errors**: Failed calls are tracked and can be resumed

### Available Commands:

```bash
# See all available endpoints (41 total)
python collectors/endpoint_processor.py

# Test any endpoint with dry-run
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --dry-run

# Adjust rate limiting
python collectors/endpoint_processor.py --endpoint PlayerGameLogs --rate-limit 0.5 --node-id server3

# Use parameter ranges (only if needed)
python collectors/endpoint_processor.py --endpoint PlayerGameLogs --param-start 0 --param-end 250 --node-id server3a
```

### Top Priority Endpoints for Distribution:

```bash
# High-volume game data (priority endpoints)
python collectors/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id server1
python collectors/endpoint_processor.py --endpoint BoxScoreFourFactorsV3 --node-id server2  
python collectors/endpoint_processor.py --endpoint BoxScorePlayerTrackV3 --node-id server3
python collectors/endpoint_processor.py --endpoint BoxScoreMiscV3 --node-id server4

# High-volume player data
python collectors/endpoint_processor.py --endpoint PlayerGameLogs --node-id server5
python collectors/endpoint_processor.py --endpoint PlayerCareerStats --node-id server6

# Team data  
python collectors/endpoint_processor.py --endpoint TeamGameLogs --node-id server7
python collectors/endpoint_processor.py --endpoint TeamDashboardByGeneralSplits --node-id server8
```

### Benefits of This Approach:

✅ **No config files needed** - Everything is in `nba_endpoints_config.py`  
✅ **Simple deployment** - One command per server  
✅ **Maximum IP distribution** - Each endpoint gets its own IP address  
✅ **Complete processing** - All dataframes from each endpoint are handled  
✅ **Easy to monitor** - Clear separation by endpoint  
✅ **Resumable** - Can restart any endpoint independently  
✅ **Scalable** - Add more servers for more endpoints  

### That's It!

Deploy each command on a different server with a different IP address to maximize API rate limit distribution.
