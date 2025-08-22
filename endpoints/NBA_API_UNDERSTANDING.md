# NBA API Endpoint Processing - Corrected Understanding

## How NBA API Endpoints Work

### Single Endpoint Call Process:
```python
# Example: BoxScoreAdvancedV3 endpoint
endpoint = nbaapi.BoxScoreAdvancedV3(game_id="0022100001")
dataframes = endpoint.get_data_frames()

# This single call returns multiple dataframes:
# - dataframes[0]: Team advanced stats for this game
# - dataframes[1]: Player advanced stats for this game  
# - dataframes[2]: Maybe additional stats tables
# Each dataframe goes into its own table in AWS
```

### Distributed Processing Approach:

#### ✅ CORRECT: One Endpoint Per Node (Optimal)
```bash
# Node 1: BoxScoreAdvancedV3 for ALL games
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id node1

# Node 2: PlayerGameLogs for ALL players  
python endpoint_processor.py --endpoint PlayerGameLogs --node-id node2

# Node 3: TeamGameLogs for ALL teams
python endpoint_processor.py --endpoint TeamGameLogs --node-id node3

# This is the OPTIMAL approach - maximum IP distribution, simple deployment
```

#### ⚠️ ACCEPTABLE: Parameter range splitting (only if needed)
```bash
# Only use this if you have more servers than endpoints
# Node 1: BoxScoreAdvancedV3 for games 1-1000
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --param-start 0 --param-end 1000 --node-id node1

# Node 2: BoxScoreAdvancedV3 for games 1001-2000  
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --param-start 1000 --param-end 2000 --node-id node2
```

#### ❌ INCORRECT: Trying to distribute individual dataframes
```bash
# This is NOT how the NBA API works
# You cannot process just "dataframe 0" from BoxScoreAdvancedV3
# Each endpoint call must process ALL its dataframes together
```

## Real-World Distribution Examples

### Scenario 1: One Endpoint Per Node (OPTIMAL)
```bash
# Server 1 (IP: 1.2.3.4): BoxScoreAdvancedV3 for ALL games
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id server1_boxscore_advanced

# Server 2 (IP: 5.6.7.8): PlayerGameLogs for ALL players
python endpoint_processor.py --endpoint PlayerGameLogs --node-id server2_player_gamelogs

# Server 3 (IP: 9.10.11.12): TeamGameLogs for ALL teams  
python endpoint_processor.py --endpoint TeamGameLogs --node-id server3_team_gamelogs

# This maximizes IP distribution and simplifies deployment
```

### Scenario 2: Only if you have more servers than endpoints
```bash
# If you have 10 servers but only 8 important endpoints, then split some:

# Server 1-8: Different endpoints (preferred)
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --node-id server1
python endpoint_processor.py --endpoint PlayerGameLogs --node-id server2
# ... etc

# Server 9-10: Split a high-volume endpoint
python endpoint_processor.py --endpoint SomeHighVolumeEndpoint --param-start 0 --param-end 5000 --node-id server9
python endpoint_processor.py --endpoint SomeHighVolumeEndpoint --param-start 5000 --param-end 10000 --node-id server10
```

## Key Benefits of This Approach

1. **Rate Limit Distribution**: Each server uses a different IP address, so API rate limits are per-IP
2. **Complete Data Integrity**: Each endpoint call processes all its dataframes together
3. **Parallel Processing**: Multiple endpoints or parameter ranges can run simultaneously
4. **Resumable**: If a server fails, it can resume from where it left off
5. **Scalable**: Add more servers to handle more endpoints or split parameter ranges further

## What Each Node Actually Does

When you run:
```bash
python endpoint_processor.py --endpoint BoxScoreAdvancedV3 --param-start 0 --param-end 100 --node-id node1
```

The node will:
1. Get game_ids 0-99 from the master games table
2. For each game_id:
   - Call `BoxScoreAdvancedV3(game_id=X)`
   - Get multiple dataframes back
   - Insert each dataframe into its appropriate table
   - Wait for rate limit delay
3. Log progress and errors
4. Continue until all 100 games are processed

This ensures that ALL data from each endpoint call is captured and stored properly.
