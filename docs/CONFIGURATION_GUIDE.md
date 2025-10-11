# NBA Data Collection System - Configuration Guide

## 🎯 **Configuration Overview**

The NBA data collection system uses a multi-layered configuration approach for maximum flexibility and maintainability.

---

## 📁 **Configuration Files**

### **1. Endpoint Configuration** (`config/endpoint_config.json`)
**Purpose**: Master definition of all NBA API endpoints with metadata

```json
{
  "endpoints": {
    "BoxScoreAdvancedV3": {
      "priority": "high",                    // Processing priority (high/medium/low)
      "required_params": ["game_id"],        // Required API parameters
      "latest_version": true,                // True for latest version endpoints
      "policy": "stacking",                  // Data handling policy
      "master": "games"                      // Master table dependency (optional)
    }
  }
}
```

**Key Fields**:
- `priority`: Determines processing order and profile inclusion
- `required_params`: Exact parameter names required by NBA API
- `latest_version`: Used for version filtering (eliminates V2/V3 duplicates)
- `master`: Links endpoints to master table dependencies ("games", "players", "teams")

### **2. Run Configuration** (`config/run_config.json`)
**Purpose**: Profile-based execution settings for different use cases

```json
{
  "collection_profiles": {
    "high_priority": {
      "description": "High priority endpoints only (latest versions)",
      "filter": "priority:high",             // Endpoint selection filter
      "latest_only": true,                   // Include only latest versions
      "slurm_config": {
        "time": "24:00:00",                  // SLURM time limit
        "mem_per_cpu": "4GB",                // Memory allocation
        "rate_limit": 0.2                    // Seconds between API calls
      }
    }
  }
}
```

**Profile Types**:
- `test`: Quick testing with 3 endpoints
- `high_priority`: Most important endpoints (21 endpoints)
- `full`: All available endpoints (comprehensive collection)

### **3. Database Configuration** (`config/database_config.json`)
**Purpose**: Database connection and table naming settings

```json
{
  "connection": {
    "host": "your-rds-host.amazonaws.com",
    "port": 5432,
    "database": "nba_data",
    "user": "your_username",
    "password_env": "DB_PASSWORD"            // Environment variable name
  },
  "table_naming": {
    "prefix": "nba_",                        // Table name prefix
    "master_tables": {
      "games": "master_nba_games",
      "players": "master_nba_players", 
      "teams": "master_nba_teams"
    }
  }
}
```

### **4. League Configuration** (`config/leagues_config.json`)
**Purpose**: Multi-league support settings

```json
{
  "leagues": {
    "NBA": {
      "league_id": "00",
      "seasons": {
        "start_year": 1996,
        "current_season": "2024-25"
      }
    }
  }
}
```

---

## 🔧 **Configuration Management**

### **Endpoint Version Filtering**
**Purpose**: Ensure only latest versions of endpoints are processed

**Implementation**:
1. Set `latest_version: true` for current endpoint versions
2. Set `latest_version: false` for outdated versions
3. Use `latest_only: true` in run profiles

**Example**:
```json
"BoxScoreAdvancedV2": {"latest_version": false},  // Will be filtered out
"BoxScoreAdvancedV3": {"latest_version": true}    // Will be included
```

### **Master Table Dependencies**
**Purpose**: Ensure master tables are processed before dependent endpoints

**Configuration**:
```json
"LeagueGameFinder": {"master": "games"},      // Creates master_nba_games
"CommonAllPlayers": {"master": "players"},    // Creates master_nba_players  
"CommonTeamYears": {"master": "teams"},       // Creates master_nba_teams
"PlayerGameLogs": {"required_params": ["player_id"]}  // Depends on players master
```

### **Parameter System Configuration**
**Purpose**: Define comprehensive parameter combinations for complete data coverage

**Built-in Parameter Generation**:
- **Seasons**: 29 seasons (1996-97 to 2024-25)
- **Season Types**: 6 types (Regular Season, Playoffs, Pre Season, Preseason, All Star, IST)
- **League ID**: Defaults to "00" (NBA)
- **Player/Team IDs**: Derived from master tables

---

## 🎮 **Common Configuration Tasks**

### **Adding a New Endpoint**
1. Add to `endpoint_config.json`:
```json
"NewEndpointV1": {
  "priority": "medium",
  "required_params": ["season", "league_id"],
  "latest_version": true,
  "policy": "stacking"
}
```

2. Test the endpoint:
```bash
python src/nba_data_processor.py --endpoint NewEndpointV1 --test-mode
```

### **Creating a Custom Profile**
1. Add to `run_config.json`:
```json
"custom_profile": {
  "description": "Custom endpoint selection",
  "endpoints": ["BoxScoreAdvancedV3", "PlayerGameLogs"],  // Specific endpoints
  "slurm_config": {
    "time": "12:00:00",
    "mem_per_cpu": "2GB", 
    "rate_limit": 0.3
  }
}
```

2. Use the profile:
```bash
python batching/scripts/get_endpoints.py custom_profile
```

### **Adjusting Performance Settings**
**Rate Limiting** (seconds between API calls):
- `0.15`: Aggressive (400 calls/minute)
- `0.2`: Balanced (300 calls/minute) 
- `0.5`: Conservative (120 calls/minute)

**Memory Allocation**:
- `2GB`: Light endpoints (single season)
- `4GB`: Standard endpoints (multiple seasons)
- `8GB`: Heavy endpoints (all players/teams)

### **Database Connection Setup**
1. Set environment variable:
```bash
export DB_PASSWORD="your_password"
```

2. Update `database_config.json` with RDS details

3. Test connection:
```bash
python src/nba_data_processor.py --test-connection
```

---

## 🔍 **Configuration Validation**

### **Verify Endpoint Configuration**
```bash
# Check for version conflicts
python batching/scripts/analyze_versions.py

# Validate endpoint selection
python batching/scripts/get_endpoints.py high_priority
```

### **Test Parameter Generation**
```bash
# Test comprehensive parameter system
python tests/test_comprehensive_params.py

# Validate season type coverage
python tests/test_season_types.py
```

### **Check Master Table Dependencies**
```bash
# Verify master table configuration
python -c "
import json
with open('config/endpoint_config.json', 'r') as f:
    config = json.load(f)
masters = {ep: cfg.get('master') for ep, cfg in config['endpoints'].items() if 'master' in cfg}
print('Master Table Endpoints:')
for ep, master in masters.items():
    print(f'  {ep} -> master_{master}')
"
```

---

## 🚀 **Production Configuration**

### **Recommended High-Priority Profile**
```json
"production_high": {
  "description": "Production high-priority with optimized settings",
  "filter": "priority:high",
  "latest_only": true,
  "slurm_config": {
    "time": "24:00:00",      // Full day for comprehensive collection
    "mem_per_cpu": "4GB",    // Adequate for most endpoints
    "rate_limit": 0.2        // Balanced performance/stability
  }
}
```

### **System Requirements**
- **Database**: PostgreSQL 12+ with sufficient storage for NBA historical data
- **Compute**: SLURM cluster or single machine with 8+ GB RAM
- **Network**: Stable internet connection for NBA API access
- **Storage**: ~100GB+ for complete historical dataset

This configuration guide provides all the settings needed to customize the NBA data collection system for different use cases while maintaining optimal performance and data quality.