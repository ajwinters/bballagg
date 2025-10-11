# Parameter System Reference

## 🎯 **NBA Parameter System Overview**

The parameter system automatically generates comprehensive parameter combinations for complete historical NBA data coverage.

---

## 🔧 **Core Parameter Types**

### **1. Season Parameters**
**Range**: 1996-97 to 2024-25 (29 seasons total)

```python
# Generated automatically
seasons = ['1996-97', '1997-98', '1998-99', ..., '2024-25']
```

**Usage**: All season-aware endpoints receive full historical coverage

### **2. Season Type Parameters**  
**Types**: 6 comprehensive season types

```python
season_types = [
    'Regular Season',    # Standard regular season games
    'Playoffs',         # Playoff games and series
    'Pre Season',       # Preseason (some endpoints use this format)
    'Preseason',        # Preseason (alternative format)
    'All Star',         # All-Star Weekend events
    'IST'              # In-Season Tournament (added 2023-24)
]
```

### **3. League ID Parameters**
**Default**: "00" (NBA)
**Future**: Support for WNBA ("10") and G-League ("20")

### **4. Master Table Parameters**
**Player IDs**: Derived from `master_nba_players` table
**Team IDs**: Derived from `master_nba_teams` table  
**Game IDs**: Derived from `master_nba_games` table

---

## 📊 **Parameter Combination Logic**

### **Season + Season Type Endpoints**
**Combinations**: 29 seasons × 6 season types = **174 combinations**

**Example Endpoints**:
- `LeagueStandingsV3`
- `TeamEstimatedMetrics` 
- `PlayerEstimatedMetrics`
- `BoxScoreAdvancedV3` (when game_id not specified)

### **Player-Based Endpoints**
**Combinations**: ~500 active players × 29 seasons = **~14,500 combinations**

**Example Endpoints**:
- `PlayerGameLogs`
- `PlayerDashboardByGeneralSplits`
- `PlayerCareerStats`

### **Team-Based Endpoints**
**Combinations**: 30 teams × 29 seasons = **870 combinations**

**Example Endpoints**:
- `TeamGameLogs`
- `TeamDashboardByGeneralSplits`
- `TeamPlayerDashboard`

### **Game-Based Endpoints**
**Combinations**: Individual game processing (1,000+ games per season)

**Example Endpoints**:
- `BoxScoreAdvancedV3`
- `PlayByPlayV3`
- `HustleStatsBoxScore`

---

## 🏗️ **Implementation Details**

### **Parameter Generation Methods**

```python
def _get_all_seasons(self) -> List[str]:
    """Generate all NBA seasons from 1996-97 to current"""
    current_year = datetime.now().year
    seasons = []
    for year in range(1996, current_year + 1):
        seasons.append(f"{year}-{str(year + 1)[2:]}")
    return seasons

def _get_all_season_types(self) -> List[str]:
    """All NBA season types including IST"""
    return ['Regular Season', 'Playoffs', 'Pre Season', 'Preseason', 'All Star', 'IST']

def _build_complete_param_set(self, endpoint_name: str, config: dict) -> List[dict]:
    """Generate all parameter combinations for comprehensive coverage"""
    required_params = config.get('required_params', [])
    combinations = []
    
    if 'season' in required_params:
        seasons = self._get_all_seasons()
        if 'season_type' in required_params:
            # Season × Season Type combinations
            season_types = self._get_all_season_types()
            for season in seasons:
                for season_type in season_types:
                    combinations.append({
                        'season': season,
                        'season_type': season_type,
                        'league_id': '00'
                    })
        else:
            # Season only
            for season in seasons:
                combinations.append({
                    'season': season,
                    'league_id': '00'
                })
    
    return combinations
```

### **Master Table Integration**

```python
def get_master_table_column_name(self, master_table: str, lookup_column: str) -> str:
    """Dynamically detect actual column name in master table"""
    table_name = self.league_config['master_tables'][master_table]
    
    # Check if table exists
    if not self.db_manager.table_exists(table_name):
        return None
    
    # Get actual column names
    columns = self.db_manager.get_table_columns(table_name)
    
    # Map common variations
    column_mappings = {
        'player_id': ['player_id', 'personid', 'person_id'],
        'team_id': ['team_id', 'teamid'], 
        'game_id': ['game_id', 'gameid']
    }
    
    if lookup_column in column_mappings:
        for variant in column_mappings[lookup_column]:
            if variant in columns:
                return variant
    
    return lookup_column if lookup_column in columns else None
```

---

## 📋 **Parameter Validation**

### **Required Parameter Detection** 
The system automatically detects required parameters for each endpoint:

```json
{
  "PlayerGameLogs": {
    "required_params": ["player_id", "season", "season_type"],
    "priority": "high"
  },
  "TeamGameLogs": {
    "required_params": ["team_id", "season"],
    "priority": "high"  
  }
}
```

### **Parameter Combination Testing**
```bash
# Test parameter generation
python tests/test_comprehensive_params.py

# Validate season type coverage
python tests/test_season_types.py

# Check specific endpoint parameters
python src/nba_data_processor.py --endpoint PlayerGameLogs --test-mode --max-items 1
```

---

## 🎯 **Usage Examples**

### **Endpoint with Season + Season Type**
```python
# Generates 174 combinations (29 × 6)
endpoint = "LeagueStandingsV3"
params = processor._build_complete_param_set(endpoint, config)
# Results in: 
# [{'season': '1996-97', 'season_type': 'Regular Season', 'league_id': '00'},
#  {'season': '1996-97', 'season_type': 'Playoffs', 'league_id': '00'},
#  ...]
```

### **Player-Based Endpoint**
```python
# Gets all players from master table, then generates season combinations
endpoint = "PlayerGameLogs"
player_ids = processor.get_master_table_ids('players', 'player_id')
# Generates ~14,500 combinations (500 players × 29 seasons)
```

### **Game-Based Endpoint**
```python
# Processes individual games from master_nba_games
endpoint = "BoxScoreAdvancedV3"
game_ids = processor.get_master_table_ids('games', 'game_id')
# Processes each game individually
```

---

## 🚨 **Common Parameter Issues**

### **Missing Master Tables**
**Problem**: Player/Team endpoints fail without master tables
**Solution**: Ensure master endpoints run first:
```bash
# Check master table status
python -c "
from src.rds_connection_manager import RDSConnectionManager
import json
with open('config/database_config.json') as f:
    db_config = json.load(f)
db = RDSConnectionManager(db_config)
for table in ['master_nba_games', 'master_nba_players', 'master_nba_teams']:
    exists = db.table_exists(table)
    print(f'{table}: {\"✅ EXISTS\" if exists else \"❌ MISSING\"}')
"
```

### **Parameter Name Variations**
**Problem**: NBA API uses inconsistent parameter names
**Solution**: Dynamic column mapping handles variations automatically:
- `game_id` ↔ `gameid`
- `player_id` ↔ `personid` 
- `team_id` ↔ `teamid`

### **Season Type Coverage**
**Problem**: Missing data for specific season types
**Solution**: IST addition ensures complete coverage:
```python
# Verify all season types included
season_types = processor._get_all_season_types()
assert 'IST' in season_types  # In-Season Tournament included
assert len(season_types) == 6  # All 6 types covered
```

---

## 📊 **Performance Considerations**

### **Combination Scaling**
- **Season-only endpoints**: 29 combinations
- **Season + Season Type**: 174 combinations  
- **Player + Season**: ~14,500 combinations
- **Team + Season**: 870 combinations

### **Processing Optimization**
- **Batch Size**: Configurable via `max_items_per_endpoint`
- **Rate Limiting**: Configurable via profile settings
- **Parallel Processing**: SLURM distribution across multiple nodes
- **Incremental Updates**: Smart detection of missing combinations

This parameter system ensures comprehensive historical coverage while maintaining optimal performance and data quality.