# NBA Data Collection System - AI Context Document

> **For AI Agents**: This document provides complete context for the NBA data collection system as of October 2025. Reference this for all future development sessions.

---

## 🏀 **System Overview**

**Purpose**: Comprehensive NBA data collection from NBA API → PostgreSQL database  
**Scale**: 136 endpoints, 29 seasons (1996-2025), 6 season types, complete historical coverage  
**Architecture**: Configuration-driven, SLURM-distributed, master table dependencies

### **Current Status (October 2025)**
✅ **Production-ready** with all major issues resolved  
✅ **Complete parameter coverage** (174 combinations per season-aware endpoint)  
✅ **Dynamic column mapping** (handles NBA API inconsistencies)  
✅ **Optimized performance** (27.6% faster via version filtering)  
✅ **Enhanced monitoring** (descriptive SLURM job names)

---

## 🔧 **Technical Architecture**

### **Core Components**
- **`src/nba_data_processor.py`** - Main processing engine with comprehensive parameter system
- **`config/endpoint_config.json`** - 136 endpoints with metadata (priority, params, versions)
- **`config/run_config.json`** - Execution profiles (test, high_priority, full)
- **`batching/`** - SLURM distributed processing scripts
- **Master Tables**: `master_nba_games`, `master_nba_players`, `master_nba_teams`

### **Key Features Implemented (October 2025)**

#### **1. Dynamic Column Mapping**
**Problem**: NBA API uses inconsistent column names (gameid vs game_id, personid vs player_id)  
**Solution**: `get_master_table_column_name()` method automatically detects variations
```python
# Handles gameid/game_id, personid/player_id, teamid/team_id automatically
def get_master_table_column_name(self, master_table: str, lookup_column: str) -> str
```

#### **2. Comprehensive Parameter System**
**Coverage**: 29 seasons × 6 season types = 174 combinations per endpoint  
**Season Types**: Regular Season, Playoffs, Pre Season, Preseason, All Star, IST
```python
def _build_complete_param_set(self, endpoint_name: str, config: dict) -> List[dict]:
    # Generates all parameter combinations for complete historical coverage
```

#### **3. Master Table Dependencies**
**Tables**: Games (LeagueGameFinder), Players (CommonAllPlayers), Teams (CommonTeamYears)  
**Logic**: Master endpoints process first, then dependent endpoints access their data

#### **4. Latest Version Filtering**
**Problem**: Processing both V2 and V3 endpoints caused duplicates  
**Solution**: `latest_version: true` filtering eliminates old versions  
**Impact**: High-priority endpoints reduced from 29 to 21 (27.6% performance gain)

#### **5. Enhanced SLURM Job Naming**
**Format**: `nba_<profile>_<endpoint>` (e.g., `nba_high_priority_PlayerGameLog`)  
**Benefit**: Easy job identification and management

---

## 📊 **System Specifications**

### **Endpoint Coverage**
- **Total**: 136 NBA API endpoints
- **High Priority**: 21 endpoints (latest versions only)
- **Parameter Combinations**: Up to 174 per season-aware endpoint
- **Historical Range**: 1996-97 to 2024-25 (29 seasons)

### **Processing Profiles**
- **test**: 3 endpoints for quick validation
- **high_priority**: 21 most important endpoints  
- **full**: All 136 endpoints (comprehensive collection)

### **Performance Characteristics**
- **API Rate Limiting**: 0.15-0.5 seconds between calls (configurable)
- **Memory**: 2-8GB per job depending on endpoint complexity
- **Processing Time**: 12-17 hours for complete high-priority historical backfill
- **Scalability**: Linear with SLURM node count

---

## 🚨 **Common Issues & Solutions**

### **Database Connection**
**Symptoms**: Connection refused, authentication failed  
**Check**: `export DB_PASSWORD="password"` and RDS accessibility  
**Test**: `python -c "from src.rds_connection_manager import RDSConnectionManager; ..."`

### **Missing Master Tables**
**Symptoms**: Endpoints requiring player/team/game IDs fail  
**Fix**: Run master endpoints first: `sbatch batching/nba_masters.sh high_priority`  
**Verify**: Check `master_nba_games`, `master_nba_players`, `master_nba_teams` exist

### **Column Name Errors**
**Symptoms**: "column not found" errors, KeyError for game_id/player_id  
**Solution**: Dynamic column mapping handles this automatically (no action needed)

### **SLURM Job Issues**
**Memory**: Increase `--mem=8GB` for heavy endpoints  
**Time**: Increase `--time=48:00:00` for comprehensive runs  
**Monitor**: `squeue -u $(whoami) | grep nba_`  
**Cancel**: `scancel -u $(whoami) -n "*nba*"`

### **API Rate Limiting**
**Symptoms**: 429 errors, "Too Many Requests"  
**Fix**: Increase `rate_limit` in `config/run_config.json` (0.2 → 0.3+ seconds)

---

## 🎮 **Common Operations**

### **Basic Testing**
```bash
# Test single endpoint locally
python src/nba_data_processor.py --endpoint CommonAllPlayers --test-mode --max-items 5
```

### **Distributed Processing**
```bash
# High-priority endpoints (recommended)
./batching/submit_distributed_nba_jobs.sh high_priority

# Monitor progress
squeue -u $(whoami) | grep nba_

# Cancel all jobs
scancel -u $(whoami) -n "*nba*"
```

### **Configuration**
```bash
# Check endpoint selection
python batching/scripts/get_endpoints.py high_priority

# Validate system health
python batching/scripts/analyze_versions.py
```

---

## 📁 **Critical Files**

### **Configuration Files**
- **`config/endpoint_config.json`** - Endpoint definitions with `latest_version` flags
- **`config/run_config.json`** - Execution profiles with `latest_only: true`
- **`config/database_config.json`** - Database connection settings

### **Processing Scripts** 
- **`src/nba_data_processor.py`** - Main processor with all recent enhancements
- **`batching/submit_distributed_nba_jobs.sh`** - Master job submission script
- **`batching/single_endpoint.sh`** - Individual endpoint processing

### **Validation Tools**
- **`batching/scripts/analyze_versions.py`** - Version conflict detection
- **`tests/test_comprehensive_params.py`** - Parameter system validation

---

## 🎯 **Development Context**

### **Recent Major Changes (October 2025)**
1. **Fixed failing distributed jobs** - Column mapping and master table issues resolved
2. **Implemented comprehensive parameters** - Complete historical coverage (174 combinations)
3. **Added team master table** - CommonTeamYears endpoint configured as teams master
4. **Optimized endpoint selection** - Latest version filtering eliminates duplicates
5. **Enhanced job monitoring** - Descriptive SLURM job names with endpoint information
6. **Added IST support** - In-Season Tournament included in season types

### **System Maturity**
The system has evolved from **proof-of-concept** to **production-ready** with:
- Robust error handling and retry logic
- Complete parameter validation and generation
- Efficient distributed processing with dependency management
- Comprehensive logging and monitoring capabilities
- Well-tested configuration and validation tools

### **Future Enhancement Areas**
- WNBA/G-League support (infrastructure exists)
- Real-time data updates (incremental processing)
- Advanced analytics and data validation
- Performance optimization for larger datasets

---

## 💡 **AI Agent Instructions**

When working with this system:

1. **Always check master table dependencies** before processing endpoints that require player/team/game IDs
2. **Use test mode first** (`--test-mode --max-items 5`) before full processing
3. **Reference endpoint_config.json** for parameter requirements and priorities
4. **Check SLURM jobs** with descriptive names for debugging distributed issues
5. **Consider parameter combinations** - season-aware endpoints generate many combinations
6. **Verify latest_version filtering** - should only process newest endpoint versions

**Database Schema**: Tables follow `nba_<endpoint>_<dataframe>` naming pattern with master tables as dependencies.

**Performance**: System designed for batch processing with rate limiting. Adjust `rate_limit` config based on API response and error rates.

This document provides complete context for development, troubleshooting, and enhancement of the NBA data collection system.