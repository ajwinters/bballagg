# NBA Data Collection System

> **Last Updated**: October 2025 - Production-ready system with comprehensive parameter coverage, dynamic column mapping, and distributed SLURM processing

## 🏀 **Project Overview**

This system collects comprehensive NBA data from the NBA API and stores it in a PostgreSQL RDS database. It processes 136 NBA API endpoints with intelligent parameter resolution, automatic retry logic, and robust error handling.

**Current Status**: ✅ Production-ready with complete historical coverage (1996-2025)

### **Key Capabilities** 
- **Master Tables**: Complete dependency resolution for Games, Players, Teams
- **Endpoint Processing**: 136 NBA API endpoints with comprehensive parameter coverage
- **Historical Coverage**: 29 seasons × 6 season types = 174 combinations per endpoint
- **Distributed Processing**: SLURM-based HPC computing with intelligent job naming
- **Dynamic Column Mapping**: Automatic handling of NBA API column name variations
- **Version Filtering**: Latest endpoint versions only (eliminates V2/V3 duplicates)
- **Configuration-Driven**: JSON-based system configuration and endpoint management

---

## � **Quick Start**

### **Prerequisites**
- Python 3.8+ with virtual environment
- PostgreSQL database (AWS RDS recommended)
- SLURM cluster access (optional, for distributed processing)

### **Basic Setup**
```bash
# Clone and setup
git clone <repository_url>
cd thebigone
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# Configure database
export DB_PASSWORD="your_password"
# Edit config/database_config.json with your RDS details

# Test single endpoint
python src/nba_data_processor.py --endpoint CommonAllPlayers --test-mode --max-items 5
```

### **Distributed Processing**
```bash
# Submit high-priority endpoints (recommended)
./batching/submit_distributed_nba_jobs.sh high_priority

# Monitor jobs
squeue -u $(whoami) | grep nba_

# Cancel if needed
scancel -u $(whoami) -n "*nba*"
```

---

## 📚 **Documentation**

### **Essential Guides**
- **[📖 Complete Documentation](docs/README.md)** - Full documentation index
- **[⚙️ Configuration Guide](docs/CONFIGURATION_GUIDE.md)** - System setup and configuration
- **[🔧 Troubleshooting](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[🎯 Implementation Summary](docs/IMPLEMENTATION_SUMMARY.md)** - Recent features and changes

### **Reference Material**
- **[📋 Job Management](docs/reference/JOB_MANAGEMENT.md)** - SLURM job operations
- **[🔀 Parameter System](docs/reference/PARAMETER_SYSTEM.md)** - Parameter handling reference
- **[🏗️ System Architecture](ARCHITECTURE.md)** - Architecture overview

---

## 📁 **Project Structure**

```
thebigone/
├── 📁 config/                    # All configuration files
│   ├── database_config.json      # Database connection settings
│   ├── endpoints_config.py       # NBA API endpoint definitions (40+ endpoints)
│   ├── leagues_config.json       # NBA/WNBA/G-League settings
│   └── run_config.json          # Batch job profiles (test/production)
├── 📁 src/                       # Core processing code
│   ├── rds_connection_manager.py # Database operations + connection management
│   ├── parameter_resolver.py    # Dynamic parameter resolution for API calls
│   ├── endpoint_processor.py    # Main processing engine
│   ├── database_manager.py      # Master table management
│   ├── dataframe_name_matcher.py # Smart table naming
│   ├── player_dashboard_enhancer.py # Player data enhancement
│   └── *_collector.py          # League-specific collectors
├── 📁 batching/                  # SLURM job management
│   ├── nba_jobs.sh              # Job submission and monitoring
│   ├── slurm_nba_collection.sh  # SLURM job template
│   └── setup.sh                 # Environment setup
├── 📁 notebooks/                # Data exploration and validation
└── 📁 tests/                    # Testing utilities
```

---

## 🔧 **Core Components**

### **1. RDS Connection Manager** (`src/rds_connection_manager.py`)
- **Purpose**: Unified database operations with intelligent connection management
- **Features**: Sleep/wake detection, automatic reconnection, batch operations
- **Key Methods**: `clean_column_names()`, `insert_dataframe_to_rds()`, `check_table_exists()`

### **2. Parameter Resolver** (`src/parameter_resolver.py`)
- **Purpose**: Dynamic parameter resolution for NBA API endpoints
- **Features**: Master table lookups, missing ID detection, validation
- **Key Methods**: `resolve_parameters_comprehensive()`, `find_all_missing_ids()`

### **3. Endpoint Processor** (`src/endpoint_processor.py`)
- **Purpose**: Main processing engine for NBA API endpoints
- **Features**: Rate limiting, error handling, data validation, table creation
- **Processing Flow**: Parameter resolution → API call → Data validation → Database insertion

### **4. Configuration System**
- **Endpoints**: Each endpoint has parameters, frequency, priority, data type
- **Leagues**: Season formats, active periods, table prefixes
- **Run Modes**: Test (small batches) vs Production (full collection)

---

## ⚙️ **Configuration Details**

### **Endpoint Configuration Structure**
```python
{
    'endpoint': 'BoxScoreAdvancedV3',
    'description': 'Advanced box score stats',
    'parameters': {'game_id': 'from_mastergames'},
    'frequency': 'after_game_completion',
    'priority': 'high',
    'data_type': 'game_stats'
}
```

### **Parameter Sources**
- `from_mastergames`: Get game IDs from master games tables
- `from_masterplayers`: Get player IDs from master players tables  
- `from_masterplayers_all_seasons`: Get all player-season combinations
- `from_masterteams`: Get team IDs from master teams tables
- `current_season`: Auto-calculate current NBA season
- `dynamic_date_range`: Use recent date ranges
- Static values: Direct parameter values

### **Run Configuration Modes**
```json
{
  "test": {
    "description": "Quick test run with 3 endpoints",
    "endpoints": ["BoxScoreAdvancedV3", "BoxScoreTraditionalV2"],
    "rate_limit": 0.5,
    "sample_size": 10
  },
  "production": {
    "description": "Full collection of all high-priority endpoints",
    "rate_limit": 0.6,
    "comprehensive": true
  }
}
```

---

## 🚀 **Usage Patterns**

### **Local Development/Testing**
```bash
# Test single endpoint
python src/endpoint_processor.py --endpoint BoxScoreAdvancedV3 --test-mode

# Test database connection
python test_consolidated_system.py

# Run master table updates
python src/database_manager.py
```

### **Batch Processing (SLURM)**
```bash
cd batching/

# Submit test job
./nba_jobs.sh submit test

# Submit production job  
./nba_jobs.sh submit full

# Monitor jobs
./nba_jobs.sh status

# View logs
./nba_jobs.sh logs <job_id>
```

### **Configuration Updates**
```bash
# Edit endpoint configurations
vim config/endpoints_config.py

# Edit run profiles
vim config/run_config.json

# Test configuration changes
./nba_jobs.sh submit test
```

---

## 🎯 **Data Flow**

1. **Parameter Resolution**: `parameter_resolver.py` determines what IDs need processing
2. **Missing ID Detection**: Compare master tables vs endpoint tables to find gaps
3. **API Calls**: Rate-limited calls to NBA API with retry logic
4. **Data Processing**: Clean column names, validate data, enhance player dashboards
5. **Database Storage**: Create tables if needed, insert data with conflict handling
6. **Error Tracking**: Log failed API calls to prevent re-attempts

---

## 🔍 **Key Design Decisions**

### **Why Configuration-Driven?**
- **Scalability**: Easy to add new endpoints without code changes
- **Flexibility**: Different processing modes (test vs production)
- **Maintainability**: Centralized parameter management

### **Why Separate Parameter Resolution?**
- **Complexity**: 500+ lines of parameter logic needed isolation
- **Reusability**: Multiple processors can use same resolution logic
- **Testing**: Easier to unit test parameter logic separately

### **Why SLURM + Local Hybrid?**
- **Initial Collection**: SLURM handles massive historical data collection
- **Ongoing Updates**: Local processing handles daily/weekly incremental updates
- **Cost Efficiency**: Don't need cluster computing for small update jobs

---

## 📊 **Database Schema**

### **Master Tables**
- `nba_players`, `wnba_players`, `gleague_players`: Player biographical data
- `nba_games`, `wnba_games`, `gleague_games`: Game schedules and basic info
- `nba_teams`, `wnba_teams`, `gleague_teams`: Team information

### **Endpoint Tables**
- Named as: `nba_{endpoint_name}_{dataframe_name}`
- Example: `nba_boxscoreadvancedv3_team`, `nba_playerdashboard_overall`
- Automatic table creation based on DataFrame structure
- Enhanced with `player_id`, `season`, `collected_at` columns where applicable

---

## 🛠️ **Development Workflow**

### **Adding New Endpoints**
1. Add endpoint configuration to `config/endpoints_config.py`
2. Test with: `./nba_jobs.sh submit test`
3. Validate data in database
4. Add to production profile

### **Debugging Issues**
1. Check logs: `./nba_jobs.sh logs <job_id>`
2. Test locally: `python src/endpoint_processor.py --endpoint <name> --test-mode`
3. Validate parameters: Check `parameter_resolver.py` logic
4. Check database: Verify table structure and data

### **Performance Optimization**
1. Adjust rate limits in `config/run_config.json`
2. Optimize batch sizes in `rds_connection_manager.py`
3. Fine-tune SLURM resource allocation

---

## 📝 **Recent Changes Log**

### **September 6, 2025 - Major Architectural Consolidation**

#### **🔄 Restructuring Completed**
- **Consolidated Database Operations**: Merged 4 duplicate `allintwo.py` files into single `rds_connection_manager.py`
- **Extracted Parameter Logic**: Moved 500+ lines of parameter resolution from `endpoint_processor.py` to dedicated `parameter_resolver.py`
- **Organized Configuration**: Centralized all configs in `/config` folder with clear separation of concerns
- **Streamlined Source Code**: Flat `/src` structure with focused, single-responsibility modules

#### **🗂️ Directory Migration**
```
BEFORE: Scattered across masters/, endpoints/collectors/, shared/utils/
AFTER: Clean src/, config/, batching/, notebooks/ structure
```

#### **⚡ Enhanced Features**
- **Connection Management**: Added sleep/wake detection and automatic reconnection
- **Error Handling**: Robust retry logic with permanent error detection  
- **Configuration System**: JSON-based endpoint, league, and run configurations
- **Testing Framework**: Integrated validation with `test_consolidated_system.py`

#### **🧹 Code Cleanup**
- Removed 4 duplicate database utility files
- Updated all import statements to use new consolidated modules
- Eliminated redundant connection managers
- Standardized function signatures across modules

#### **✅ Validation**
- All tests passing: Directory structure, configuration loading, imports, database connections
- System ready for production endpoint configuration work
- SLURM batch processing operational

---

## 🎯 **AI Agent Instructions**

When working on this system:

1. **Always Reference**: This README for context and current architecture
2. **Configuration Changes**: Update `config/endpoints_config.py` for new endpoints
3. **Database Operations**: Use `rds_connection_manager.py` methods, not raw psycopg2
4. **Parameter Logic**: Modify `parameter_resolver.py` for new parameter sources
5. **Testing**: Run `test_consolidated_system.py` after major changes
6. **Error Handling**: Check `failed_api_calls` table for debugging recurring issues

### **Common Tasks**
- **Add Endpoint**: Update `endpoints_config.py` → Test → Validate data
- **Debug Failed Calls**: Check logs → Validate parameters → Test API response
- **Performance Issues**: Adjust rate limits → Optimize batch sizes → Monitor memory usage
- **New League**: Add to `leagues_config.json` → Update table prefixes → Test collection

---

## 📞 **Quick Reference**

| Task | Command |
|------|---------|
| Test system | `python test_consolidated_system.py` |
| Test endpoint | `python src/endpoint_processor.py --endpoint <name>` |
| Submit batch job | `cd batching && ./nba_jobs.sh submit test` |
| Check job status | `./nba_jobs.sh status` |
| View profiles | `./nba_jobs.sh profiles` |
| Database operations | `python src/database_manager.py` |

**Database**: PostgreSQL RDS on AWS  
**API**: NBA Stats API (nba_api package)  
**Compute**: SLURM cluster + local processing hybrid  
**Storage**: 40+ endpoint tables + 9 master tables across 3 leagues
