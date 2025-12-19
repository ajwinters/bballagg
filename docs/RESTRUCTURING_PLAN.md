# NBA Data Collection System - Restructuring Plan
**Date**: November 28, 2025  
**Based On**: [CODE_REVIEW_2025-11-28.md](CODE_REVIEW_2025-11-28.md)  
**Status**: Proposed - Awaiting Approval

---

## 🎯 Restructuring Goals

1. **Eliminate code duplication** - Consolidate overlapping functionality
2. **Improve file organization** - Clear, logical structure
3. **Enhance maintainability** - Single responsibility, clear boundaries
4. **Remove technical debt** - Clean up archives, fix hardcoded values
5. **Prepare for testing** - Structure that supports comprehensive testing

---

## 📁 Proposed New Structure

### Target File Organization

```
thebigone/
├── src/                           # Core application code
│   ├── __init__.py               # Package initialization
│   ├── core/                     # Core processing logic
│   │   ├── __init__.py
│   │   ├── processor.py          # Main NBA data processor (consolidated)
│   │   ├── parameter_resolver.py # Parameter resolution logic
│   │   └── column_mapper.py     # Column name standardization
│   ├── database/                 # Database operations
│   │   ├── __init__.py
│   │   ├── connection.py         # RDS connection management
│   │   ├── operations.py         # CRUD operations
│   │   └── master_tables.py      # Master table management
│   ├── models/                   # Data models and schemas
│   │   ├── __init__.py
│   │   ├── endpoint.py           # Endpoint configuration model
│   │   └── config.py             # Configuration validation
│   ├── utils/                    # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging_config.py     # Logging setup
│   │   ├── validators.py         # Input validation
│   │   └── helpers.py            # Common helper functions
│   └── cli/                      # Command-line interface
│       ├── __init__.py
│       └── main.py               # CLI entry point
│
├── config/                       # Configuration files (no changes)
│   ├── endpoint_config.json
│   ├── database_config.json
│   ├── leagues_config.json
│   ├── parameter_mappings.json
│   └── run_config.json
│
├── scripts/                      # Operational scripts
│   ├── __init__.py
│   ├── setup_database.py         # Database initialization
│   ├── validate_config.py        # Configuration validation
│   └── health_check.py           # System health check
│
├── batching/                     # SLURM job management (minimal changes)
│   ├── jobs/                     # Job submission scripts
│   │   ├── submit_distributed.sh
│   │   ├── single_endpoint.sh
│   │   └── masters.sh
│   ├── scripts/                  # Job management utilities
│   │   ├── get_endpoints.py
│   │   ├── analyze_versions.py
│   │   └── monitor.py
│   └── templates/                # SLURM templates
│       └── job_template.sh
│
├── tests/                        # Comprehensive test suite
│   ├── __init__.py
│   ├── conftest.py               # Pytest configuration
│   ├── unit/                     # Unit tests
│   │   ├── __init__.py
│   │   ├── test_processor.py
│   │   ├── test_parameter_resolver.py
│   │   ├── test_column_mapper.py
│   │   └── test_validators.py
│   ├── integration/              # Integration tests
│   │   ├── __init__.py
│   │   ├── test_database.py
│   │   ├── test_api_calls.py
│   │   └── test_end_to_end.py
│   └── fixtures/                 # Test data and fixtures
│       ├── sample_configs.json
│       └── sample_api_responses.json
│
├── docs/                         # Documentation (enhanced)
│   ├── README.md
│   ├── ARCHITECTURE.md
│   ├── CONFIGURATION_GUIDE.md
│   ├── IMPLEMENTATION_SUMMARY.md
│   ├── CODE_REVIEW_2025-11-28.md      # ← New
│   ├── RESTRUCTURING_PLAN.md          # ← This file
│   ├── DEPLOYMENT.md                  # ← New
│   ├── TESTING_GUIDE.md               # ← New
│   ├── API_DOCUMENTATION.md           # ← New
│   └── reference/
│       ├── JOB_MANAGEMENT.md
│       └── PARAMETER_SYSTEM.md
│
├── notebooks/                    # Analysis notebooks
│   ├── README.md                 # Notebook index
│   ├── exploration/              # Data exploration
│   └── validation/               # Data validation
│
├── archive/                      # Archived code (moved from _temp)
│   ├── README.md                 # Archive index
│   ├── 2024-09/                  # Date-organized archives
│   │   ├── old_database_manager.py
│   │   └── legacy_scripts/
│   └── notebooks/                # Old notebooks
│
├── logs/                         # Application logs
│   └── .gitkeep
│
├── .github/                      # GitHub configuration
│   ├── workflows/                # CI/CD workflows
│   │   ├── tests.yml            # Run tests on PR
│   │   ├── lint.yml             # Code quality checks
│   │   └── docs.yml             # Documentation checks
│   └── ISSUE_TEMPLATE.md
│
├── .gitignore                    # Enhanced git ignore
├── .env.example                  # Environment variable template
├── pyproject.toml                # Python project configuration
├── setup.py                      # Package setup
├── requirements.txt              # Core dependencies
├── requirements-dev.txt          # Development dependencies
├── README.md                     # Main project README
├── AI_CONTEXT.md                 # AI agent context
└── TODO.md                       # Project TODO list
```

---

## 🔄 Migration Plan

### Phase 1: Security & Cleanup (Week 1)

#### Step 1.1: Remove Hardcoded Credentials
**Priority**: 🔴 CRITICAL  
**Estimated Time**: 30 minutes  

```bash
# Actions:
1. Create .env.example file
2. Update database_manager.py to use environment variables
3. Document environment setup in README
4. Add .env to .gitignore
```

**Files to modify**:
- [`src/database_manager.py`](../src/database_manager.py) - Remove hardcoded password
- Create `.env.example`
- Update [`.gitignore`](../.gitignore)

**Validation**:
```python
# Verify no hardcoded credentials remain
git grep -i "password.*=" src/
git grep -i "CharlesBark" .
```

#### Step 1.2: Archive Cleanup
**Priority**: 🔴 HIGH  
**Estimated Time**: 2 hours  

```bash
# Actions:
1. Create archive/ directory structure
2. Move _temp/ contents to archive/ with dates
3. Create archive README documenting what was archived
4. Remove _temp/ directory
5. Update documentation references
```

**Migration mapping**:
```
_temp/old_database_manager.py       → archive/2024-09/old_database_manager.py
_temp/archive/                      → archive/2024-early/
_temp/masters/                      → scripts/legacy/ (some may be current)
_temp/validation/                   → tests/integration/ (if still relevant)
```

**Decision points**:
- [ ] Review `_temp/masters/` scripts - are any still in use?
- [ ] Check `_temp/validation/` - migrate to tests or archive?

### Phase 2: Code Consolidation (Week 2)

#### Step 2.1: Create New Directory Structure
**Priority**: 🟡 HIGH  
**Estimated Time**: 1 hour  

```bash
# Create new directories
mkdir -p src/{core,database,models,utils,cli}
mkdir -p tests/{unit,integration,fixtures}
mkdir -p scripts
mkdir -p batching/{jobs,scripts,templates}
mkdir -p archive
mkdir -p .github/workflows

# Create __init__.py files
touch src/__init__.py
touch src/{core,database,models,utils,cli}/__init__.py
touch tests/__init__.py
touch tests/{unit,integration}/__init__.py
```

#### Step 2.2: Consolidate Endpoint Processors
**Priority**: 🟡 HIGH  
**Estimated Time**: 6-8 hours  

**Analysis**:
- [`nba_data_processor.py`](../src/nba_data_processor.py) - 1644 lines, comprehensive
- [`endpoint_processor.py`](../src/endpoint_processor.py) - 1234 lines, similar functionality

**Decision**: Keep `nba_data_processor.py` as primary, extract utilities

**Actions**:
1. Analyze both files for unique functionality
2. Create `src/core/processor.py` based on `nba_data_processor.py`
3. Extract parameter logic to `src/core/parameter_resolver.py`
4. Extract column mapping to `src/core/column_mapper.py`
5. Move utilities to `src/utils/`
6. Update all imports
7. Archive old `endpoint_processor.py`

**Code organization**:
```python
# src/core/processor.py
class NBADataProcessor:
    """Main processor - orchestration only"""
    def __init__(self, config):
        self.param_resolver = ParameterResolver(config)
        self.column_mapper = ColumnMapper(config)
        self.db = DatabaseManager(config)
    
    def process_endpoint(self, endpoint_name):
        # High-level orchestration
        pass

# src/core/parameter_resolver.py  
class ParameterResolver:
    """Parameter resolution logic - already exists, enhance"""
    pass

# src/core/column_mapper.py
class ColumnMapper:
    """Column name standardization - extract from processor"""
    pass
```

#### Step 2.3: Consolidate Database Management
**Priority**: 🟡 MEDIUM  
**Estimated Time**: 4-6 hours  

**Analysis**:
- [`rds_connection_manager.py`](../src/rds_connection_manager.py) - 495 lines, modern approach ✅
- [`database_manager.py`](../src/database_manager.py) - 766 lines, legacy implementation ⚠️

**Decision**: Keep `rds_connection_manager.py`, migrate useful functions from `database_manager.py`

**Actions**:
1. Rename `rds_connection_manager.py` → `src/database/connection.py`
2. Extract master table logic from `database_manager.py` → `src/database/master_tables.py`
3. Create `src/database/operations.py` for CRUD operations
4. Update imports throughout codebase
5. Archive old `database_manager.py`

**Code organization**:
```python
# src/database/connection.py (from rds_connection_manager.py)
class DatabaseConnection:
    """Connection management with sleep/wake detection"""
    pass

# src/database/master_tables.py (from database_manager.py)
class MasterTableManager:
    """Master table creation and updates"""
    pass

# src/database/operations.py (new)
class DatabaseOperations:
    """CRUD operations for endpoint data"""
    pass
```

### Phase 3: Testing Infrastructure (Week 3)

#### Step 3.1: Set Up Testing Framework
**Priority**: 🟡 MEDIUM  
**Estimated Time**: 4 hours  

```bash
# Install dev dependencies
pip install pytest pytest-cov pytest-mock black flake8 mypy

# Create requirements-dev.txt
cat > requirements-dev.txt << EOF
pytest>=7.0.0
pytest-cov>=4.0.0
pytest-mock>=3.10.0
black>=23.0.0
flake8>=6.0.0
mypy>=1.0.0
responses>=0.23.0  # For mocking API calls
EOF

# Create pytest configuration
cat > pyproject.toml << EOF
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --cov=src --cov-report=html --cov-report=term"

[tool.black]
line-length = 100
target-version = ['py38']

[tool.mypy]
python_version = "3.8"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
EOF
```

#### Step 3.2: Write Initial Tests
**Priority**: 🟡 MEDIUM  
**Estimated Time**: 8-12 hours  

**Test coverage priorities**:
1. ✅ Configuration loading and validation
2. ✅ Parameter resolution logic
3. ✅ Column name mapping
4. ✅ Database connection management
5. ⚪ API call mocking
6. ⚪ End-to-end workflow

**Example test structure**:
```python
# tests/unit/test_parameter_resolver.py
import pytest
from src.core.parameter_resolver import ParameterResolver

class TestParameterResolver:
    @pytest.fixture
    def resolver(self):
        config = {...}  # Load test config
        return ParameterResolver(config)
    
    def test_resolve_season_parameter(self, resolver):
        result = resolver.resolve_season()
        assert result in format "YYYY-YY"
    
    def test_resolve_player_ids(self, resolver):
        # Test with mock database
        pass
```

#### Step 3.3: Set Up CI/CD
**Priority**: ⚪ LOW  
**Estimated Time**: 2-4 hours  

**GitHub Actions workflow**:
```yaml
# .github/workflows/tests.yml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.8'
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: pytest
      - run: black --check src/
      - run: flake8 src/
```

### Phase 4: Documentation & Polish (Week 4)

#### Step 4.1: Update Documentation
**Priority**: ⚪ MEDIUM  
**Estimated Time**: 6-8 hours  

**New documentation needed**:
1. **DEPLOYMENT.md** - Production deployment guide
2. **TESTING_GUIDE.md** - How to write and run tests
3. **API_DOCUMENTATION.md** - Auto-generated API docs
4. **MIGRATION_GUIDE.md** - For users of old structure

**Updates to existing docs**:
- [`README.md`](../README.md) - Update file structure section
- [`ARCHITECTURE.md`](../docs/ARCHITECTURE.md) - Reflect new organization
- [`AI_CONTEXT.md`](../AI_CONTEXT.md) - Update file paths

#### Step 4.2: Code Quality Tools
**Priority**: ⚪ MEDIUM  
**Estimated Time**: 4 hours  

```bash
# Format all code
black src/ tests/ scripts/

# Check linting
flake8 src/ --max-line-length=100

# Type checking
mypy src/ --strict

# Generate documentation
pdoc3 --html --output-dir docs/api src/
```

#### Step 4.3: Final Validation
**Priority**: ⚪ HIGH  
**Estimated Time**: 4 hours  

**Checklist**:
- [ ] All tests passing
- [ ] Code formatted and linted
- [ ] Documentation updated
- [ ] No hardcoded credentials
- [ ] Archive properly organized
- [ ] CLI working with new structure
- [ ] SLURM jobs functional
- [ ] Database operations validated

---

## 📋 Detailed File Migration Matrix

### Core Source Files

| Current Location | New Location | Action | Priority |
|-----------------|--------------|--------|----------|
| `src/nba_data_processor.py` | `src/core/processor.py` | Refactor & move | 🟡 HIGH |
| `src/endpoint_processor.py` | `archive/2024-09/` | Archive | 🟡 HIGH |
| `src/parameter_resolver.py` | `src/core/parameter_resolver.py` | Move | 🟡 MEDIUM |
| `src/rds_connection_manager.py` | `src/database/connection.py` | Move & rename | 🟡 MEDIUM |
| `src/database_manager.py` | Split into `src/database/master_tables.py` & archive | Refactor | 🟡 MEDIUM |

### Archive Migration

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `_temp/old_database_manager.py` | `archive/2024-09/old_database_manager.py` | Direct move |
| `_temp/archive/*` | `archive/2024-early/*` | Direct move |
| `_temp/masters/*` | Review → `scripts/legacy/` or archive | Needs review |
| `_temp/validation/*` | `tests/integration/` or archive | Needs review |

### Script Organization

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `database_cleanup.py` | `scripts/cleanup_database.py` | Move to scripts/ |
| `batching/scripts/*` | `batching/scripts/*` | Keep in place |
| New files needed | `scripts/setup_database.py` | Create |
| New files needed | `scripts/validate_config.py` | Create |

---

## 🎯 Success Criteria

### Phase 1 Complete When:
- [ ] No hardcoded credentials in source code
- [ ] `_temp/` directory removed
- [ ] `archive/` directory organized with README
- [ ] All code still functional

### Phase 2 Complete When:
- [ ] New directory structure created
- [ ] No duplicate processor files
- [ ] Single database manager approach
- [ ] All imports updated and working
- [ ] SLURM jobs functional with new paths

### Phase 3 Complete When:
- [ ] Pytest configured and running
- [ ] >50% code coverage on core modules
- [ ] CI/CD pipeline running
- [ ] Tests passing on all commits

### Phase 4 Complete When:
- [ ] All documentation updated
- [ ] Code formatted and linted
- [ ] API documentation generated
- [ ] Migration guide complete

---

## ⚠️ Risk Mitigation

### Risk 1: Breaking Changes
**Mitigation**:
- Work in feature branch
- Maintain backward compatibility where possible
- Test thoroughly before merging
- Create rollback plan

### Risk 2: Lost Functionality
**Mitigation**:
- Comprehensive code review before archiving
- Document all archived code
- Keep archive accessible
- Test all critical paths

### Risk 3: Import Path Changes
**Mitigation**:
- Use find/replace carefully
- Test after each major change
- Update docs immediately
- Create compatibility layer if needed

### Risk 4: SLURM Job Failures
**Mitigation**:
- Test jobs in dev environment first
- Update job scripts incrementally
- Keep old scripts until validated
- Document all path changes

---

## 📊 Timeline Summary

| Phase | Duration | Effort (Hours) | Priority |
|-------|----------|----------------|----------|
| **Phase 1**: Security & Cleanup | Week 1 | 8-12 | 🔴 CRITICAL |
| **Phase 2**: Code Consolidation | Week 2 | 16-24 | 🟡 HIGH |
| **Phase 3**: Testing Infrastructure | Week 3 | 16-24 | 🟡 MEDIUM |
| **Phase 4**: Documentation & Polish | Week 4 | 16-20 | ⚪ MEDIUM |
| **Total** | **4 weeks** | **56-80 hours** | - |

**Note**: Hours assume part-time work (10-20 hours/week)

---

## 🚀 Quick Start: First Steps

### Immediate Actions (Today)

```bash
# 1. Create feature branch
git checkout -b restructure/consolidation

# 2. Create .env.example
cat > .env.example << 'EOF'
# Database Configuration
DB_HOST=your-rds-host.amazonaws.com
DB_NAME=thebigone
DB_USER=your_username
DB_PASSWORD=your_password_here
DB_PORT=5432
DB_SSLMODE=require
EOF

# 3. Update .gitignore
echo ".env" >> .gitignore
echo "*.pyc" >> .gitignore
echo "__pycache__/" >> .gitignore
echo ".pytest_cache/" >> .gitignore
echo "htmlcov/" >> .gitignore

# 4. Create archive directory
mkdir -p archive/2024-09
echo "# Archived Code" > archive/README.md
echo "This directory contains code archived during the 2024-11 restructuring." >> archive/README.md

# 5. Move hardcoded credentials
# (Manual - update database_manager.py to use os.environ)
```

### Week 1 Checklist

- [ ] Day 1: Security fixes (credentials)
- [ ] Day 2: Archive cleanup planning
- [ ] Day 3: Move _temp/ to archive/
- [ ] Day 4: Test all functionality still works
- [ ] Day 5: Commit and create PR for Phase 1

---

## 📝 Notes for Implementation

### Configuration Migration
- Keep all JSON configs in `config/` - no changes needed
- Validate configs on startup
- Add schema validation in future

### Database Schema
- No database schema changes required
- Table names remain the same
- Migration is code organization only

### Backward Compatibility
- Consider creating `src/legacy/` with compatibility shims
- Allows gradual migration of external dependencies
- Can be removed after validation period

### Documentation
- Update all relative paths in markdown
- Test all documentation links
- Keep old docs until restructure complete

---

## ✅ Approval Checklist

Before beginning restructuring:

- [ ] **Review complete plan** with stakeholders
- [ ] **Backup current codebase** (git tag)
- [ ] **Create feature branch** for restructuring
- [ ] **Set up communication channel** for questions
- [ ] **Allocate time** for testing and validation
- [ ] **Prepare rollback plan** if needed

---

**Plan Created**: November 28, 2025  
**Created By**: AI Architect (Roo)  
**Status**: Awaiting Approval  
**Next Action**: Review and approve plan, then begin Phase 1