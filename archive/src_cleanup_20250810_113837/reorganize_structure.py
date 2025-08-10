"""
File Structure Reorganization

This script organizes the project to separate master data collection from endpoint processing:

NEW STRUCTURE:
/masters/                    # Master data collection system
├── collectors/              # Collection scripts
├── data/                   # Master data files
│   ├── comprehensive/      # Combined league data
│   └── leagues/           # League-separated data
├── config/                # Configuration files
└── scripts/               # Utility scripts

/endpoints/                 # Endpoint data collection system  
├── collectors/            # Endpoint collection scripts
├── data/                 # Endpoint data files
├── config/               # Endpoint configurations
└── results/              # Collection results

/shared/                   # Shared utilities
├── utils/                # Common utilities
└── config/              # Shared configurations
"""

import os
import shutil
import json
from datetime import datetime

class FileStructureReorganizer:
    """Reorganizes project files into logical structure"""
    
    def __init__(self, root_dir='.'):
        self.root_dir = root_dir
        self.reorganization_plan = {}
        self.backup_created = False
        
    def create_new_structure(self):
        """Create the new directory structure"""
        
        print("📁 CREATING NEW FILE STRUCTURE")
        print("=" * 50)
        
        # Define new directory structure
        new_dirs = [
            # Masters system
            'masters',
            'masters/collectors',
            'masters/data',
            'masters/data/comprehensive',
            'masters/data/leagues',
            'masters/config',
            'masters/scripts',
            'masters/tests',
            
            # Endpoints system
            'endpoints',
            'endpoints/collectors', 
            'endpoints/data',
            'endpoints/config',
            'endpoints/results',
            'endpoints/tests',
            
            # Shared utilities
            'shared',
            'shared/utils',
            'shared/config',
            
            # Archive/backup
            'archive',
            'archive/original_structure'
        ]
        
        # Create directories
        for dir_path in new_dirs:
            full_path = os.path.join(self.root_dir, dir_path)
            os.makedirs(full_path, exist_ok=True)
            print(f"   ✅ Created: {dir_path}")
        
        return new_dirs
    
    def move_master_files(self):
        """Move master data collection files to new structure"""
        
        print(f"\\n📊 REORGANIZING MASTER DATA FILES")
        print("=" * 50)
        
        # Master collection scripts
        master_scripts = [
            ('src/league_separated_master_collection.py', 'masters/collectors/league_separated_collection.py'),
            ('src/test_master_collection.py', 'masters/collectors/legacy_collection.py'),
            ('src/multi_league_test.py', 'masters/tests/multi_league_test.py'),
            ('src/test_season_formats.py', 'masters/tests/season_formats_test.py'),
            ('src/test_league_collection.py', 'masters/tests/quick_collection_test.py'),
            ('src/test_wnba_collection.py', 'masters/tests/wnba_collection_test.py'),
            ('src/audit_and_separate_tables.py', 'masters/scripts/audit_and_separate.py'),
            ('src/verify_master_tables.py', 'masters/scripts/verify_tables.py'),
            ('src/fix_master_games_table.py', 'masters/scripts/fix_games_table.py'),
            ('src/fix_games_ids.py', 'masters/scripts/fix_game_ids.py'),
            ('src/collection_summary.py', 'masters/scripts/collection_summary.py')
        ]
        
        moved_count = 0
        for src_path, dest_path in master_scripts:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Moved: {src_path} → {dest_path}")
                moved_count += 1
            else:
                print(f"   ⚠️  Not found: {src_path}")
        
        print(f"   📊 Master scripts moved: {moved_count}/{len(master_scripts)}")
        
        # Master data files
        master_data_files = [
            ('data/comprehensive_master_games.csv', 'masters/data/comprehensive/games.csv'),
            ('data/comprehensive_master_players.csv', 'masters/data/comprehensive/players.csv'),
            ('data/master_teams.csv', 'masters/data/comprehensive/teams.csv'),
            ('data/master_seasons.csv', 'masters/data/comprehensive/seasons.csv'),
            ('data/master_players.csv', 'masters/data/comprehensive/legacy_players.csv'),
        ]
        
        # League-separated files
        if os.path.exists('data/leagues'):
            league_files = os.listdir('data/leagues')
            for filename in league_files:
                if filename.endswith('.csv'):
                    src_path = f'data/leagues/{filename}'
                    dest_path = f'masters/data/leagues/{filename}'
                    master_data_files.append((src_path, dest_path))
        
        moved_data_count = 0
        for src_path, dest_path in master_data_files:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Data moved: {src_path} → {dest_path}")
                moved_data_count += 1
        
        print(f"   📊 Master data files moved: {moved_data_count}")
        
        # Master config files
        master_configs = [
            ('config/master_tables_config.json', 'masters/config/tables_config.json'),
            ('data/master_collection_summary.json', 'masters/config/collection_summary.json'),
            ('data/leagues/league_separation_summary.json', 'masters/config/league_separation_summary.json')
        ]
        
        moved_config_count = 0
        for src_path, dest_path in master_configs:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Config moved: {src_path} → {dest_path}")
                moved_config_count += 1
        
        return {
            'scripts': moved_count,
            'data': moved_data_count,
            'configs': moved_config_count
        }
    
    def move_endpoint_files(self):
        """Move endpoint processing files to new structure"""
        
        print(f"\\n🔌 REORGANIZING ENDPOINT PROCESSING FILES")
        print("=" * 50)
        
        # Endpoint collection scripts
        endpoint_scripts = [
            ('src/final_data_collector.py', 'endpoints/collectors/comprehensive_collector.py'),
            ('src/systematic_endpoint_tester.py', 'endpoints/tests/systematic_tester.py'),
            ('src/production_data_collector.py', 'endpoints/collectors/production_collector.py'),
            ('src/improved_data_collector.py', 'endpoints/collectors/improved_collector.py'),
            ('src/fixed_gameid_collector.py', 'endpoints/collectors/fixed_gameid_collector.py'),
            ('src/nba_endpoint_processor.py', 'endpoints/collectors/endpoint_processor.py'),
            ('src/comprehensive_games_collection.py', 'endpoints/collectors/games_collection.py'),
            ('src/comprehensive_players_collection.py', 'endpoints/collectors/players_collection.py'),
            ('src/test_endpoints.py', 'endpoints/tests/endpoint_tests.py'),
            ('src/test_local_collection.py', 'endpoints/tests/local_collection_test.py'),
            ('src/games_process.py', 'endpoints/collectors/games_processor.py'),
            ('src/quick_test.py', 'endpoints/scripts/quick_test.py')
        ]
        
        moved_count = 0
        for src_path, dest_path in endpoint_scripts:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Moved: {src_path} → {dest_path}")
                moved_count += 1
            else:
                print(f"   ⚠️  Not found: {src_path}")
        
        print(f"   📊 Endpoint scripts moved: {moved_count}/{len(endpoint_scripts)}")
        
        # Endpoint config files
        endpoint_configs = [
            ('config/nba_endpoints_config.py', 'endpoints/config/nba_endpoints_config.py'),
            ('config/requirements.txt', 'endpoints/config/requirements.txt')
        ]
        
        moved_config_count = 0
        for src_path, dest_path in endpoint_configs:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Config moved: {src_path} → {dest_path}")
                moved_config_count += 1
        
        # Endpoint data files (results from previous collections)
        endpoint_data_files = [
            ('data/api_exploration_results.csv', 'endpoints/data/api_exploration_results.csv'),
            ('data/players_api_exploration_results.csv', 'endpoints/data/players_api_exploration_results.csv')
        ]
        
        moved_data_count = 0
        for src_path, dest_path in endpoint_data_files:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Data moved: {src_path} → {dest_path}")
                moved_data_count += 1
        
        return {
            'scripts': moved_count,
            'configs': moved_config_count,
            'data': moved_data_count
        }
    
    def move_shared_files(self):
        """Move shared utility files"""
        
        print(f"\\n🔧 ORGANIZING SHARED UTILITIES")
        print("=" * 50)
        
        # Shared utility files
        shared_files = [
            ('src/allintwo.py', 'shared/utils/database_utils.py'),
            ('src/nba_demo.py', 'shared/scripts/nba_demo.py'),
            ('src/final_demo.py', 'shared/scripts/final_demo.py')
        ]
        
        moved_count = 0
        for src_path, dest_path in shared_files:
            if os.path.exists(src_path):
                dest_full_path = os.path.join(self.root_dir, dest_path)
                os.makedirs(os.path.dirname(dest_full_path), exist_ok=True)
                shutil.copy2(src_path, dest_full_path)
                print(f"   ✅ Moved: {src_path} → {dest_path}")
                moved_count += 1
        
        # Create shared config
        shared_config = {
            'project_name': 'NBA Data Collection System',
            'reorganization_date': datetime.now().isoformat(),
            'structure': {
                'masters': 'Master data collection system',
                'endpoints': 'Endpoint data processing system', 
                'shared': 'Shared utilities and configurations'
            },
            'python_path': {
                'masters': 'masters',
                'endpoints': 'endpoints',
                'shared': 'shared'
            }
        }
        
        config_path = os.path.join(self.root_dir, 'shared/config/project_config.json')
        with open(config_path, 'w') as f:
            json.dump(shared_config, f, indent=2)
        
        print(f"   ✅ Created: shared/config/project_config.json")
        
        return moved_count + 1
    
    def archive_original_structure(self):
        """Archive the original file structure"""
        
        print(f"\\n📦 ARCHIVING ORIGINAL STRUCTURE")
        print("=" * 50)
        
        # Archive key directories
        dirs_to_archive = ['src', 'data', 'config']
        archived_count = 0
        
        for dir_name in dirs_to_archive:
            if os.path.exists(dir_name):
                archive_dest = f'archive/original_structure/{dir_name}'
                if os.path.exists(archive_dest):
                    shutil.rmtree(archive_dest)
                shutil.copytree(dir_name, archive_dest)
                print(f"   ✅ Archived: {dir_name} → {archive_dest}")
                archived_count += 1
        
        # Create archive summary
        archive_summary = {
            'archive_date': datetime.now().isoformat(),
            'archived_directories': dirs_to_archive,
            'reason': 'File structure reorganization - separating masters from endpoints',
            'restoration_note': 'Original structure preserved for reference and rollback if needed'
        }
        
        summary_path = os.path.join(self.root_dir, 'archive/original_structure/archive_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(archive_summary, f, indent=2)
        
        print(f"   ✅ Archive summary created")
        
        return archived_count
    
    def create_readme_files(self):
        """Create README files for the new structure"""
        
        print(f"\\n📄 CREATING DOCUMENTATION")
        print("=" * 50)
        
        # Main README
        main_readme = '''# NBA Data Collection System - Reorganized Structure

## 🏗️ New Project Structure

This project has been reorganized to separate master data collection from endpoint processing:

### 📊 `/masters/` - Master Data Collection System
- **Purpose**: Collect and organize fundamental NBA data (games, players, teams, seasons)
- **Key Features**: Multi-league support (NBA, WNBA, G-League), proper season formatting
- **Entry Point**: `masters/collectors/league_separated_collection.py`

### 🔌 `/endpoints/` - Endpoint Data Processing System  
- **Purpose**: Process specific NBA API endpoints using master data as reference
- **Key Features**: Systematic endpoint testing, production data collection
- **Entry Point**: `endpoints/collectors/comprehensive_collector.py`

### 🔧 `/shared/` - Shared Utilities
- **Purpose**: Common utilities and configurations used by both systems
- **Contents**: Database utilities, shared scripts, project configuration

### 📦 `/archive/` - Original Structure Backup
- **Purpose**: Backup of original file structure for reference/rollback
- **Contents**: Complete copy of original src/, data/, config/ directories

## 🚀 Getting Started

1. **Master Data Collection**: Start with `masters/collectors/league_separated_collection.py`
2. **Endpoint Processing**: Use `endpoints/collectors/comprehensive_collector.py`
3. **Testing**: Run tests in respective `/tests/` directories

## 📁 Directory Details

Each system directory contains:
- `collectors/` - Main collection scripts
- `config/` - Configuration files  
- `data/` - Data files (masters) or results (endpoints)
- `tests/` - Test scripts
- `scripts/` - Utility scripts
'''
        
        with open('README_NEW_STRUCTURE.md', 'w') as f:
            f.write(main_readme)
        print(f"   ✅ Created: README_NEW_STRUCTURE.md")
        
        # Masters README
        masters_readme = '''# Masters System - NBA Data Collection

## Purpose
Collects and organizes fundamental NBA data across multiple leagues with proper formatting.

## Key Features
- ✅ Multi-league support (NBA, WNBA, G-League)
- ✅ Proper season formatting per league
- ✅ Automatic league separation
- ✅ Data integrity validation

## Quick Start
```python
from collectors.league_separated_collection import LeagueSeparatedMasterCollector

collector = LeagueSeparatedMasterCollector()
results = collector.run_league_separated_collection(test_mode=True)
```

## Data Structure
- `data/comprehensive/` - All leagues combined
- `data/leagues/` - League-separated tables
'''
        
        masters_readme_path = os.path.join(self.root_dir, 'masters/README.md')
        with open(masters_readme_path, 'w') as f:
            f.write(masters_readme)
        print(f"   ✅ Created: masters/README.md")
        
        # Endpoints README
        endpoints_readme = '''# Endpoints System - NBA API Processing

## Purpose
Processes specific NBA API endpoints using master data as reference for systematic data collection.

## Key Features
- ✅ Uses master data for reference
- ✅ League-specific processing
- ✅ Systematic endpoint testing
- ✅ Production data collection

## Quick Start
```python
from collectors.comprehensive_collector import FinalDataCollector

collector = FinalDataCollector()
results = collector.collect_with_all_fixes()
```

## Data Flow
1. Uses master data from `../masters/data/`
2. Processes NBA API endpoints systematically
3. Saves results to `data/` and `results/`
'''
        
        endpoints_readme_path = os.path.join(self.root_dir, 'endpoints/README.md')
        with open(endpoints_readme_path, 'w') as f:
            f.write(endpoints_readme)
        print(f"   ✅ Created: endpoints/README.md")
        
        return 3
    
    def run_complete_reorganization(self):
        """Run the complete file structure reorganization"""
        
        print("🏗️  NBA DATA COLLECTION - FILE STRUCTURE REORGANIZATION")
        print("=" * 70)
        
        print(f"Starting reorganization at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Create new structure
        new_dirs = self.create_new_structure()
        
        # Step 2: Archive original structure
        archived_count = self.archive_original_structure()
        
        # Step 3: Move master files
        master_stats = self.move_master_files()
        
        # Step 4: Move endpoint files
        endpoint_stats = self.move_endpoint_files()
        
        # Step 5: Move shared files
        shared_count = self.move_shared_files()
        
        # Step 6: Create documentation
        docs_count = self.create_readme_files()
        
        # Summary
        print(f"\\n🎉 REORGANIZATION COMPLETE!")
        print("=" * 70)
        
        print(f"📁 New Structure Created:")
        print(f"   Directories: {len(new_dirs)}")
        
        print(f"\\n📊 Master System:")
        print(f"   Scripts: {master_stats['scripts']}")
        print(f"   Data files: {master_stats['data']}")
        print(f"   Config files: {master_stats['configs']}")
        
        print(f"\\n🔌 Endpoints System:")
        print(f"   Scripts: {endpoint_stats['scripts']}")
        print(f"   Config files: {endpoint_stats['configs']}")
        print(f"   Data files: {endpoint_stats['data']}")
        
        print(f"\\n🔧 Shared System:")
        print(f"   Files: {shared_count}")
        
        print(f"\\n📦 Archive:")
        print(f"   Directories archived: {archived_count}")
        
        print(f"\\n📄 Documentation:")
        print(f"   README files: {docs_count}")
        
        print(f"\\n✅ Next Steps:")
        print(f"   1. Test masters system: cd masters && python collectors/league_separated_collection.py")
        print(f"   2. Test endpoints system: cd endpoints && python collectors/comprehensive_collector.py")
        print(f"   3. Review documentation: README_NEW_STRUCTURE.md")
        
        return True


def main():
    """Main execution"""
    
    reorganizer = FileStructureReorganizer()
    success = reorganizer.run_complete_reorganization()
    
    return success


if __name__ == "__main__":
    main()
