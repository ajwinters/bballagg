"""
Source Folder Cleanup

This script cleans up the old src folder now that all files have been 
reorganized into the new structure. It will:

1. Identify files that have been moved to the new structure
2. Identify files that are truly redundant/obsolete
3. Archive any files that might still be useful
4. Clean up the src folder

MOVED FILES STATUS:
- Masters files → masters/collectors/, masters/scripts/, masters/tests/
- Endpoints files → endpoints/collectors/, endpoints/tests/  
- Shared utilities → shared/utils/, shared/scripts/
"""

import os
import shutil
from datetime import datetime

class SrcFolderCleanup:
    """Safely cleanup the old src folder after reorganization"""
    
    def __init__(self):
        self.src_dir = 'src'
        self.moved_files = {}
        self.redundant_files = []
        self.cleanup_results = {}
        
    def analyze_src_files(self):
        """Analyze which files in src are redundant vs moved"""
        
        print("🔍 ANALYZING SRC FOLDER FILES")
        print("=" * 50)
        
        # Files that were moved to new structure
        file_mappings = {
            # Master system files (moved)
            'league_separated_master_collection.py': 'masters/collectors/league_separated_collection.py',
            'test_master_collection.py': 'masters/collectors/legacy_collection.py',
            'multi_league_test.py': 'masters/tests/multi_league_test.py',
            'test_season_formats.py': 'masters/tests/season_formats_test.py',
            'test_league_collection.py': 'masters/tests/quick_collection_test.py',
            'test_wnba_collection.py': 'masters/tests/wnba_collection_test.py',
            'audit_and_separate_tables.py': 'masters/scripts/audit_and_separate.py',
            'verify_master_tables.py': 'masters/scripts/verify_tables.py',
            'fix_master_games_table.py': 'masters/scripts/fix_games_table.py',
            'fix_games_ids.py': 'masters/scripts/fix_game_ids.py',
            'collection_summary.py': 'masters/scripts/collection_summary.py',
            
            # Endpoint system files (moved)
            'final_data_collector.py': 'endpoints/collectors/comprehensive_collector.py',
            'systematic_endpoint_tester.py': 'endpoints/tests/systematic_tester.py',
            'production_data_collector.py': 'endpoints/collectors/production_collector.py',
            'improved_data_collector.py': 'endpoints/collectors/improved_collector.py',
            'fixed_gameid_collector.py': 'endpoints/collectors/fixed_gameid_collector.py',
            'nba_endpoint_processor.py': 'endpoints/collectors/endpoint_processor.py',
            'comprehensive_games_collection.py': 'endpoints/collectors/games_collection.py',
            'comprehensive_players_collection.py': 'endpoints/collectors/players_collection.py',
            'test_endpoints.py': 'endpoints/tests/endpoint_tests.py',
            'test_local_collection.py': 'endpoints/tests/local_collection_test.py',
            'games_process.py': 'endpoints/collectors/games_processor.py',
            'quick_test.py': 'endpoints/scripts/quick_test.py',
            
            # Shared utilities (moved)
            'allintwo.py': 'shared/utils/database_utils.py',
            'nba_demo.py': 'shared/scripts/nba_demo.py',
            'final_demo.py': 'shared/scripts/final_demo.py'
        }
        
        # One-time use files that are now redundant
        redundant_files = [
            'reorganize_structure.py',  # Used once for reorganization
            'quick_fix.py'  # Temporary fix script
        ]
        
        print("📊 File Analysis Results:")
        
        # Check moved files
        moved_count = 0
        moved_verified = 0
        
        for src_file, new_location in file_mappings.items():
            src_path = os.path.join(self.src_dir, src_file)
            if os.path.exists(src_path):
                if os.path.exists(new_location):
                    print(f"   ✅ {src_file} → {new_location} (MOVED)")
                    self.moved_files[src_file] = new_location
                    moved_verified += 1
                else:
                    print(f"   ⚠️  {src_file} → {new_location} (TARGET MISSING)")
                moved_count += 1
            else:
                print(f"   ℹ️  {src_file} → Already removed")
        
        # Check redundant files
        redundant_count = 0
        for filename in redundant_files:
            src_path = os.path.join(self.src_dir, filename)
            if os.path.exists(src_path):
                print(f"   🗑️  {filename} (REDUNDANT - one-time use)")
                self.redundant_files.append(filename)
                redundant_count += 1
        
        # Check for any remaining files
        remaining_files = []
        if os.path.exists(self.src_dir):
            all_files = [f for f in os.listdir(self.src_dir) if f.endswith('.py')]
            for filename in all_files:
                if filename not in file_mappings and filename not in redundant_files:
                    remaining_files.append(filename)
                    print(f"   ❓ {filename} (UNKNOWN - needs review)")
        
        print(f"\\n📋 Summary:")
        print(f"   ✅ Files successfully moved: {moved_verified}/{moved_count}")
        print(f"   🗑️  Files identified as redundant: {redundant_count}")
        print(f"   ❓ Files needing review: {len(remaining_files)}")
        
        return {
            'moved_verified': moved_verified,
            'moved_total': moved_count,
            'redundant_count': redundant_count,
            'remaining_files': remaining_files,
            'safe_to_cleanup': moved_verified == moved_count and len(remaining_files) == 0
        }
    
    def show_cleanup_plan(self, analysis_results):
        """Show the cleanup plan"""
        
        print(f"\\n📋 CLEANUP PLAN")
        print("=" * 50)
        
        if analysis_results['safe_to_cleanup']:
            print("✅ **SAFE TO CLEAN UP SRC FOLDER**")
            print("   All important files have been moved to new structure")
            print("   Only redundant/temporary files remain")
            
            print(f"\\n🗑️  Files to be removed:")
            for filename in self.moved_files.keys():
                print(f"   • {filename} (moved to new location)")
            
            for filename in self.redundant_files:
                print(f"   • {filename} (redundant/temporary)")
            
            print(f"\\n💾 Safety measures:")
            print("   • Complete backup will be created before deletion")
            print("   • Files are already preserved in archive/original_structure/")
            print("   • All moved files verified in new locations")
            
            return 'safe'
            
        else:
            print("⚠️  **CLEANUP NEEDS REVIEW**")
            
            if analysis_results['moved_verified'] != analysis_results['moved_total']:
                print(f"   • Some moved files not verified in new locations")
            
            if analysis_results['remaining_files']:
                print(f"   • {len(analysis_results['remaining_files'])} files need manual review:")
                for filename in analysis_results['remaining_files']:
                    print(f"     - {filename}")
            
            return 'needs_review'
    
    def create_final_archive(self):
        """Create final archive of src folder before cleanup"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = f"archive/src_cleanup_{timestamp}"
        
        print(f"📦 Creating final src archive: {archive_path}")
        
        if os.path.exists(self.src_dir):
            shutil.copytree(self.src_dir, archive_path)
            print(f"   ✅ Src folder archived successfully")
            return archive_path
        else:
            print(f"   ⚠️  Src folder not found")
            return None
    
    def cleanup_src_folder(self):
        """Clean up the src folder"""
        
        print(f"\\n🧹 CLEANING UP SRC FOLDER")
        print("=" * 50)
        
        if not os.path.exists(self.src_dir):
            print("   ℹ️  Src folder already removed")
            return True
        
        # Remove Python files
        python_files_removed = 0
        if os.path.exists(self.src_dir):
            files = os.listdir(self.src_dir)
            
            for filename in files:
                file_path = os.path.join(self.src_dir, filename)
                
                if filename.endswith('.py'):
                    if filename in self.moved_files or filename in self.redundant_files:
                        os.remove(file_path)
                        print(f"   🗑️  Removed: {filename}")
                        python_files_removed += 1
                    else:
                        print(f"   ⏸️  Kept: {filename} (needs review)")
                elif filename == '__pycache__':
                    shutil.rmtree(file_path)
                    print(f"   🗑️  Removed: __pycache__ directory")
        
        # Check if folder is now empty or nearly empty
        remaining_files = []
        if os.path.exists(self.src_dir):
            remaining_files = [f for f in os.listdir(self.src_dir) if not f.startswith('.')]
        
        if len(remaining_files) == 0:
            # Remove empty src folder
            os.rmdir(self.src_dir)
            print(f"   🗑️  Removed empty src folder")
            return True
        else:
            print(f"   ⏸️  Src folder kept with {len(remaining_files)} remaining files:")
            for filename in remaining_files:
                print(f"      • {filename}")
            return False
    
    def show_final_structure(self):
        """Show the final clean project structure"""
        
        print(f"\\n📁 CLEAN PROJECT STRUCTURE")
        print("=" * 50)
        
        print("✅ **Organized Systems:**")
        
        # Masters system
        if os.path.exists('masters'):
            masters_files = sum(len(files) for _, _, files in os.walk('masters'))
            print(f"   📊 masters/ - Master data collection ({masters_files} files)")
        
        # Endpoints system  
        if os.path.exists('endpoints'):
            endpoints_files = sum(len(files) for _, _, files in os.walk('endpoints'))
            print(f"   🔌 endpoints/ - Endpoint processing ({endpoints_files} files)")
        
        # Shared utilities
        if os.path.exists('shared'):
            shared_files = sum(len(files) for _, _, files in os.walk('shared'))
            print(f"   🔧 shared/ - Shared utilities ({shared_files} files)")
        
        print(f"\\n📦 **Archives & Backups:**")
        if os.path.exists('archive'):
            archive_dirs = [d for d in os.listdir('archive') if os.path.isdir(os.path.join('archive', d))]
            print(f"   📦 archive/ - {len(archive_dirs)} backup directories")
            for archive_dir in archive_dirs:
                print(f"      • {archive_dir}")
        
        print(f"\\n🗂️  **Other Directories:**")
        main_dirs = ['notebooks', 'config', 'deployment']
        for dir_name in main_dirs:
            if os.path.exists(dir_name):
                files_count = sum(len(files) for _, _, files in os.walk(dir_name))
                print(f"   📁 {dir_name}/ ({files_count} files)")
    
    def run_src_cleanup(self):
        """Run complete src folder cleanup"""
        
        print("🧹 SRC FOLDER CLEANUP PROCESS")
        print("=" * 60)
        
        # Step 1: Analyze files
        analysis_results = self.analyze_src_files()
        
        # Step 2: Show cleanup plan
        cleanup_status = self.show_cleanup_plan(analysis_results)
        
        # Step 3: Get user confirmation and proceed
        if cleanup_status == 'safe':
            print(f"\\n❓ Proceed with src folder cleanup?")
            print("   This will:")
            print("   1. Create final archive of src folder") 
            print("   2. Remove all redundant/moved files")
            print("   3. Remove empty src folder if possible")
            
            response = input("\\nProceed? (y/N): ").strip().lower()
            
            if response == 'y':
                # Create archive
                archive_path = self.create_final_archive()
                
                # Clean up
                cleanup_success = self.cleanup_src_folder()
                
                # Show results
                self.show_final_structure()
                
                print(f"\\n🎉 SRC CLEANUP COMPLETE!")
                if archive_path:
                    print(f"   📦 Final archive: {archive_path}")
                if cleanup_success:
                    print(f"   🗑️  Src folder completely removed")
                else:
                    print(f"   ⏸️  Src folder cleaned but not removed (files remaining)")
                
                return True
            else:
                print(f"   ⏸️  Cleanup cancelled")
                return False
        else:
            print(f"\\n⏸️  Cleanup not recommended - review remaining files first")
            return False


def main():
    """Main cleanup execution"""
    
    cleanup_manager = SrcFolderCleanup()
    success = cleanup_manager.run_src_cleanup()
    
    if success:
        print(f"\\n✨ Your project structure is now completely clean and organized!")
        print("   📊 masters/ - Master data collection system")
        print("   🔌 endpoints/ - Endpoint processing system")
        print("   🔧 shared/ - Shared utilities")
        print("   📦 archive/ - Complete backups for safety")
    else:
        print(f"\\n⏸️  Src cleanup cancelled - no changes made")


if __name__ == "__main__":
    main()
