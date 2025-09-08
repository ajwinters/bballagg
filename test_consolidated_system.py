#!/usr/bin/env python3
"""
Test script for the consolidated NBA data collection system
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all our consolidated modules import correctly"""
    print("🧪 Testing consolidated system imports...")
    
    try:
        from rds_connection_manager import RDSConnectionManager
        print("✅ RDS Connection Manager imported successfully")
        
        from parameter_resolver import resolve_parameters_comprehensive
        print("✅ Parameter Resolver imported successfully")
        
        from dataframe_name_matcher import match_dataframes_to_names
        print("✅ DataFrame Name Matcher imported successfully")
        
        from player_dashboard_enhancer import is_player_dashboard_endpoint
        print("✅ Player Dashboard Enhancer imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False


def test_database_connection():
    """Test database connection with the new manager"""
    print("\n🔌 Testing database connection...")
    
    try:
        from rds_connection_manager import RDSConnectionManager
        
        with RDSConnectionManager() as conn_manager:
            if conn_manager.ensure_connection():
                print("✅ Database connection successful")
                
                # Test a simple query
                result = conn_manager.execute_query("SELECT 1 as test")
                if result:
                    print("✅ Database query test successful")
                    return True
                else:
                    print("❌ Database query test failed")
                    return False
            else:
                print("❌ Database connection failed")
                return False
                
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False


def test_config_loading():
    """Test configuration file loading"""
    print("\n⚙️  Testing configuration loading...")
    
    try:
        import json
        import os
        
        # Test database config
        db_config_path = os.path.join(os.path.dirname(__file__), 'config', 'database_config.json')
        with open(db_config_path, 'r') as f:
            db_config = json.load(f)
        print(f"✅ Database config loaded: {db_config['host']}")
        
        # Test leagues config
        leagues_config_path = os.path.join(os.path.dirname(__file__), 'config', 'leagues_config.json')
        with open(leagues_config_path, 'r') as f:
            leagues_config = json.load(f)
        print(f"✅ Leagues config loaded: {len(leagues_config)} leagues")
        
        # Test run config
        run_config_path = os.path.join(os.path.dirname(__file__), 'config', 'run_config.json')
        with open(run_config_path, 'r') as f:
            run_config = json.load(f)
        print(f"✅ Run config loaded: {len(run_config['collection_profiles'])} profiles")
        
        return True
        
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        return False


def test_directory_structure():
    """Test that our new directory structure is correct"""
    print("\n📁 Testing directory structure...")
    
    required_dirs = [
        'src',
        'config', 
        'batching',
        'notebooks'
    ]
    
    required_files = [
        'src/rds_connection_manager.py',
        'src/parameter_resolver.py',
        'src/endpoint_processor.py',
        'src/database_manager.py',
        'config/database_config.json',
        'config/leagues_config.json',
        'config/run_config.json',
        'batching/nba_jobs.sh'
    ]
    
    base_path = os.path.dirname(__file__)
    
    # Check directories
    for dir_name in required_dirs:
        dir_path = os.path.join(base_path, dir_name)
        if os.path.exists(dir_path):
            print(f"✅ Directory exists: {dir_name}")
        else:
            print(f"❌ Directory missing: {dir_name}")
            return False
    
    # Check files
    for file_name in required_files:
        file_path = os.path.join(base_path, file_name)
        if os.path.exists(file_path):
            print(f"✅ File exists: {file_name}")
        else:
            print(f"❌ File missing: {file_name}")
            return False
    
    return True


def main():
    """Run all tests"""
    print("🚀 NBA Data Collection System - Integration Test")
    print("=" * 60)
    
    all_tests_passed = True
    
    # Run tests
    all_tests_passed &= test_directory_structure()
    all_tests_passed &= test_config_loading()
    all_tests_passed &= test_imports()
    all_tests_passed &= test_database_connection()
    
    print("\n" + "=" * 60)
    if all_tests_passed:
        print("🎉 All tests passed! Your consolidated system is ready.")
        print("\nNext steps:")
        print("1. Test endpoint processing: python src/endpoint_processor.py")
        print("2. Submit a test batch job: cd batching && ./nba_jobs.sh submit test")
        print("3. Monitor logs: cd batching && ./nba_jobs.sh status")
    else:
        print("❌ Some tests failed. Please check the errors above.")
    
    return all_tests_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
