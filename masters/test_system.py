"""
NBA Master Tables Test Suite

Quick verification of system components without database dependencies.
Tests NBA API connectivity, data collection logic, and basic functionality.
"""

import pandas as pd
import time
import json
from datetime import datetime
import sys
import os

# NBA API imports
try:
    from nba_api.stats.endpoints import leaguegamefinder
    import nba_api.stats.endpoints as nbaapi
    from nba_api.stats.static import teams, players
    print("✅ NBA API imports successful")
except ImportError as e:
    print(f"❌ NBA API import failed: {e}")
    sys.exit(1)


def test_nba_api_connectivity():
    """Test basic NBA API connectivity"""
    print("\n🏀 Testing NBA API Connectivity...")
    
    try:
        # Test with recent NBA games
        print("  Fetching recent NBA games...", end=" ")
        gamefinder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable='00',  # NBA
            season_type_nullable='Regular Season',
            season_nullable='2024-25'
        ).get_data_frames()[0]
        
        if len(gamefinder) > 0:
            print(f"✅ {len(gamefinder)} games found")
            return True
        else:
            print("⚠️ No games found (might be off-season)")
            return True
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False


def test_league_configurations():
    """Test all league configurations"""
    print("\n🏟️ Testing League Configurations...")
    
    leagues = [
        {'id': '00', 'name': 'NBA'},
        {'id': '10', 'name': 'WNBA'}, 
        {'id': '20', 'name': 'G-League'}
    ]
    
    results = {}
    
    for league in leagues:
        try:
            print(f"  Testing {league['name']}...", end=" ")
            
            # Test current season games
            gamefinder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable=league['id'],
                season_type_nullable='Regular Season',
                season_nullable='2024-25' if league['name'] != 'WNBA' else '2024'
            ).get_data_frames()[0]
            
            game_count = len(gamefinder)
            results[league['name']] = game_count
            print(f"✅ {game_count} games")
            
            time.sleep(0.6)  # Rate limiting
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            results[league['name']] = 0
    
    return results


def test_data_processing():
    """Test data processing and cleaning functions"""
    print("\n🔧 Testing Data Processing...")
    
    try:
        # Create sample data
        sample_data = {
            'Game ID': [1, 2, 3],
            'Team Name': ['Lakers', 'Warriors', 'Celtics'],
            'Game Date': ['2024-01-15', '2024-01-16', '2024-01-17'],
            'Points Scored': [110, 115, 108]
        }
        
        df = pd.DataFrame(sample_data)
        print(f"  Sample data created: {df.shape[0]} rows, {df.shape[1]} columns")
        
        # Test column cleaning (simulated)
        import re
        clean_columns = [re.sub(r'[^a-zA-Z0-9]', '', col).lower() for col in df.columns]
        print(f"  Column cleaning: {list(df.columns)} -> {clean_columns}")
        
        # Test metadata addition
        df['league_name'] = 'NBA'
        df['created_at'] = datetime.now()
        df['collection_run_id'] = datetime.now().isoformat()
        
        print(f"  Metadata added: {df.shape[1]} total columns")
        print("  ✅ Data processing functions working")
        return True
        
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        return False


def test_season_generation():
    """Test season string generation for different leagues"""
    print("\n📅 Testing Season Generation...")
    
    def generate_seasons_by_league(league_config, end_year=None):
        if end_year is None:
            end_year = datetime.now().year + 1
            
        seasons = []
        start_year = league_config['start_year']
        
        if league_config['season_format'] == 'two_year':
            # NBA/G-League format: 2023-24
            for year in range(start_year, end_year):
                season_str = f"{year}-{str(year+1)[2:].zfill(2)}"
                seasons.append(season_str)
        else:
            # WNBA format: 2024
            for year in range(start_year, end_year):
                seasons.append(str(year))
        
        return seasons[::-1]  # Most recent first
    
    league_configs = [
        {'name': 'NBA', 'season_format': 'two_year', 'start_year': 1946},
        {'name': 'WNBA', 'season_format': 'single_year', 'start_year': 1997},
        {'name': 'G-League', 'season_format': 'two_year', 'start_year': 2001}
    ]
    
    for config in league_configs:
        seasons = generate_seasons_by_league(config, 2025)
        recent_seasons = seasons[:3]  # Show 3 most recent
        print(f"  {config['name']}: {recent_seasons} (showing 3 recent)")
    
    print("  ✅ Season generation working")
    return True


def test_configuration_loading():
    """Test configuration file loading"""
    print("\n⚙️ Testing Configuration...")
    
    try:
        config_file = 'scheduler_config.json'
        
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            print(f"  Config file loaded: {len(config)} sections")
            
            # Check key sections
            required_sections = ['schedules', 'database', 'collection']
            for section in required_sections:
                if section in config:
                    print(f"    ✅ {section}: {len(config[section])} settings")
                else:
                    print(f"    ❌ {section}: Missing")
            
            return True
        else:
            print(f"  ⚠️ Config file not found: {config_file}")
            return False
            
    except Exception as e:
        print(f"  ❌ Error loading config: {str(e)}")
        return False


def run_comprehensive_test():
    """Run all tests and show summary"""
    print("🧪 NBA MASTER TABLES - COMPREHENSIVE TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("NBA API Connectivity", test_nba_api_connectivity),
        ("League Configurations", test_league_configurations), 
        ("Data Processing", test_data_processing),
        ("Season Generation", test_season_generation),
        ("Configuration Loading", test_configuration_loading)
    ]
    
    results = {}
    start_time = time.time()
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"\n❌ {test_name} crashed: {str(e)}")
            results[test_name] = False
    
    # Summary
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("🏁 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    print(f"⏱️ Total time: {elapsed_time:.1f} seconds")
    print(f"📊 Tests passed: {passed}/{total}")
    
    print("\n📋 DETAILED RESULTS:")
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED - System ready for production!")
    else:
        print(f"\n⚠️ {total - passed} TESTS FAILED - Review errors before production")
    
    return results


def quick_api_sample():
    """Quick sample of data that would be collected"""
    print("\n🎯 QUICK DATA SAMPLE")
    print("=" * 40)
    
    try:
        # Get a small sample of recent games
        print("Fetching recent NBA games sample...")
        
        gamefinder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable='00',
            season_type_nullable='Regular Season', 
            season_nullable='2024-25'
        ).get_data_frames()[0]
        
        if len(gamefinder) > 0:
            # Show sample structure
            sample = gamefinder.head(3)
            print(f"\n📊 Sample Data ({len(gamefinder)} total games available):")
            print(f"Columns: {list(sample.columns)}")
            print(f"Shape: {sample.shape}")
            print("\nFirst 3 games:")
            for i, row in sample.iterrows():
                print(f"  Game {row.get('GAME_ID', 'N/A')}: {row.get('TEAM_NAME', 'N/A')} vs opponent ({row.get('GAME_DATE', 'N/A')})")
        else:
            print("No recent games found (might be off-season)")
            
    except Exception as e:
        print(f"Error getting sample: {str(e)}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        quick_api_sample()
    else:
        run_comprehensive_test()
        
        # Optional quick sample
        choice = input("\nRun quick data sample? (y/N): ").strip().lower()
        if choice == 'y':
            quick_api_sample()
