"""
NBA Data Collection Test - Local Version

This script demonstrates our systematic NBA data collection without requiring database connectivity.
It will create CSV files with sample data to validate our approach.
"""

import pandas as pd
import time
import os
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
from nba_api.stats.static import teams

def create_sample_masters():
    """Create sample master tables locally"""
    print("=== CREATING SAMPLE MASTER TABLES ===")
    
    # Create data directory if it doesn't exist
    os.makedirs('../data', exist_ok=True)
    
    # Master Teams (from static data)
    print("Creating master teams...")
    teams_data = teams.get_teams()
    master_teams = pd.DataFrame(teams_data)
    master_teams.to_csv('../data/master_teams.csv', index=False)
    print(f"✓ Master teams: {len(master_teams)} teams saved")
    
    # Sample seasons (just recent ones for testing)
    print("Creating master seasons...")
    recent_seasons = ['2023-24', '2022-23', '2021-22']
    master_seasons = pd.DataFrame({
        'season': recent_seasons,
        'start_year': [int(s.split('-')[0]) for s in recent_seasons],
        'end_year': [int('20' + s.split('-')[1]) for s in recent_seasons],
        'created_date': pd.Timestamp.now()
    })
    master_seasons.to_csv('../data/master_seasons.csv', index=False)
    print(f"✓ Master seasons: {len(master_seasons)} seasons saved")
    
    # Sample games (from one recent season)
    print("Creating sample master games...")
    try:
        sample_season = '2023-24'
        print(f"Fetching games for {sample_season}...")
        
        # Get recent games only (limit for testing)
        gamefinder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable='00', 
            season_type_nullable="Regular Season",
            season_nullable=sample_season
        ).get_data_frames()[0]
        
        # Take a sample of games for testing
        sample_games = gamefinder.head(100).copy()  # First 100 games
        sample_games['GAME_DATE'] = pd.to_datetime(sample_games['GAME_DATE'])
        sample_games.to_csv('../data/sample_master_games.csv', index=False)
        
        print(f"✓ Sample games: {len(sample_games)} games saved")
        print(f"  Game date range: {sample_games['GAME_DATE'].min().date()} to {sample_games['GAME_DATE'].max().date()}")
        print(f"  Unique games: {sample_games['GAME_ID'].nunique()}")
        
    except Exception as e:
        print(f"✗ Failed to fetch sample games: {str(e)}")
        # Create minimal fake data for testing
        sample_games = pd.DataFrame({
            'GAME_ID': ['0022301200', '0022301201', '0022301202'],
            'GAME_DATE': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
            'TEAM_ID': [1610612747, 1610612738, 1610612739],
            'TEAM_NAME': ['Lakers', 'Celtics', 'Cavaliers']
        })
        sample_games.to_csv('../data/sample_master_games.csv', index=False)
        print("✓ Created minimal sample games for testing")
    
    return master_teams, master_seasons, sample_games

def test_endpoint_processing():
    """Test our endpoint processing with sample data"""
    print("\n=== TESTING ENDPOINT PROCESSING ===")
    
    # Load our sample data
    try:
        sample_games = pd.read_csv('../data/sample_master_games.csv')
        print(f"Loaded {len(sample_games)} sample games")
        
        # Get a few game IDs for testing
        test_game_ids = sample_games['GAME_ID'].head(3).tolist() if 'GAME_ID' in sample_games.columns else ['0022301200']
        print(f"Testing with game IDs: {test_game_ids}")
        
        # Test some boxscore endpoints
        test_endpoints = [
            ('BoxScoreTraditionalV2', nbaapi.BoxScoreTraditionalV2),
            ('BoxScoreAdvancedV2', nbaapi.BoxScoreAdvancedV2)
        ]
        
        results = {}
        
        for endpoint_name, endpoint_class in test_endpoints:
            print(f"\nTesting {endpoint_name}...")
            endpoint_results = []
            
            for game_id in test_game_ids:
                try:
                    print(f"  Processing game {game_id}...")
                    
                    # Make API call
                    endpoint_instance = endpoint_class(game_id=game_id)
                    dataframes = endpoint_instance.get_data_frames()
                    
                    # Process each dataframe
                    expected_keys = list(endpoint_instance.expected_data.keys()) if hasattr(endpoint_instance, 'expected_data') else []
                    
                    for i, df in enumerate(dataframes):
                        if not df.empty:
                            df_name = expected_keys[i] if i < len(expected_keys) else f'df_{i}'
                            table_name = f"{endpoint_name.lower()}_{df_name.lower()}"
                            
                            # Save to CSV (simulating database insert)
                            os.makedirs('../data/endpoints', exist_ok=True)
                            csv_path = f'../data/endpoints/{table_name}_sample.csv'
                            
                            if os.path.exists(csv_path):
                                # Append mode
                                existing = pd.read_csv(csv_path)
                                combined = pd.concat([existing, df], ignore_index=True)
                                combined.to_csv(csv_path, index=False)
                            else:
                                df.to_csv(csv_path, index=False)
                            
                            print(f"    ✓ {table_name}: {len(df)} rows saved")
                            endpoint_results.append({
                                'table': table_name,
                                'rows': len(df),
                                'columns': len(df.columns)
                            })
                    
                    time.sleep(0.6)  # Rate limiting
                    
                except Exception as e:
                    print(f"    ✗ Error with game {game_id}: {str(e)}")
            
            results[endpoint_name] = endpoint_results
        
        return results
        
    except Exception as e:
        print(f"Error in endpoint testing: {str(e)}")
        return {}

def main():
    """Main execution"""
    print("NBA Data Collection Test (Local Version)")
    print("=" * 50)
    
    # Create master tables
    master_teams, master_seasons, sample_games = create_sample_masters()
    
    # Test endpoint processing
    endpoint_results = test_endpoint_processing()
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    
    print("✓ Master tables created:")
    print(f"  - Teams: {len(master_teams) if 'master_teams' in locals() else 0}")
    print(f"  - Seasons: {len(master_seasons) if 'master_seasons' in locals() else 0}")
    print(f"  - Games: {len(sample_games) if 'sample_games' in locals() else 0}")
    
    print("✓ Endpoint processing tested:")
    for endpoint, results in endpoint_results.items():
        print(f"  - {endpoint}: {len(results)} tables created")
        for result in results:
            print(f"    • {result['table']}: {result['rows']} rows, {result['columns']} columns")
    
    print("\nFiles created in ../data/:")
    print("  - master_teams.csv")
    print("  - master_seasons.csv")
    print("  - sample_master_games.csv")
    print("  - endpoints/ (endpoint data files)")
    
    print("\n🎉 Local NBA data collection test completed successfully!")

if __name__ == "__main__":
    main()
