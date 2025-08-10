"""
Quick NBA API test to get proper game IDs and test our endpoint processing
"""

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
import nba_api.stats.endpoints as nbaapi
import time

def test_with_real_data():
    """Test with a small amount of real NBA data"""
    print("=== TESTING WITH REAL NBA DATA ===")
    
    try:
        # Get a small sample of recent games
        print("Fetching recent games...")
        gamefinder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable='00', 
            season_type_nullable="Regular Season",
            season_nullable='2023-24'
        ).get_data_frames()[0]
        
        # Get just the first 5 unique games for testing
        unique_games = gamefinder['GAME_ID'].unique()[:3]
        print(f"Testing with game IDs: {unique_games}")
        
        # Test BoxScore endpoint
        for game_id in unique_games:
            print(f"\nTesting game {game_id}...")
            try:
                # Test BoxScoreTraditionalV2
                boxscore = nbaapi.BoxScoreTraditionalV2(game_id=game_id)
                dfs = boxscore.get_data_frames()
                
                print(f"  ✓ BoxScoreTraditionalV2: {len(dfs)} dataframes")
                for i, df in enumerate(dfs):
                    if not df.empty:
                        print(f"    DataFrame {i}: {len(df)} rows, {len(df.columns)} columns")
                        # Save first few rows as sample
                        sample_path = f'../data/sample_boxscore_{game_id}_{i}.csv'
                        df.head(3).to_csv(sample_path, index=False)
                    else:
                        print(f"    DataFrame {i}: EMPTY")
                
                time.sleep(1)  # Rate limiting
                break  # Just test one game for now
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}")
                continue
        
        print("\n✅ Real data test completed!")
        
    except Exception as e:
        print(f"Error in real data test: {str(e)}")

if __name__ == "__main__":
    test_with_real_data()
