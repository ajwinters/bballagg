"""
Test WNBA collection specifically to validate season format
"""
import sys
import os
import time
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nba_api.stats.endpoints import leaguegamefinder

def test_wnba_seasons():
    """Test WNBA data collection with correct season format"""
    
    print("🏀 Testing WNBA Season Formats")
    print("=" * 40)
    
    # Test both formats to see which works
    test_seasons = ['2024', '2023', '2022']  # WNBA format
    league_id = '10'  # WNBA
    
    for season in test_seasons:
        print(f"\n📊 Testing WNBA season: {season}")
        
        try:
            # Try regular season games
            gamefinder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable=league_id,
                season_type_nullable='Regular Season',
                season_nullable=season
            ).get_data_frames()[0]
            
            print(f"   ✅ Found {len(gamefinder)} regular season games")
            
            if len(gamefinder) > 0:
                # Show sample data
                print(f"   Sample game IDs: {gamefinder['GAME_ID'].head(3).tolist()}")
                date_range = f"{gamefinder['GAME_DATE'].min()} to {gamefinder['GAME_DATE'].max()}"
                print(f"   Date range: {date_range}")
            
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")

if __name__ == "__main__":
    test_wnba_seasons()
