"""
Test script to validate season formats for different leagues
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from league_separated_master_collection import LeagueSeparatedMasterCollector

def test_season_formats():
    """Test the different season formats for each league"""
    
    collector = LeagueSeparatedMasterCollector()
    
    print("🧪 Testing Season Formats by League")
    print("=" * 40)
    
    for league in collector.league_configs:
        league_name = league['name']
        print(f"\n🏀 {league_name} ({league['full_name']}):")
        
        # Get last 5 seasons for this league
        seasons = collector.generate_seasons_by_league(league_name)[:5]
        print(f"   Season format: {seasons}")
        print(f"   League ID: {league['id']}")

if __name__ == "__main__":
    test_season_formats()
