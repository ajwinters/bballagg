#!/usr/bin/env python3
"""
Quick test to validate all season types including IST.
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from nba_data_processor import NBADataProcessor

def test_season_types():
    """Test that all season types including IST are included"""
    print("Testing Season Types Coverage")
    print("=" * 40)
    
    processor = NBADataProcessor(test_mode=False)  # Production mode to see all types
    
    # Get all season types
    season_types = processor._get_all_season_types()
    
    print(f"Total season types: {len(season_types)}")
    print("Season types included:")
    for i, season_type in enumerate(season_types, 1):
        print(f"  {i}. {season_type}")
    
    # Verify IST is included
    if 'IST' in season_types:
        print("\n✓ IST (In-Season Tournament) is included!")
    else:
        print("\n✗ IST (In-Season Tournament) is missing!")
    
    # Show total combinations for an endpoint with season_type
    seasons = processor._get_all_seasons()
    total_combinations = len(seasons) * len(season_types)
    
    print(f"\nFor endpoints with season + season_type:")
    print(f"  {len(seasons)} seasons × {len(season_types)} season types = {total_combinations} total combinations")
    
    return 'IST' in season_types

if __name__ == '__main__':
    success = test_season_types()
    sys.exit(0 if success else 1)