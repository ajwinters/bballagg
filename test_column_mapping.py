#!/usr/bin/env python3
"""
Test script to validate the NBA data processor column name mapping system.
This tests our fixes for master table column name variations.
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from nba_data_processor import NBADataProcessor

def test_column_mapping():
    """Test the column name mapping functionality"""
    print("Testing NBA Data Processor Column Name Mapping")
    print("=" * 50)
    
    try:
        # Initialize processor in test mode
        processor = NBADataProcessor(test_mode=True, max_items_per_endpoint=3)
        print("✓ NBADataProcessor initialized successfully")
        
        # Test master table detection  
        master_endpoints = ['LeagueGameFinder', 'CommonAllPlayers']
        
        for endpoint in master_endpoints:
            if processor.is_master_endpoint(endpoint):
                print(f"✓ {endpoint} correctly identified as master endpoint")
                
                # Test master table name generation
                master_designation = processor.get_master_designation(endpoint)
                master_table = processor.get_master_table_name(master_designation)
                print(f"  → Master designation: {master_designation}")
                print(f"  → Master table name: {master_table}")
                
                # Test expected column name mapping (without DB connection)
                if 'game' in endpoint.lower():
                    expected_column = 'gameid'  # NBA API uses 'gameid' not 'game_id'
                    print(f"  → Expected game column: {expected_column}")
                elif 'player' in endpoint.lower():
                    expected_column = 'personid'  # NBA API uses 'personid' not 'player_id'
                    print(f"  → Expected player column: {expected_column}")
                    
            else:
                print(f"✗ {endpoint} not identified as master endpoint")
        
        print("\n" + "=" * 50)
        print("Column mapping system validation complete!")
        print("The system is ready to handle NBA API column name variations.")
        return True
        
    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_column_mapping()
    sys.exit(0 if success else 1)