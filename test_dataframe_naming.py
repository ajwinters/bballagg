#!/usr/bin/env python3
"""
Test script to verify the new NBA API dataframe naming convention
"""

import sys
import os

# Add project paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'endpoints'))
sys.path.append(os.path.join(project_root, 'endpoints', 'config'))

import nba_api.stats.endpoints as nbaapi

def test_dataframe_naming():
    """Test the dataframe naming for key endpoints"""
    
    print("🏀 Testing NBA API Dataframe Naming Convention")
    print("="*60)
    
    test_cases = [
        ('BoxScoreTraditionalV3', {'game_id': '0022400001'}),
        ('BoxScoreFourFactorsV3', {'game_id': '0022400001'}),
        ('BoxScoreAdvancedV3', {'game_id': '0022400001'}),
        ('PlayByPlayV3', {'game_id': '0022400001'}),
        ('PlayerDashboardByClutch', {'player_id': 2544, 'last_n_games': 10})
    ]
    
    for endpoint_name, params in test_cases:
        try:
            print(f"\n--- {endpoint_name} ---")
            
            # Get endpoint class and create instance
            endpoint_class = getattr(nbaapi, endpoint_name)
            instance = endpoint_class(**params)
            
            # Get dataframe names
            if hasattr(instance, 'data_sets') and instance.data_sets:
                dataframe_names = [ds.lower() for ds in instance.data_sets]
                print(f"📊 Dataframe names: {dataframe_names}")
                
                # Show suggested table names
                for i, df_name in enumerate(dataframe_names):
                    table_name = f"nba_{endpoint_name.lower()}_{df_name}"
                    print(f"   {i}: {df_name} → {table_name}")
                    
            else:
                print("❌ No data_sets metadata available")
                print("   Will use index-based naming: nba_endpoint_0, nba_endpoint_1, etc.")
                
        except Exception as e:
            print(f"❌ Error testing {endpoint_name}: {e}")
    
    print(f"\n{'='*60}")
    print("✅ Dataframe naming investigation complete!")
    print("🚀 Updated endpoint processor will use proper naming convention")

if __name__ == '__main__':
    test_dataframe_naming()
