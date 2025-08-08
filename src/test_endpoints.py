"""
NBA Endpoint Test Runner

Simple test runner for validating our endpoint processing system
without requiring database connectivity.
"""

import pandas as pd
import time
import sys
import os

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.append(os.path.join(os.path.dirname(__file__)))

import nba_api.stats.endpoints as nbaapi
from nba_endpoints_config import ALL_ENDPOINTS, get_endpoints_by_priority


def test_endpoint_structure():
    """Test endpoint structure and dataframe extraction"""
    print("=== TESTING NBA API ENDPOINT STRUCTURE ===")
    
    # Test parameters
    test_game = '0022201200'
    test_player = '203076'  # Anthony Davis
    test_team = '1610612747'  # Lakers
    
    # Test a few key endpoints
    test_endpoints = [
        {
            'name': 'BoxScoreTraditionalV2',
            'class': nbaapi.BoxScoreTraditionalV2,
            'params': {'game_id': test_game}
        },
        {
            'name': 'PlayerGameLog', 
            'class': nbaapi.PlayerGameLog,
            'params': {'player_id': test_player}
        },
        {
            'name': 'CommonTeamRoster',
            'class': nbaapi.CommonTeamRoster,
            'params': {'team_id': test_team}
        }
    ]
    
    results = {}
    
    for endpoint in test_endpoints:
        print(f"\nTesting {endpoint['name']}...")
        try:
            # Create endpoint instance
            instance = endpoint['class'](**endpoint['params'])
            
            # Get dataframes
            dataframes = instance.get_data_frames()
            
            # Get expected data keys
            expected_keys = list(instance.expected_data.keys()) if hasattr(instance, 'expected_data') else []
            
            print(f"  ✓ Returns {len(dataframes)} dataframes")
            print(f"  Expected keys: {expected_keys}")
            
            # Analyze each dataframe
            df_info = []
            for i, df in enumerate(dataframes):
                if not df.empty:
                    df_info.append({
                        'index': i,
                        'key': expected_keys[i] if i < len(expected_keys) else f'df_{i}',
                        'rows': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns)[:5]  # First 5 columns
                    })
                    print(f"    DataFrame {i} ({expected_keys[i] if i < len(expected_keys) else 'unnamed'}): {len(df)} rows, {len(df.columns)} cols")
                else:
                    print(f"    DataFrame {i}: EMPTY")
            
            results[endpoint['name']] = {
                'success': True,
                'dataframes': df_info,
                'expected_keys': expected_keys
            }
            
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            results[endpoint['name']] = {
                'success': False,
                'error': str(e)
            }
    
    return results


def test_table_naming():
    """Test table naming conventions"""
    print("\n=== TESTING TABLE NAMING ===")
    
    def generate_table_name(endpoint_name, dataframe_name):
        """Generate standardized table names"""
        endpoint_lower = endpoint_name.lower()
        df_name_lower = dataframe_name.lower()
        return f"{endpoint_lower}_{df_name_lower}"
    
    test_cases = [
        ('BoxScoreTraditionalV2', 'PlayerStats'),
        ('PlayerGameLog', 'PlayerGameLog'),
        ('CommonTeamRoster', 'CommonTeamRoster')
    ]
    
    for endpoint, df_name in test_cases:
        table_name = generate_table_name(endpoint, df_name)
        print(f"  {endpoint} + {df_name} = {table_name}")


def analyze_endpoint_config():
    """Analyze our endpoint configuration"""
    print("\n=== ANALYZING ENDPOINT CONFIGURATION ===")
    
    total_endpoints = 0
    for category, endpoints in ALL_ENDPOINTS.items():
        print(f"\n{category.upper()}: {len(endpoints)} endpoints")
        total_endpoints += len(endpoints)
        
        # Group by priority
        priority_counts = {}
        for ep in endpoints:
            priority = ep['priority']
            priority_counts[priority] = priority_counts.get(priority, 0) + 1
        
        for priority, count in priority_counts.items():
            print(f"  {priority} priority: {count}")
    
    print(f"\nTotal endpoints configured: {total_endpoints}")
    
    # Show high priority endpoints
    high_priority = get_endpoints_by_priority('high')
    print(f"\nHigh priority endpoints ({len(high_priority)}):")
    for ep in high_priority:
        print(f"  - {ep['endpoint']} ({list(ep['parameters'].keys())[0]})")


def main():
    """Run all tests"""
    print("NBA API Endpoint Testing")
    print("=" * 50)
    
    # Test endpoint structure
    endpoint_results = test_endpoint_structure()
    
    # Test table naming
    test_table_naming()
    
    # Analyze configuration
    analyze_endpoint_config()
    
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    
    successful_endpoints = [name for name, result in endpoint_results.items() if result['success']]
    failed_endpoints = [name for name, result in endpoint_results.items() if not result['success']]
    
    print(f"✓ Successful endpoints: {len(successful_endpoints)}")
    for name in successful_endpoints:
        print(f"  - {name}")
    
    if failed_endpoints:
        print(f"✗ Failed endpoints: {len(failed_endpoints)}")
        for name in failed_endpoints:
            print(f"  - {name}: {endpoint_results[name]['error']}")
    
    print(f"\nOverall success rate: {len(successful_endpoints) / len(endpoint_results) * 100:.1f}%")


if __name__ == "__main__":
    main()
