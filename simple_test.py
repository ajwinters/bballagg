#!/usr/bin/env python3
"""
Simple test to understand the NoneType error
"""
import sys
import os
sys.path.append('src')

import nba_api.stats.endpoints as nbaapi

# Test one of the failing game IDs
test_game_id = '0028600932'  # This was failing in the logs

print(f"Testing game ID: {test_game_id}")

try:
    print("Creating endpoint instance...")
    endpoint = nbaapi.boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=test_game_id)
    
    print("Getting data frames...")
    dataframes = endpoint.get_data_frames()
    
    print(f"Dataframes type: {type(dataframes)}")
    print(f"Is None: {dataframes is None}")
    
    if dataframes is not None:
        print(f"Number of dataframes: {len(dataframes)}")
        if len(dataframes) > 0:
            print(f"First dataframe shape: {dataframes[0].shape}")
            print(f"First dataframe empty: {dataframes[0].empty}")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
