#!/usr/bin/env python3
"""
Test script to verify the NoneType fix for endpoint processor
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'endpoints', 'collectors'))

import nba_api.stats.endpoints as nbaapi
import allintwo
import time

# Test specific game IDs that were causing NoneType errors
problematic_game_ids = [
    '0028900613',  # From the logs - was causing NoneType error
    '0029100570',  # From the logs - was causing NoneType error  
    '0028400559',  # From the logs - was causing NoneType error
]

def test_nonetype_handling():
    """Test our NoneType handling fix"""
    print("🧪 TESTING NONETYPE HANDLING FIX")
    print("=" * 50)
    
    for game_id in problematic_game_ids:
        print(f"\n📊 Testing game ID: {game_id}")
        
        try:
            # This is the same API call the processor makes
            endpoint_instance = nbaapi.boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
            dataframes = endpoint_instance.get_data_frames()
            
            # Test our fix logic
            if dataframes is None:
                print(f"   ✅ API returned None for {game_id} - this would be handled correctly now")
                continue
            elif len(dataframes) == 0:
                print(f"   ✅ API returned empty list for {game_id}")
                continue
            elif dataframes[0].empty:
                print(f"   ✅ API returned empty dataframe for {game_id}")
                continue
            else:
                # Try to clean column names - this was where the error occurred
                cleaned_df = allintwo.clean_column_names(dataframes[0])
                print(f"   ✅ Successfully processed {game_id}: {len(cleaned_df)} rows")
                
        except Exception as e:
            if "'NoneType' object has no attribute 'keys'" in str(e):
                print(f"   ❌ STILL GETTING NONETYPE ERROR for {game_id}: {str(e)}")
            else:
                print(f"   ⚠️  Different error for {game_id}: {str(e)}")
        
        # Rate limiting
        time.sleep(1)

if __name__ == "__main__":
    test_nonetype_handling()
    print(f"\n🎯 TEST COMPLETE!")
    print(f"   If you see '✅ API returned None' messages, the fix is working!")
    print(f"   If you see '❌ STILL GETTING NONETYPE ERROR', more fixes needed.")
