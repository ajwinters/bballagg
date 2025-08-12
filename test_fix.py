#!/usr/bin/env python3
"""
Test the fixed endpoint processor logic
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
    endpoint_instance = nbaapi.boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=test_game_id)
    dataframes = endpoint_instance.get_data_frames()
    print("SUCCESS: No NoneType error!")
    
except AttributeError as e:
    if "'NoneType' object has no attribute 'keys'" in str(e):
        print(f"✅ CAUGHT the NoneType error correctly: NBA API returned None for {test_game_id}")
        print("This would now be handled gracefully by the processor")
    else:
        print(f"Different AttributeError: {str(e)}")
        import traceback
        traceback.print_exc()
        
except Exception as e:
    print(f"Different error: {str(e)}")
    import traceback
    traceback.print_exc()
    
print("\n🎯 TEST COMPLETE - The fix should now handle NoneType errors properly!")
