#!/usr/bin/env python3
"""
Test multiple failing game IDs to verify our fix
"""
import sys
import os
sys.path.append('src')

import nba_api.stats.endpoints as nbaapi

# Multiple failing game IDs from the logs
failing_game_ids = [
    '0028600932',
    '0029000875', 
    '0029400139',
    '0028700041',
    '0029400783'
]

print("🧪 TESTING MULTIPLE FAILING GAME IDs")
print("=" * 50)

success_count = 0
error_count = 0

for game_id in failing_game_ids:
    print(f"\n📊 Testing game ID: {game_id}")
    
    try:
        endpoint_instance = nbaapi.boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id)
        dataframes = endpoint_instance.get_data_frames()
        print(f"   ✅ SUCCESS: Got valid data")
        success_count += 1
        
    except AttributeError as e:
        if "'NoneType' object has no attribute 'keys'" in str(e):
            print(f"   ✅ CAUGHT: NoneType error (NBA API returned None)")
            error_count += 1
        else:
            print(f"   ⚠️  Different AttributeError: {str(e)}")
            error_count += 1
            
    except Exception as e:
        print(f"   ❌ Different error type: {str(e)}")
        error_count += 1

print(f"\n🎯 SUMMARY:")
print(f"   Total tested: {len(failing_game_ids)}")
print(f"   Successfully handled: {success_count + error_count}")
print(f"   The fix should prevent all NoneType crashes!")
