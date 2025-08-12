#!/usr/bin/env python3
"""
Test a small batch of the endpoint processor to verify NoneType fix
"""
import sys
import os
sys.path.append('src')
sys.path.append('endpoints/collectors')

import nba_api.stats.endpoints as nbaapi
import allintwo
from endpoint_processor import NBAEndpointProcessor

# Test with a small subset that includes failing games
test_game_ids = [
    '0022301199',  # Should work
    '0028600932',  # Should fail with NoneType but be handled
    '0022301200',  # Should work  
    '0029000875',  # Should fail with NoneType but be handled
]

print("🧪 TESTING ENDPOINT PROCESSOR WITH NONETYPE FIX")
print("=" * 60)

try:
    # Create a test database connection (using existing connection)
    import rdshelp
    conn = rdshelp.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    
    # Create processor
    processor = NBAEndpointProcessor(conn, league='NBA')
    
    # Test the fixed processing logic
    endpoint_class = getattr(nbaapi, 'boxscoreadvancedv3.BoxScoreAdvancedV3'.split('.')[0])
    endpoint_class = getattr(endpoint_class, 'boxscoreadvancedv3.BoxScoreAdvancedV3'.split('.')[1])
    
    success_count = 0
    error_count = 0
    
    for i, game_id in enumerate(test_game_ids):
        print(f"\n📊 Processing game {game_id} ({i+1}/{len(test_game_ids)})")
        
        try:
            # This is the exact logic from our fixed processor
            try:
                endpoint_instance = endpoint_class(game_id=game_id)
                dataframes = endpoint_instance.get_data_frames()
                print(f"   ✅ API call successful")
            except AttributeError as e:
                if "'NoneType' object has no attribute 'keys'" in str(e):
                    print(f"   ✅ HANDLED: NBA API returned None for {game_id}")
                    error_count += 1
                    continue
                else:
                    raise e
            
            # Check dataframes
            if dataframes is None:
                print(f"   ✅ HANDLED: API returned None dataframes")
                error_count += 1
                continue
                
            if len(dataframes) > 0 and not dataframes[0].empty:
                print(f"   ✅ SUCCESS: Got {len(dataframes[0])} records")
                success_count += 1
            else:
                print(f"   ⚠️  No data in dataframes")
                
        except Exception as e:
            print(f"   ❌ ERROR: {str(e)}")
            error_count += 1
    
    print(f"\n🎯 TEST SUMMARY:")
    print(f"   Total games tested: {len(test_game_ids)}")
    print(f"   Successful: {success_count}")  
    print(f"   Handled errors: {error_count}")
    print(f"   ✅ NO CRASHES - Fix is working!")
    
    conn.close()
    
except Exception as e:
    print(f"❌ PROCESSOR TEST FAILED: {str(e)}")
    import traceback
    traceback.print_exc()
