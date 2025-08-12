#!/usr/bin/env python3

"""
Test script to verify if recent failed game IDs actually work with NBA API
This will help determine if the failures are legitimate or due to a bug
"""

import sys
import os
import time

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_recent_failed_game_ids():
    """Test recent failed game IDs to see if they actually work"""
    
    # Recent failed game IDs from the log (most recent ones first)
    test_game_ids = [
        "0029400377",  # Most recent
        "0028900649", 
        "0028800984",
        "0029200200",
        "0028600500",
        "0028800428",
        "0029200183",
        "0028400369",
        "0028700560",
        "0029200329"   # Oldest in this batch
    ]
    
    # Also test some very recent game IDs (2024-25 season)
    recent_season_game_ids = [
        "0022400001",  # 2024-25 season opener format
        "0022400100",
        "0022400200",
        "0022301230",  # 2023-24 season games (should definitely work)
        "0022301200",
        "0022301150"
    ]
    
    print("[TEST] Testing Recent Failed Game IDs vs Working Game IDs")
    print("=" * 60)
    
    try:
        from nba_api.stats.endpoints import BoxScoreAdvancedV3
        print("[SUCCESS] NBA API imported successfully")
    except Exception as e:
        print(f"[ERROR] Could not import NBA API: {e}")
        return
    
    print("\n[SECTION 1] Testing Recently Failed Game IDs")
    print("-" * 40)
    
    failed_count = 0
    success_count = 0
    
    for game_id in test_game_ids:
        print(f"[TEST] Testing game ID: {game_id}...")
        
        try:
            # Try to get data using the same endpoint that's failing
            endpoint = BoxScoreAdvancedV3(game_id=game_id)
            dataframes = endpoint.get_data_frames()
            
            if dataframes and len(dataframes) > 0:
                df = dataframes[0]
                if df is not None and len(df) > 0:
                    print(f"   [SUCCESS] Got {len(df)} rows of data")
                    success_count += 1
                else:
                    print(f"   [EMPTY] API returned empty dataframe")
                    failed_count += 1
            else:
                print(f"   [NONE] API returned None/empty result")
                failed_count += 1
                
        except AttributeError as e:
            if "'NoneType' object has no attribute 'keys'" in str(e):
                print(f"   [NONE] API returned None (no data available)")
                failed_count += 1
            else:
                print(f"   [ERROR] AttributeError: {e}")
                failed_count += 1
        except Exception as e:
            print(f"   [ERROR] Exception: {e}")
            failed_count += 1
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n[RESULTS] Failed Game IDs: {success_count} success, {failed_count} failed")
    
    print("\n[SECTION 2] Testing Recent Season Game IDs (Should Work)")
    print("-" * 50)
    
    recent_success = 0
    recent_failed = 0
    
    for game_id in recent_season_game_ids:
        print(f"[TEST] Testing recent game ID: {game_id}...")
        
        try:
            endpoint = BoxScoreAdvancedV3(game_id=game_id)
            dataframes = endpoint.get_data_frames()
            
            if dataframes and len(dataframes) > 0:
                df = dataframes[0]
                if df is not None and len(df) > 0:
                    print(f"   [SUCCESS] Got {len(df)} rows of data")
                    recent_success += 1
                else:
                    print(f"   [EMPTY] API returned empty dataframe")
                    recent_failed += 1
            else:
                print(f"   [NONE] API returned None/empty result")
                recent_failed += 1
                
        except AttributeError as e:
            if "'NoneType' object has no attribute 'keys'" in str(e):
                print(f"   [NONE] API returned None (no data available)")
                recent_failed += 1
            else:
                print(f"   [ERROR] AttributeError: {e}")
                recent_failed += 1
        except Exception as e:
            print(f"   [ERROR] Exception: {e}")
            recent_failed += 1
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n[RESULTS] Recent Game IDs: {recent_success} success, {recent_failed} failed")
    
    print("\n" + "=" * 60)
    print("[ANALYSIS] Test Results Summary")
    print("=" * 60)
    
    if failed_count == len(test_game_ids):
        print("[CONCLUSION] All recently 'failed' game IDs are legitimately returning None")
        print("             This suggests the system is working correctly and these are")
        print("             older games that don't have BoxScoreAdvancedV3 data available")
    elif success_count > 0:
        print(f"[CONCLUSION] {success_count}/{len(test_game_ids)} 'failed' IDs actually work!")
        print("             This suggests there may be a bug in our processing logic")
    
    if recent_success == 0:
        print("[WARNING] Even recent season games are failing - possible API issue")
    elif recent_success > recent_failed:
        print(f"[GOOD] Recent games work well ({recent_success}/{len(recent_season_game_ids)} success)")
        print("       This confirms the NBA API and our code are working for current data")
    
    print("\n[RECOMMENDATION]")
    if failed_count == len(test_game_ids) and recent_success > 0:
        print("✅ System appears to be working correctly!")
        print("✅ Failed IDs are old games without advanced stats")  
        print("✅ Recent games work fine")
        print("💡 Consider filtering out games older than 2015 to improve speed")
    else:
        print("🔍 Further investigation needed - some failures may be fixable")

def main():
    """Main test function"""
    print("[START] NBA API Game ID Verification Test")
    test_recent_failed_game_ids()
    print("\n[COMPLETE] Test finished")

if __name__ == "__main__":
    main()
