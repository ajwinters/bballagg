#!/usr/bin/env python3
"""
Edge case testing for NBA API endpoint fixes
Tests various error conditions to ensure robust handling
"""

import sys
import os
import logging

# Add project paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'endpoints'))
sys.path.append(os.path.join(project_root, 'endpoints', 'config'))

import nba_api.stats.endpoints as nbaapi
from endpoints.collectors.single_endpoint_processor_simple import make_api_call, validate_api_parameters

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_bad_parameters():
    """Test API calls with bad parameters to ensure proper error handling"""
    
    logger.info("=== Testing Bad Parameter Handling ===")
    
    test_cases = [
        # Invalid game IDs
        ("BoxScoreAdvancedV3", {"game_id": "0000000000"}),  # Non-existent game
        ("BoxScoreAdvancedV3", {"game_id": "invalid_id"}),  # Invalid format
        
        # Invalid player IDs  
        ("PlayerDashboardByClutch", {"player_id": 9999999, "last_n_games": 10}),  # Non-existent player
        ("PlayerDashboardByClutch", {"player_id": -1, "last_n_games": 10}),  # Invalid ID
        
        # Missing required parameters
        ("BoxScoreAdvancedV3", {}),  # No parameters
        ("PlayerDashboardByClutch", {"last_n_games": 10}),  # Missing player_id
    ]
    
    for endpoint_name, params in test_cases:
        try:
            logger.info(f"Testing {endpoint_name} with bad params: {params}")
            
            # First test parameter validation
            is_valid, error_msg = validate_api_parameters(endpoint_name, params, logger)
            if not is_valid:
                logger.info(f"   ✅ Parameter validation caught error: {error_msg}")
                continue
            
            # If validation passed, test API call
            endpoint_class = getattr(nbaapi, endpoint_name)
            result = make_api_call(endpoint_class, params, 0.1, logger)
            
            if result == "PERMANENT_ERROR":
                logger.info(f"   ✅ Correctly identified as permanent error")
            elif result is None:
                logger.info(f"   ✅ Returned None (acceptable for bad data)")
            else:
                logger.warning(f"   ⚠️  Unexpected success with bad parameters")
                
        except Exception as e:
            logger.error(f"   ❌ Unexpected exception: {e}")
        
        print()

def test_empty_responses():
    """Test handling of empty API responses"""
    
    logger.info("=== Testing Empty Response Handling ===")
    
    # Test with very old game IDs that might return empty data
    old_game_ids = [
        "0019700001",  # Very old game (1970s)
        "0020000001",  # 2000 season
    ]
    
    for game_id in old_game_ids:
        try:
            logger.info(f"Testing empty response with game_id: {game_id}")
            
            endpoint_class = nbaapi.BoxScoreAdvancedV3
            result = make_api_call(endpoint_class, {"game_id": game_id}, 0.1, logger)
            
            if result is None:
                logger.info(f"   ✅ Correctly handled empty response")
            elif result == "PERMANENT_ERROR":
                logger.info(f"   ✅ Correctly identified as permanent error")
            elif isinstance(result, list) and len(result) == 0:
                logger.info(f"   ✅ Correctly returned empty list")
            else:
                logger.info(f"   ✅ Got valid response (unexpected but OK): {len(result)} dataframes")
                
        except Exception as e:
            logger.error(f"   ❌ Exception with old game ID {game_id}: {e}")
        
        print()

def test_rate_limiting():
    """Test that rate limiting works correctly"""
    
    logger.info("=== Testing Rate Limiting ===")
    
    import time
    
    start_time = time.time()
    
    # Make a few quick API calls to test rate limiting
    endpoint_class = nbaapi.BoxScoreAdvancedV3
    params = {"game_id": "0022400001"}
    
    for i in range(2):
        logger.info(f"API call {i+1}...")
        result = make_api_call(endpoint_class, params, 1.0, logger)  # 1 second rate limit
        
        if result and isinstance(result, list):
            logger.info(f"   ✅ Call {i+1} successful")
        else:
            logger.warning(f"   ⚠️  Call {i+1} failed or returned {type(result)}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"Total duration: {duration:.2f} seconds")
    if duration >= 1.0:  # Should take at least 1 second due to rate limiting
        logger.info("   ✅ Rate limiting appears to be working")
    else:
        logger.warning("   ⚠️  Rate limiting may not be working correctly")
    
    print()

def test_retry_logic():
    """Test the retry logic with permanent vs temporary errors"""
    
    logger.info("=== Testing Retry Logic ===")
    
    # Test permanent error (bad parameter)
    logger.info("Testing permanent error handling...")
    endpoint_class = nbaapi.BoxScoreAdvancedV3
    bad_params = {"game_id": "invalid"}
    
    result = make_api_call(endpoint_class, bad_params, 0.1, logger)
    if result == "PERMANENT_ERROR":
        logger.info("   ✅ Correctly identified permanent error, no retries")
    else:
        logger.warning(f"   ⚠️  Expected PERMANENT_ERROR, got {result}")
    
    print()

def main():
    """Run edge case tests"""
    
    print("NBA Endpoint Edge Case Test Suite")
    print("="*50)
    print()
    
    try:
        test_bad_parameters()
        test_empty_responses()
        test_rate_limiting()
        test_retry_logic()
        
        logger.info("=== Edge Case Test Suite Complete ===")
        
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
