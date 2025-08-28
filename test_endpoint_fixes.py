#!/usr/bin/env python3
"""
Test script to validate the NBA API endpoint fixes
Tests the specific issues reported:
1. BoxScoreAdvancedV3 - NoneType errors
2. BoxScoreMiscV3 - NoneType errors  
3. BoxScoreFourFactorsV3 - NoneType errors
4. PlayerDashboardByClutch - 'int' has no attribute 'get'
5. PlayByPlayV3 - list index out of range
"""

import sys
import os
import json
import logging

# Add project paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'endpoints'))
sys.path.append(os.path.join(project_root, 'endpoints', 'config'))

import nba_api.stats.endpoints as nbaapi
from endpoints.config.nba_endpoints_config import get_endpoint_by_name

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_endpoint_configuration():
    """Test that endpoint configurations are parsed correctly"""
    
    problematic_endpoints = [
        'BoxScoreAdvancedV3',
        'BoxScoreMiscV3', 
        'BoxScoreFourFactorsV3',
        'PlayerDashboardByClutch',
        'PlayByPlayV3'
    ]
    
    logger.info("=== Testing Endpoint Configuration Parsing ===")
    
    for endpoint_name in problematic_endpoints:
        try:
            config = get_endpoint_by_name(endpoint_name)
            if config:
                logger.info(f"✅ {endpoint_name}: Configuration found")
                logger.info(f"   Parameters: {config.get('parameters', {})}")
                
                # Test parameter parsing
                for param_key, param_source in config.get('parameters', {}).items():
                    if isinstance(param_source, str):
                        logger.info(f"   - {param_key}: string value '{param_source}'")
                    elif isinstance(param_source, (int, float, bool)):
                        logger.info(f"   - {param_key}: static value {param_source} (type: {type(param_source).__name__})")
                    else:
                        logger.info(f"   - {param_key}: object {param_source} (type: {type(param_source).__name__})")
                        try:
                            source_type = param_source.get('source', 'unknown')
                            logger.info(f"     Source type: {source_type}")
                        except AttributeError as e:
                            logger.warning(f"     ⚠️  Cannot parse as dict: {e}")
            else:
                logger.error(f"❌ {endpoint_name}: Configuration not found")
                
        except Exception as e:
            logger.error(f"❌ {endpoint_name}: Error loading configuration: {e}")
        
        print()

def test_parameter_validation():
    """Test parameter validation logic"""
    
    logger.info("=== Testing Parameter Validation ===")
    
    # Import the validation function
    from endpoints.collectors.single_endpoint_processor_simple import validate_api_parameters
    
    test_cases = [
        # Valid parameters
        ("BoxScoreAdvancedV3", {"game_id": "0022400001"}, True),
        ("PlayerDashboardByClutch", {"player_id": 2544, "last_n_games": 30}, True),
        
        # Invalid parameters
        ("BoxScoreAdvancedV3", {"game_id": None}, False),
        ("BoxScoreAdvancedV3", {"game_id": "invalid"}, False),
        ("BoxScoreAdvancedV3", {"game_id": "123"}, False),  # Too short
        ("PlayerDashboardByClutch", {"player_id": "not_a_number"}, False),
        ("PlayerDashboardByClutch", {"player_id": -1}, False),  # Invalid ID
    ]
    
    for endpoint_name, params, expected_valid in test_cases:
        is_valid, error_msg = validate_api_parameters(endpoint_name, params, logger)
        
        if is_valid == expected_valid:
            status = "✅ PASS"
        else:
            status = "❌ FAIL"
        
        logger.info(f"{status} {endpoint_name} with {params}")
        if error_msg:
            logger.info(f"   Error: {error_msg}")
        print()

def test_api_class_availability():
    """Test that NBA API classes are available"""
    
    logger.info("=== Testing NBA API Class Availability ===")
    
    endpoints_to_test = [
        'BoxScoreAdvancedV3',
        'BoxScoreMiscV3', 
        'BoxScoreFourFactorsV3',
        'PlayerDashboardByClutch',
        'PlayByPlayV3'
    ]
    
    for endpoint_name in endpoints_to_test:
        try:
            endpoint_class = getattr(nbaapi, endpoint_name)
            logger.info(f"✅ {endpoint_name}: Class available - {endpoint_class}")
            
            # Try to get help to understand parameters
            try:
                help_text = str(endpoint_class.__init__.__doc__)
                if help_text and help_text != 'None':
                    logger.info(f"   Parameters info available")
                else:
                    logger.warning(f"   No parameter documentation found")
            except:
                logger.warning(f"   Could not access parameter documentation")
                
        except AttributeError:
            logger.error(f"❌ {endpoint_name}: Class not found in nba_api")
        except Exception as e:
            logger.error(f"❌ {endpoint_name}: Error accessing class: {e}")
        
        print()

def test_sample_api_calls():
    """Test sample API calls with known good data"""
    
    logger.info("=== Testing Sample API Calls ===")
    
    # Use recent NBA game ID that should exist
    test_game_id = "0022400001"  # 2024-25 season
    test_player_id = 2544  # LeBron James
    
    test_cases = [
        ("BoxScoreAdvancedV3", {"game_id": test_game_id}),
        ("BoxScoreMiscV3", {"game_id": test_game_id}),
        ("BoxScoreFourFactorsV3", {"game_id": test_game_id}),
        ("PlayerDashboardByClutch", {"player_id": test_player_id, "last_n_games": 10}),
        ("PlayByPlayV3", {"game_id": test_game_id}),
    ]
    
    for endpoint_name, params in test_cases:
        try:
            logger.info(f"Testing {endpoint_name} with {params}")
            
            endpoint_class = getattr(nbaapi, endpoint_name)
            endpoint_instance = endpoint_class(**params)
            dataframes = endpoint_instance.get_data_frames()
            
            if dataframes is None:
                logger.warning(f"⚠️  {endpoint_name}: API returned None")
            elif isinstance(dataframes, list):
                logger.info(f"✅ {endpoint_name}: Got {len(dataframes)} dataframes")
                for i, df in enumerate(dataframes):
                    if df is not None and hasattr(df, 'shape'):
                        logger.info(f"   Dataframe {i}: shape {df.shape}")
                    else:
                        logger.warning(f"   Dataframe {i}: None or invalid")
            else:
                logger.warning(f"⚠️  {endpoint_name}: Unexpected return type: {type(dataframes)}")
                
        except Exception as e:
            error_str = str(e).lower()
            if any(indicator in error_str for indicator in ['invalid', 'parameter', 'bad request', '400']):
                logger.error(f"❌ {endpoint_name}: Parameter error: {e}")
            else:
                logger.error(f"❌ {endpoint_name}: API error: {e}")
        
        print()

def main():
    """Run all tests"""
    
    print("NBA Endpoint Fixes Test Suite")
    print("="*50)
    print()
    
    try:
        test_endpoint_configuration()
        test_parameter_validation() 
        test_api_class_availability()
        test_sample_api_calls()
        
        logger.info("=== Test Suite Complete ===")
        
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
