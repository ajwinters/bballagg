#!/usr/bin/env python3
"""
Simple test for single endpoint processing with column name fixes
"""

import os
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.single_endpoint_processor import process_single_endpoint, setup_logging, load_database_config

def test_single_endpoint():
    """Test a single endpoint to verify fixes"""
    
    # Setup logging
    logger = setup_logging("test_node", "INFO")
    
    # Load database config
    try:
        db_config = load_database_config("config/database_config.json")
        logger.info(f"Database config loaded: {db_config['host']}")
    except Exception as e:
        logger.error(f"Failed to load database config: {e}")
        return False
    
    # Test BoxScoreTraditionalV2 (the one with 'to' column issue)
    endpoint_name = "BoxScoreTraditionalV2"
    node_id = "test_column_fix"
    rate_limit = 1.0
    
    logger.info(f"Testing {endpoint_name} with column name fixes...")
    
    try:
        success, summary = process_single_endpoint(endpoint_name, node_id, rate_limit, logger)
        
        if success:
            logger.info(f"✅ {endpoint_name} processed successfully!")
            logger.info(f"Summary: {summary}")
        else:
            logger.error(f"❌ {endpoint_name} failed")
            logger.error(f"Summary: {summary}")
        
        return success
        
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        return False

if __name__ == '__main__':
    test_single_endpoint()
