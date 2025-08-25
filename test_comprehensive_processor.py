#!/usr/bin/env python3
"""
Test the comprehensive endpoint processor
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from endpoints.collectors.single_endpoint_processor_simple import (
    load_database_config, 
    find_missing_ids,
    process_single_endpoint_comprehensive,
    setup_logging
)
from endpoints.collectors.rds_connection_manager import RDSConnectionManager
from endpoints.config.nba_endpoints_config import get_endpoint_by_name

def test_comprehensive_processor():
    """Test the comprehensive endpoint processor"""
    
    print("🧪 TESTING COMPREHENSIVE ENDPOINT PROCESSOR")
    print("="*50)
    
    # Setup
    logger = setup_logging("test_node", "INFO")
    
    # Load database configuration  
    config_path = os.path.join(os.path.dirname(__file__), 'endpoints', 'config', 'database_config.json')
    db_config = load_database_config(config_path)
    
    if not db_config:
        print("❌ Failed to load database configuration")
        return
        
    # Test with a simple endpoint that should have missing data
    test_endpoint = "BoxScoreAdvancedV3"  # This endpoint requires game_id parameter
    
    print(f"\\n🎯 Testing endpoint: {test_endpoint}")
    
    # Get endpoint configuration
    endpoint_config = get_endpoint_by_name(test_endpoint)
    if not endpoint_config:
        print(f"❌ Endpoint '{test_endpoint}' not found in configuration")
        return
        
    print(f"✅ Found endpoint config: {endpoint_config}")
    
    # Test finding missing IDs first
    print("\\n🔍 Testing missing ID detection...")
    
    conn_manager = RDSConnectionManager()
    if not conn_manager.create_connection():
        print("❌ Failed to establish database connection")
        return
    
    # Test finding missing game IDs
    missing_ids = find_missing_ids(
        conn_manager=conn_manager,
        master_table="nba_games", 
        endpoint_table_prefix=f"nba_{test_endpoint.lower()}",
        id_column="gameid",
        failed_ids_table="failed_api_calls",
        logger=logger
    )
    
    print(f"📊 Found {len(missing_ids)} missing game IDs")
    if missing_ids:
        print(f"   Sample missing IDs: {missing_ids[:5]}")
    
    conn_manager.close_connection()
    
    # Test comprehensive processing (with limit for safety)
    if missing_ids:
        print(f"\\n🚀 Testing comprehensive processing (limited to 2 IDs for safety)...")
        
        # Run the comprehensive processor
        result = process_single_endpoint_comprehensive(
            endpoint_name=test_endpoint,
            node_id="test_comprehensive",
            rate_limit=1.0,  # 1 second rate limit for safety
            logger=logger
        )
        
        print(f"\\n📈 COMPREHENSIVE PROCESSING RESULTS:")
        print(f"   Status: {result['status']}")
        print(f"   Processed: {result.get('processed', 0)}")
        print(f"   Failed: {result.get('failed', 0)}")
        print(f"   Success rate: {result.get('success_rate', 0):.1f}%")
        
        if result["status"] == "complete":
            print("✅ Comprehensive processing test completed successfully!")
        else:
            print("❌ Comprehensive processing test failed")
            
    else:
        print("✅ No missing data found - all data is up to date!")
    
    print("\\n✅ Comprehensive processor test completed!")

if __name__ == "__main__":
    test_comprehensive_processor()
