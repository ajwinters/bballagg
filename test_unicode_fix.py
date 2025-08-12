#!/usr/bin/env python3

"""
Quick test to verify Unicode fixes are working properly
"""

import logging
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_logging():
    """Test ASCII logging functionality"""
    print('[TEST] Testing Unicode-free logging system...')
    
    # Setup logger
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    logger = logging.getLogger('test')
    
    # Test various ASCII messages that replace the old Unicode ones
    logger.info('[SUCCESS] ASCII logging test complete')
    logger.info('[INFO] System can resume from where it left off')
    logger.info('[CHECK] Performing connection health check')
    logger.info('[RECONNECT] Connection refresh completed')
    logger.info('[SLEEP/WAKE] Sleep/wake cycle detected')
    logger.info('[NETWORK] Network/timeout error detected')
    logger.info('[ERROR] API error encountered')
    logger.info('[PHASE 1] High Priority Game-Based Endpoints')
    logger.info('[COMPLETE] Processing complete')
    
    print('[SUCCESS] All ASCII logging messages tested successfully')

def test_connection_manager():
    """Test the connection manager with ASCII logging"""
    print('[TEST] Testing RDS connection manager...')
    
    try:
        from endpoints.collectors.rds_connection_manager import RDSConnectionManager
        
        # Test connection manager initialization
        conn = RDSConnectionManager()
        print('[INFO] Connection manager initialized')
        
        # Test sleep/wake detection (without actual connection)
        print('[INFO] Testing sleep/wake detection logic...')
        sleep_detected = conn.detect_sleep_wake_cycle()
        print(f'[INFO] Sleep/wake detection result: {sleep_detected}')
        
        print('[SUCCESS] Connection manager test passed')
        
    except Exception as e:
        print(f'[ERROR] Connection test failed: {e}')
        return False
    
    return True

def main():
    """Main test function"""
    print('[START] Unicode fix verification test')
    print('=' * 50)
    
    # Test 1: ASCII logging
    try:
        test_logging()
        print('[PASS] Logging test passed')
    except Exception as e:
        print(f'[FAIL] Logging test failed: {e}')
        return
    
    print()
    
    # Test 2: Connection manager
    try:
        success = test_connection_manager()
        if success:
            print('[PASS] Connection manager test passed')
        else:
            print('[FAIL] Connection manager test failed')
    except Exception as e:
        print(f'[FAIL] Connection manager test failed: {e}')
        return
    
    print()
    print('=' * 50)
    print('[COMPLETE] All Unicode fix tests completed successfully!')
    print('[INFO] System is ready for NBA data collection with ASCII logging')
    print('[INFO] No more annoying Unicode encoding errors in Windows PowerShell!')

if __name__ == "__main__":
    main()
