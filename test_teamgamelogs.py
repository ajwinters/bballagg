#!/usr/bin/env python3
"""
Test script to validate TeamGameLogs endpoint parameter handling.
"""

import sys
import os

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from nba_data_processor import NBADataProcessor

def test_teamgamelogs_params():
    """Test TeamGameLogs parameter handling"""
    print("Testing TeamGameLogs Parameter Handling")
    print("=" * 50)
    
    try:
        # Initialize processor in test mode
        processor = NBADataProcessor(test_mode=True, max_items_per_endpoint=2)
        print("✓ NBADataProcessor initialized successfully")
        
        # Test TeamGameLogs parameter detection
        endpoint_name = 'TeamGameLogs'
        config = processor.endpoint_config['endpoints'].get(endpoint_name)
        
        if config:
            print(f"✓ Found {endpoint_name} configuration")
            print(f"  → Required params: {config['required_params']}")
            
            # Test parameter resolution
            missing_data = processor.get_missing_ids_for_endpoint(endpoint_name, config)
            
            print(f"  → Generated {len(missing_data)} parameter combinations")
            if missing_data:
                print(f"  → Sample combination: {missing_data[0]}")
                
                # Verify all required parameters are present
                required_params = set(config['required_params'])
                generated_params = set(missing_data[0].keys())
                missing_required = required_params - generated_params
                
                if missing_required:
                    print(f"  ✗ Missing required parameters: {missing_required}")
                else:
                    print(f"  ✓ All required parameters present")
            
        else:
            print(f"✗ {endpoint_name} configuration not found")
        
        print("\n" + "=" * 50)
        print("TeamGameLogs parameter handling test complete!")
        return True
        
    except Exception as e:
        print(f"✗ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_teamgamelogs_params()
    sys.exit(0 if success else 1)