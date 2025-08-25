#!/usr/bin/env python3
"""
Quick test for the simplified single endpoint processor
"""

import subprocess
import sys
import os

def test_simplified_processor():
    """Test the simplified processor locally"""
    
    # Change to endpoints directory
    endpoints_dir = os.path.join(os.path.dirname(__file__))
    os.chdir(endpoints_dir)
    
    print("Testing simplified single endpoint processor...")
    print("=" * 50)
    
    # Test with LeagueDashPlayerBioStats (simple endpoint)
    cmd = [
        sys.executable, 
        "collectors/single_endpoint_processor_simple.py",
        "--endpoint", "LeagueDashPlayerBioStats",
        "--node-id", "test_local_node", 
        "--rate-limit", "1.0",
        "--db-config", "config/database_config.json",
        "--log-level", "INFO"
    ]
    
    print(f"Command: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        print("STDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
        print(f"\nReturn code: {result.returncode}")
        
        if result.returncode == 0:
            print("\n✅ Test PASSED - Simplified processor works!")
        else:
            print("\n❌ Test FAILED - Check errors above")
            
    except subprocess.TimeoutExpired:
        print("❌ Test TIMEOUT - Process took too long")
    except Exception as e:
        print(f"❌ Test ERROR: {e}")

if __name__ == '__main__':
    test_simplified_processor()
