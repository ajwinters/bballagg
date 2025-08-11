"""
Quick Validation Test for Fixed NBA Master Tables System

This script will quickly test:
1. Database connectivity (using your working RDS setup)
2. NBA API functionality with corrected parameters
3. Data collection for a small sample
"""

import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the fixed manager
from database_manager_fixed import FixedMasterTablesManager

def run_validation():
    """Run quick validation tests"""
    print("🏀 NBA MASTER TABLES - VALIDATION TEST")
    print("=" * 50)
    
    print("📋 Running comprehensive validation...")
    print("   This will test database, API, and data collection")
    
    # Initialize the fixed manager
    manager = FixedMasterTablesManager()
    
    # Run the test
    success = manager.run_quick_test()
    
    if success:
        print(f"\n🎉 SUCCESS! The fixed system is working correctly")
        print(f"\nNext Steps:")
        print(f"   1. System is ready for comprehensive data collection")
        print(f"   2. All 3 leagues (NBA, WNBA, G-League) are accessible")
        print(f"   3. Database connectivity is confirmed")
        print(f"   4. Can proceed with full master tables creation")
        
        print(f"\n💡 Would you like to:")
        print(f"   • Run full data collection (adds ~33k missing games)")
        print(f"   • Create the complete master tables system")
        print(f"   • Set up automated scheduling")
    else:
        print(f"\n⚠️  Some issues were found during validation")
        print(f"   Please review the test output above for details")

if __name__ == "__main__":
    run_validation()
