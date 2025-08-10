"""
Quick test of the league-separated master collection in test mode
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from league_separated_master_collection import LeagueSeparatedMasterCollector

def quick_test():
    print("🧪 Quick Test - League-Separated Master Collection")
    print("=" * 60)
    
    # Initialize collector
    collector = LeagueSeparatedMasterCollector()
    
    # Run in test mode (NBA only, recent seasons)
    print("Running test mode (NBA only, last 3 seasons)...")
    results = collector.run_league_separated_collection(test_mode=True)
    
    return results

if __name__ == "__main__":
    quick_test()
