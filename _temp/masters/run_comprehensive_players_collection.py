#!/usr/bin/env python3
"""
Comprehensive NBA Players Historical Data Collection

This script rebuilds the master players tables with ALL historical seasons,
not just the current season. This provides the proper foundation for
comprehensive player dashboard data collection.

Before: Only current season players (e.g., ~450 NBA players for 2024-25)
After: ALL player-season combinations (e.g., ~2,500 players × ~78 seasons = 195,000+ records)

Usage:
    python run_comprehensive_players_collection.py --mode test     # Test with 5 recent seasons
    python run_comprehensive_players_collection.py --mode full    # Full historical collection
"""

import argparse
import sys
import os
from datetime import datetime

# Import the enhanced players collector
from players_collector import PlayersCollector

def main():
    parser = argparse.ArgumentParser(description='Run comprehensive NBA players historical collection')
    parser.add_argument('--mode', choices=['test', 'full', 'incremental'], default='test',
                       help='Collection mode: test (5 players), full (all players), incremental (new players only)')
    parser.add_argument('--league', choices=['NBA', 'WNBA', 'G-League'], 
                       help='Collect for specific league only')
    
    args = parser.parse_args()
    
    print("🏀 NBA COMPREHENSIVE PLAYERS HISTORICAL COLLECTION")
    print("=" * 60)
    print(f"Mode: {args.mode.upper()}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.mode == 'full':
        print("\n⚠️  FULL HISTORICAL MODE")
        print("This will collect ALL players who ever played in each league:")
        print("  • NBA: ~5,115 total players (all-time)")
        print("  • WNBA: ~800 total players (all-time)")  
        print("  • G-League: ~2,200 total players (all-time)")
        print("\nThis will make ~8,000 total API calls (1 CommonAllPlayers + 1 CommonPlayerInfo per player).")
        print("Estimated time: 15-20 minutes with rate limiting.")
        
        confirm = input("\nContinue with full historical collection? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Collection cancelled.")
            return
    
    # Determine backfill mode based on arguments
    if args.mode == 'incremental':
        backfill_mode = False
        print(f"📈 Running in INCREMENTAL mode (new players only)")
    else:
        backfill_mode = True
        if args.mode == 'test':
            print(f"🧪 Running in TEST mode (all players, limited scope)")
        else:
            print(f"🔄 Running in FULL mode (all players)")
    
    try:
        collector = PlayersCollector()
        
        if args.league:
            # Single league collection
            print(f"\n🎯 Collecting {args.league} players...")
            result = collector.collect_comprehensive_players(args.league, backfill_mode)
            print(f"\nResult: {result} players collected for {args.league}")
            
        else:
            # All leagues collection
            print(f"\n🌍 Collecting all leagues players...")
            results = collector.collect_all_leagues_players_comprehensive(backfill_mode)
            
            print(f"\n📊 FINAL RESULTS:")
            total = sum(results.values())
            for league, count in results.items():
                print(f"  {league}: {count:,} players")
            print(f"  TOTAL: {total:,} players")
            
        print(f"\n✅ Collection completed at {datetime.now().strftime('%H:%M:%S')}")
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Your master players tables now contain comprehensive biographical data")
        print(f"2. Each record is unique on (player_id)")
        print(f"3. Run your PlayerDashboard endpoints - they will now collect ALL historical data!")
        print(f"\nExample dashboard collection command:")
        print(f"python single_endpoint_processor_simple.py --endpoint PlayerDashboardByShootingSplits")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during collection: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
