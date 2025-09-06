#!/usr/bin/env python3
"""
Weekly NBA Players Incremental Update

This script is designed for weekly runs to add only NEW players to the master tables.
It compares the current CommonAllPlayers data with the existing master table and only
processes the difference (new players that joined the league).

Process:
1. Pull ALL players from CommonAllPlayers (one API call)
2. Compare with existing master table to find NEW players only
3. For each NEW player, call CommonPlayerInfo to get detailed data
4. Insert only the new players

Usage:
    python weekly_players_update.py --league NBA     # Update specific league
    python weekly_players_update.py                  # Update all leagues
"""

import argparse
import sys
import os
from datetime import datetime

# Import the enhanced players collector
from players_collector import PlayersCollector

def main():
    parser = argparse.ArgumentParser(description='Weekly incremental NBA players update')
    parser.add_argument('--league', choices=['NBA', 'WNBA', 'G-League'], 
                       help='Update specific league only (default: all leagues)')
    parser.add_argument('--force-all', action='store_true',
                       help='Force update all players (same as backfill mode)')
    
    args = parser.parse_args()
    
    print("🏀 WEEKLY NBA PLAYERS INCREMENTAL UPDATE")
    print("=" * 50)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Determine mode
    if args.force_all:
        backfill_mode = True
        print("🔄 Mode: FULL UPDATE (all players)")
    else:
        backfill_mode = False
        print("📈 Mode: INCREMENTAL UPDATE (new players only)")
    
    try:
        collector = PlayersCollector()
        
        if args.league:
            # Single league update
            print(f"\n🎯 Updating {args.league} players...")
            result = collector.collect_comprehensive_players(args.league, backfill_mode)
            
            if backfill_mode:
                print(f"\nResult: {result} total players in {args.league} master table")
            else:
                print(f"\nResult: {result} NEW players added to {args.league} master table")
            
        else:
            # All leagues update
            print(f"\n🌍 Updating all leagues players...")
            results = collector.collect_all_leagues_players_comprehensive(backfill_mode)
            
            print(f"\n📊 FINAL RESULTS:")
            total = sum(results.values()) if results else 0
            for league, count in results.items():
                if backfill_mode:
                    print(f"  {league}: {count:,} total players")
                else:
                    print(f"  {league}: {count:,} NEW players added")
            
            if backfill_mode:
                print(f"  TOTAL: {total:,} players in all master tables")
            else:
                print(f"  TOTAL: {total:,} new players added across all leagues")
            
        print(f"\n✅ Update completed at {datetime.now().strftime('%H:%M:%S')}")
        
        # Calculate total for single league case
        if args.league:
            total = result
        
        if not backfill_mode and total == 0:
            print(f"\n🎉 No new players found - master tables are up to date!")
        elif not backfill_mode and total > 0:
            print(f"\n🆕 Added {total} new players to master tables")
            print(f"💡 TIP: Run your PlayerDashboard endpoints to collect data for new players")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Update interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during update: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
