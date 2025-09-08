#!/usr/bin/env python3
"""
Test the incremental players update logic

This script tests that the filtering logic correctly identifies new players
by comparing CommonAllPlayers data with the existing master table.
"""

from players_collector import PlayersCollector

def test_incremental_logic():
    print("🧪 TESTING INCREMENTAL PLAYERS UPDATE LOGIC")
    print("=" * 50)
    
    collector = PlayersCollector()
    conn = collector.db_manager.connect_to_database()
    
    if not conn:
        print("❌ Could not connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Test with NBA
        print("📋 Step 1: Getting all NBA players from CommonAllPlayers...")
        all_players_df = collector.get_all_players_comprehensive('00')  # NBA
        total_api_players = len(all_players_df)
        print(f"   ✅ Found {total_api_players} total players from API")
        
        print("\n🏀 Step 2: Checking current master table...")
        cursor.execute("SELECT COUNT(*) FROM nba_players")
        total_db_players = cursor.fetchone()[0]
        print(f"   ✅ Found {total_db_players} players in master table")
        
        print("\n🔍 Step 3: Testing filter logic...")
        new_players_df = collector.filter_new_players_only(cursor, all_players_df, 'nba_players')
        new_players_count = len(new_players_df)
        
        print(f"\n📊 RESULTS:")
        print(f"   Total players in API: {total_api_players}")
        print(f"   Total players in DB:  {total_db_players}")
        print(f"   New players found:    {new_players_count}")
        print(f"   Expected new:         {total_api_players - total_db_players}")
        
        if new_players_count == (total_api_players - total_db_players):
            print(f"\n✅ INCREMENTAL LOGIC WORKING CORRECTLY!")
            print(f"   Filter correctly identified {new_players_count} new players")
        else:
            print(f"\n⚠️  POTENTIAL ISSUE WITH INCREMENTAL LOGIC")
            print(f"   Expected {total_api_players - total_db_players} new players")
            print(f"   But filter found {new_players_count} new players")
        
        if new_players_count > 0:
            print(f"\n🆕 Sample of new players that would be processed:")
            sample_new = new_players_df.head(3)
            for _, player in sample_new.iterrows():
                print(f"   - {player.get('DISPLAY_FIRST_LAST', 'Unknown')} (ID: {player.get('PERSON_ID', 'Unknown')})")
        else:
            print(f"\n🎉 Master table is up to date - no new players to add!")
        
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    test_incremental_logic()
