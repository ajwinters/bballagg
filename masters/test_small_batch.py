#!/usr/bin/env python3
"""Quick test of the players collection system with just a few players"""

from players_collector import PlayersCollector

def test_small_batch():
    collector = PlayersCollector()
    
    print("🔬 QUICK TEST: Processing small batch of NBA players")
    print("=" * 50)
    
    # Get a small sample for testing
    conn = collector.db_manager.connect_to_database()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    try:
        cursor = conn.cursor()
        
        # Get just the first few players from CommonAllPlayers
        print("📋 Getting small sample of players...")
        all_players_df = collector.get_all_players_comprehensive('00')  # NBA
        
        if all_players_df is None or len(all_players_df) == 0:
            print("❌ No players found")
            return
        
        # Take just first 10 players for testing
        small_sample = all_players_df.head(10)
        print(f"✅ Got {len(small_sample)} players for testing")
        
        # Enhance them
        print("🔍 Enhancing players with detailed info...")
        enhanced_players_df = collector.enhance_players_with_detailed_info(small_sample)
        print(f"✅ Enhanced {len(enhanced_players_df)} players")
        
        # Process them
        print("💾 Processing enhanced player data...")
        players_processed = collector.process_enhanced_players_data(enhanced_players_df, 'NBA')
        print(f"✅ Processed {len(players_processed)} players")
        
        # Insert them
        if len(players_processed) > 0:
            print("📝 Inserting players into database...")
            players_inserted = collector.bulk_insert_enhanced_players(cursor, conn, players_processed, 'nba_players')
            print(f"✅ Successfully inserted {players_inserted} players!")
            
            # Show what was inserted
            cursor.execute("SELECT playerid, playername, college, draftyear FROM nba_players ORDER BY createdat DESC LIMIT 5")
            recent_players = cursor.fetchall()
            
            print("\n📊 Recently inserted players:")
            for player in recent_players:
                print(f"  {player[1]} (ID: {player[0]}) - {player[2] or 'No college'} - Draft: {player[3] or 'Undrafted'}")
        else:
            print("❌ No players to insert")
        
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == '__main__':
    test_small_batch()
