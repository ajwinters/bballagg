#!/usr/bin/env python3
"""
Check what seasons and total games are available in master tables
"""

import json
import psycopg2

def check_master_table_data():
    """Check the actual data available in master tables"""
    print("🔍 CHECKING MASTER TABLE DATA AVAILABILITY")
    print("=" * 60)
    
    # Load database config
    with open('endpoints/config/database_config.json', 'r') as f:
        config = json.load(f)
    
    # Connect to database
    connection = psycopg2.connect(
        host=config['host'],
        database=config['name'],
        user=config['user'],
        password=config['password'],
        port=int(config['port'])
    )
    cursor = connection.cursor()
    
    # Check NBA games table
    print("📊 NBA GAMES TABLE ANALYSIS:")
    print("-" * 40)
    
    # Total games
    cursor.execute("SELECT COUNT(*) FROM nba_games")
    total_games = cursor.fetchone()[0]
    print(f"Total games in nba_games: {total_games:,}")
    
    # Games by season
    cursor.execute("""
        SELECT seasonid, COUNT(*) as game_count
        FROM nba_games 
        GROUP BY seasonid 
        ORDER BY seasonid DESC
    """)
    
    print(f"\nGames by season:")
    total_historical = 0
    for row in cursor.fetchall():
        season, count = row
        total_historical += count
        print(f"  {season}: {count:,} games")
    
    print(f"\nTotal historical games: {total_historical:,}")
    
    # Current filtered count (2023-2024 only)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM nba_games 
        WHERE seasonid LIKE '%2023%' OR seasonid LIKE '%2024%'
    """)
    filtered_games = cursor.fetchone()[0]
    print(f"Current filter (2023-2024): {filtered_games:,} games")
    
    # Check other master tables
    print(f"\n📊 OTHER MASTER TABLES:")
    print("-" * 40)
    
    for table in ['nba_players', 'nba_teams', 'gleague_games', 'wnba_games']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count:,} records")
        except Exception as e:
            print(f"{table}: Error - {e}")
    
    # Check what seasons are available in each
    print(f"\n📅 SEASON AVAILABILITY:")
    print("-" * 40)
    
    for table in ['nba_games', 'gleague_games', 'wnba_games']:
        try:
            cursor.execute(f"""
                SELECT MIN(seasonid) as earliest, MAX(seasonid) as latest, COUNT(DISTINCT seasonid) as season_count
                FROM {table}
            """)
            row = cursor.fetchone()
            print(f"{table}:")
            print(f"  Earliest season: {row[0]}")
            print(f"  Latest season: {row[1]}")
            print(f"  Total seasons: {row[2]}")
            print()
        except Exception as e:
            print(f"{table}: Error - {e}")
    
    connection.close()

if __name__ == "__main__":
    check_master_table_data()
