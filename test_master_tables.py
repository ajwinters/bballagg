#!/usr/bin/env python3
"""
Quick test script to validate master table parameter resolution
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from endpoints.collectors.single_endpoint_processor_simple import load_database_config
from endpoints.collectors.rds_connection_manager import RDSConnectionManager
import json

def test_master_table_resolution():
    """Test the master table parameter resolution"""
    
    print("🧪 TESTING MASTER TABLE PARAMETER RESOLUTION")
    print("="*50)
    
    # Load database configuration
    config_path = os.path.join(os.path.dirname(__file__), 'endpoints', 'config', 'database_config.json')
    db_config = load_database_config(config_path)
    
    if not db_config:
        print("❌ Failed to load database configuration")
        return
        
    # Create connection manager
    conn_manager = RDSConnectionManager()
    if not conn_manager.create_connection():
        print("❌ Failed to establish database connection")
        return
        
    print("✅ Database connection established")
    
    # Test games table resolution
    print("\\n🎮 Testing Games Table Resolution...")
    league_tables = {
        'nba': 'nba_games',
        'gleague': 'gleague_games', 
        'wnba': 'wnba_games'
    }
    
    for league, table_name in league_tables.items():
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
            print(f"  📊 {table_name}: {count:,} games")
            
            # Test sample query
            query = f"SELECT gameid FROM {table_name} ORDER BY gamedate DESC LIMIT 3"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                games = cursor.fetchall()
            
            if games:
                game_ids = [row[0] for row in games]
                print(f"    🎯 Sample IDs: {game_ids}")
                
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
    
    # Test players table resolution  
    print("\\n👥 Testing Players Table Resolution...")
    league_tables = {
        'nba': 'nba_players',
        'gleague': 'gleague_players', 
        'wnba': 'wnba_players'
    }
    
    for league, table_name in league_tables.items():
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
            print(f"  👤 {table_name}: {count:,} players")
            
            # Test sample query
            query = f"SELECT playerid, playername FROM {table_name} ORDER BY playername LIMIT 3"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                players = cursor.fetchall()
            
            if players:
                print(f"    🎯 Sample players: {[(p[0], p[1]) for p in players]}")
                
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
    
    # Test teams table resolution
    print("\\n🏀 Testing Teams Table Resolution...")
    league_tables = {
        'nba': 'nba_teams',
        'gleague': 'gleague_teams', 
        'wnba': 'wnba_teams'
    }
    
    for league, table_name in league_tables.items():
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
            print(f"  🏆 {table_name}: {count:,} teams")
            
            # Test sample query
            query = f"SELECT teamid, teamname FROM {table_name} ORDER BY teamname LIMIT 3"
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                teams = cursor.fetchall()
            
            if teams:
                print(f"    🎯 Sample teams: {[(t[0], t[1]) for t in teams]}")
                
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")
    
    print("\\n✅ Master table resolution test completed!")
    conn_manager.close_connection()

if __name__ == "__main__":
    test_master_table_resolution()
