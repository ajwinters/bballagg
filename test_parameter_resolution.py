#!/usr/bin/env python3
"""
Test the actual parameter resolution logic from the processor
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from endpoints.collectors.single_endpoint_processor_simple import load_database_config
from endpoints.collectors.rds_connection_manager import RDSConnectionManager
import logging

# Setup logging to see the actual processor messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_parameter_resolution():
    """Test the parameter resolution logic that was throwing SQL errors"""
    
    print("🔧 TESTING PARAMETER RESOLUTION LOGIC")
    print("="*45)
    
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
    
    # Test the exact logic from resolve_parameters function
    print("\\n🎮 Testing from_mastergames resolution...")
    
    # Use actual master games tables found in database
    league_tables = {
        'nba': 'nba_games',
        'gleague': 'gleague_games', 
        'wnba': 'wnba_games'
    }
    
    game_ids = []
    for league, table_name in league_tables.items():
        try:
            # This is the exact query that was failing
            query = f"SELECT DISTINCT gameid FROM {table_name} WHERE seasonid LIKE '%2023%' OR seasonid LIKE '%2024%' LIMIT 20"
            print(f"  🔍 Executing: {query}")
            
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            if results:
                league_game_ids = [row[0] for row in results]
                game_ids.extend(league_game_ids)
                logger.info(f"Found {len(league_game_ids)} game IDs from {table_name}: {league_game_ids[:3]}...")
            else:
                print(f"  ⚠️  No results from {table_name}")
                
        except Exception as e:
            logger.error(f"Table {table_name} error: {e}")
            continue
    
    if game_ids:
        selected_game_id = game_ids[0]
        print(f"\\n✅ Successfully resolved game_id parameter: {selected_game_id}")
    else:
        print("\\n❌ No game IDs found")
    
    # Test players resolution
    print("\\n👥 Testing from_masterplayers resolution...")
    
    league_tables = {
        'nba': 'nba_players',
        'gleague': 'gleague_players', 
        'wnba': 'wnba_players'
    }
    
    player_ids = []
    for league, table_name in league_tables.items():
        try:
            query = f"SELECT DISTINCT playerid FROM {table_name} WHERE season LIKE '%2024%' LIMIT 20"
            print(f"  🔍 Executing: {query}")
            
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            if results:
                league_player_ids = [row[0] for row in results]
                player_ids.extend(league_player_ids)
                logger.info(f"Found {len(league_player_ids)} player IDs from {table_name}: {league_player_ids[:3]}...")
            else:
                print(f"  ⚠️  No results from {table_name}")
                
        except Exception as e:
            logger.error(f"Table {table_name} error: {e}")
            continue
    
    if player_ids:
        selected_player_id = player_ids[0]
        print(f"\\n✅ Successfully resolved player_id parameter: {selected_player_id}")
    else:
        print("\\n❌ No player IDs found")
    
    # Test teams resolution
    print("\\n🏀 Testing from_masterteams resolution...")
    
    league_tables = {
        'nba': 'nba_teams',
        'gleague': 'gleague_teams', 
        'wnba': 'wnba_teams'
    }
    
    team_ids = []
    for league, table_name in league_tables.items():
        try:
            query = f"SELECT DISTINCT teamid FROM {table_name} LIMIT 20"
            print(f"  🔍 Executing: {query}")
            
            with conn_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            if results:
                league_team_ids = [row[0] for row in results]
                team_ids.extend(league_team_ids)
                logger.info(f"Found {len(league_team_ids)} team IDs from {table_name}: {league_team_ids[:3]}...")
            else:
                print(f"  ⚠️  No results from {table_name}")
                
        except Exception as e:
            logger.error(f"Table {table_name} error: {e}")
            continue
    
    if team_ids:
        selected_team_id = team_ids[0]
        print(f"\\n✅ Successfully resolved team_id parameter: {selected_team_id}")
    else:
        print("\\n❌ No team IDs found")
    
    print("\\n✅ Parameter resolution test completed!")
    conn_manager.close_connection()

if __name__ == "__main__":
    test_parameter_resolution()
