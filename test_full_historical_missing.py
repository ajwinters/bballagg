#!/usr/bin/env python3
"""
Test missing ID detection with ALL historical games (no season filter)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from endpoints.collectors.single_endpoint_processor_simple import (
    load_database_config, 
    find_missing_ids,
    setup_logging
)
from endpoints.collectors.rds_connection_manager import RDSConnectionManager

def test_full_historical_missing_ids():
    """Test missing ID detection with full historical dataset"""
    print("🔍 TESTING FULL HISTORICAL MISSING ID DETECTION")
    print("=" * 60)
    
    # Setup logging
    logger = setup_logging("test_full_historical", "INFO")
    
    # Load database configuration
    config_path = "endpoints/config/database_config.json"
    db_config = load_database_config(config_path)
    
    # Initialize database connection manager
    conn_manager = RDSConnectionManager()
    
    try:
        conn_manager.create_connection()
        
        # Test missing game IDs (ALL HISTORICAL)
        print("📊 MISSING GAME IDS (ALL SEASONS 1983-2025):")
        print("-" * 50)
        
        missing_ids = find_missing_ids(
            conn_manager=conn_manager,
            master_table='nba_games', 
            endpoint_table_prefix='nba_boxscoreadvancedv3',
            id_column='gameid',
            failed_ids_table='failed_api_calls',
            logger=logger
        )
        
        print(f"📈 RESULTS:")
        print(f"   Total missing game IDs: {len(missing_ids):,}")
        print(f"   Sample missing IDs: {missing_ids[:10]}")
        
        # Calculate expected processing time
        processing_time_hours = len(missing_ids) * 1.0 / 3600  # 1 second per API call
        print(f"   Estimated processing time: {processing_time_hours:.1f} hours")
        
        # Also check other master tables
        print(f"\n📊 OTHER MASTER TABLES:")
        print("-" * 50)
        
        # G-League games
        gleague_missing = find_missing_ids(
            conn_manager=conn_manager,
            master_table='gleague_games', 
            endpoint_table_prefix='nba_boxscoreadvancedv3',
            id_column='gameid',
            failed_ids_table='failed_api_calls',
            logger=logger
        )
        print(f"G-League missing game IDs: {len(gleague_missing):,}")
        
        # WNBA games  
        wnba_missing = find_missing_ids(
            conn_manager=conn_manager,
            master_table='wnba_games', 
            endpoint_table_prefix='nba_boxscoreadvancedv3',
            id_column='gameid',
            failed_ids_table='failed_api_calls',
            logger=logger
        )
        print(f"WNBA missing game IDs: {len(wnba_missing):,}")
        
        # Players
        player_missing = find_missing_ids(
            conn_manager=conn_manager,
            master_table='nba_players', 
            endpoint_table_prefix='nba_commonplayerinfo',
            id_column='playerid',
            failed_ids_table='failed_api_calls',
            logger=logger
        )
        print(f"Player missing IDs: {len(player_missing):,}")
        
        # Teams
        team_missing = find_missing_ids(
            conn_manager=conn_manager,
            master_table='nba_teams', 
            endpoint_table_prefix='nba_commonteamroster',
            id_column='teamid',
            failed_ids_table='failed_api_calls',
            logger=logger
        )
        print(f"Team missing IDs: {len(team_missing):,}")
        
        print(f"\n🎯 COMPREHENSIVE PROCESSING SCALE:")
        print("-" * 50)
        total_missing = len(missing_ids) + len(gleague_missing) + len(wnba_missing)
        print(f"Total game-based missing IDs: {total_missing:,}")
        print(f"Total estimated API calls (all endpoints): {total_missing * 16:,}+")
        total_hours = total_missing * 16 * 0.3 / 3600  # 16 endpoints * 0.3 second rate limit
        print(f"Total estimated processing time: {total_hours:.0f} hours")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn_manager.close_connection()

if __name__ == "__main__":
    test_full_historical_missing_ids()
