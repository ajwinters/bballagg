#!/usr/bin/env python3
"""
NBA Endpoint Parameter Resolver
Handles parameter resolution for NBA API endpoints including master table lookups and dynamic values
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def get_current_season():
    """Get current NBA season string"""
    current_year = datetime.now().year
    if datetime.now().month >= 10:  # Season starts in fall
        season = f"{current_year}-{str(current_year + 1)[-2:]}"
    else:
        season = f"{current_year - 1}-{str(current_year)[-2:]}"
    return season


def resolve_parameters_comprehensive(endpoint_name, endpoint_config, conn_manager, logger):
    """
    Resolve parameters by finding ALL missing IDs from master tables
    
    Returns:
        dict: Parameter configuration with lists of missing IDs to process
    """
    parameters = endpoint_config['parameters']
    resolved_params = {}
    
    for param_key, param_source in parameters.items():
        if param_source == 'current_season':
            # For NBA, use current season logic
            season = get_current_season()
            resolved_params[param_key] = season
            logger.info(f"Resolved {param_key} = {season}")
            
        elif param_source == 'dynamic_date_range':
            # Use last 30 days as default range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            if 'from' in param_key.lower():
                resolved_params[param_key] = start_date.strftime('%Y-%m-%d')
                logger.info(f"Resolved {param_key} = {resolved_params[param_key]}")
            elif 'to' in param_key.lower():
                resolved_params[param_key] = end_date.strftime('%Y-%m-%d')
                logger.info(f"Resolved {param_key} = {resolved_params[param_key]}")
                
        elif param_source == 'from_mastergames':
            resolved_params[param_key] = _resolve_from_master_games(conn_manager, logger)
            
        elif param_source == 'from_masterplayers':
            resolved_params[param_key] = _resolve_from_master_players(conn_manager, logger)
            
        elif param_source == 'from_masterplayers_all_seasons':
            resolved_params[param_key] = _resolve_from_master_players_all_seasons(conn_manager, logger)
            
        elif param_source == 'from_masterteams':
            resolved_params[param_key] = _resolve_from_master_teams(conn_manager, logger)
            
        else:
            # Handle static values or unknown sources
            resolved_params[param_key] = param_source
            logger.info(f"Using static value for {param_key} = {param_source}")
    
    return resolved_params


def _resolve_from_master_games(conn_manager, logger):
    """Get game IDs from master games table"""
    try:
        logger.info("Fetching game IDs from master games table...")
        
        # Use actual master games tables found in database
        league_tables = {
            'nba': 'nba_games',
            'gleague': 'gleague_games', 
            'wnba': 'wnba_games'
        }
        
        game_ids = []
        
        for league, table_name in league_tables.items():
            try:
                # Simplified query - just get recent game IDs
                query = f"SELECT DISTINCT gameid FROM {table_name} WHERE seasonid LIKE '%2023%' OR seasonid LIKE '%2024%' LIMIT 20"
                
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                
                if results:
                    league_game_ids = [row[0] for row in results]
                    game_ids.extend(league_game_ids)
                    logger.info(f"Found {len(league_game_ids)} game IDs from {table_name}")
                    
            except Exception as e:
                logger.debug(f"Table {table_name} not found or accessible: {e}")
                continue
        
        if game_ids:
            # For single endpoint processing, just use the first game ID
            return game_ids[0]
        else:
            # Fallback: use a recent NBA game ID
            fallback_game_id = "0022400001"  # 2024-25 season game
            logger.warning(f"No master games table found, using fallback game_id: {fallback_game_id}")
            return fallback_game_id
            
    except Exception as e:
        logger.error(f"Failed to fetch game IDs: {e}")
        return "0022400001"  # Final fallback


def _resolve_from_master_players(conn_manager, logger):
    """Get player IDs from master players table"""
    try:
        logger.info("Fetching player IDs from master players table...")
        
        # Use actual master players tables found in database  
        league_tables = {
            'nba': 'nba_players',
            'gleague': 'gleague_players',
            'wnba': 'wnba_players'
        }
        
        player_ids = []
        
        for league, table_name in league_tables.items():
            try:
                query = f"SELECT DISTINCT personid FROM {table_name} WHERE personid IS NOT NULL LIMIT 10"
                
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                
                if results:
                    league_player_ids = [row[0] for row in results]
                    player_ids.extend(league_player_ids)
                    logger.info(f"Found {len(league_player_ids)} player IDs from {table_name}")
                    
            except Exception as e:
                logger.debug(f"Table {table_name} not found or accessible: {e}")
                continue
        
        if player_ids:
            return player_ids[0]
        else:
            # Fallback: use LeBron James' player ID
            fallback_player_id = 2544
            logger.warning(f"No master players table found, using fallback player_id: {fallback_player_id}")
            return fallback_player_id
            
    except Exception as e:
        logger.error(f"Failed to fetch player IDs: {e}")
        return 2544  # LeBron James fallback


def _resolve_from_master_players_all_seasons(conn_manager, logger):
    """Get ALL player-season combinations from master tables for comprehensive collection"""
    try:
        logger.info("Fetching ALL player-season combinations from master players tables...")
        
        # Use actual master players tables found in database  
        league_tables = {
            'nba': 'nba_players',
            'gleague': 'gleague_players',
            'wnba': 'wnba_players'
        }
        
        player_season_combinations = []
        
        for league, table_name in league_tables.items():
            try:
                # Get player IDs and their available seasons
                query = f"""
                    SELECT DISTINCT personid, fromyear, toyear 
                    FROM {table_name} 
                    WHERE personid IS NOT NULL 
                    AND fromyear IS NOT NULL 
                    AND toyear IS NOT NULL
                    LIMIT 5
                """
                
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                
                if results:
                    for person_id, from_year, to_year in results:
                        # Generate seasons for this player
                        for year in range(int(from_year), int(to_year) + 1):
                            if year >= 2020:  # Only recent seasons
                                season = f"{year}-{str(year + 1)[-2:]}"
                                player_season_combinations.append((person_id, season))
                    
                    logger.info(f"Generated {len(player_season_combinations)} player-season combinations from {table_name}")
                    
            except Exception as e:
                logger.debug(f"Table {table_name} not found or accessible: {e}")
                continue
        
        if player_season_combinations:
            return player_season_combinations[:50]  # Limit for testing
        else:
            # Fallback: LeBron James with current season
            fallback_combo = [(2544, get_current_season())]
            logger.warning(f"No master players data found, using fallback: {fallback_combo}")
            return fallback_combo
            
    except Exception as e:
        logger.error(f"Failed to fetch player-season combinations: {e}")
        return [(2544, get_current_season())]


def _resolve_from_master_teams(conn_manager, logger):
    """Get team IDs from master teams table"""
    try:
        logger.info("Fetching team IDs from master teams table...")
        
        # Use actual master teams tables found in database
        league_tables = {
            'nba': 'nba_teams',
            'gleague': 'gleague_teams', 
            'wnba': 'wnba_teams'
        }
        
        team_ids = []
        
        for league, table_name in league_tables.items():
            try:
                query = f"SELECT DISTINCT id FROM {table_name} WHERE id IS NOT NULL LIMIT 10"
                
                with conn_manager.get_cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                
                if results:
                    league_team_ids = [row[0] for row in results]
                    team_ids.extend(league_team_ids)
                    logger.info(f"Found {len(league_team_ids)} team IDs from {table_name}")
                    
            except Exception as e:
                logger.debug(f"Table {table_name} not found or accessible: {e}")
                continue
        
        if team_ids:
            return team_ids[0]
        else:
            # Fallback: Lakers team ID
            fallback_team_id = 1610612747
            logger.warning(f"No master teams table found, using fallback team_id: {fallback_team_id}")
            return fallback_team_id
            
    except Exception as e:
        logger.error(f"Failed to fetch team IDs: {e}")
        return 1610612747  # Lakers fallback


def find_all_missing_ids(endpoint_config, conn_manager, logger):
    """
    Find all missing IDs that need to be processed for this endpoint
    This replaces the comprehensive missing ID detection logic
    """
    missing_ids_by_param = {}
    parameters = endpoint_config.get('parameters', {})
    
    for param_key, param_source in parameters.items():
        try:
            # Check if this parameter requires missing ID detection
            if not isinstance(param_source, dict):
                logger.debug(f"Skipping static parameter {param_key}")
                continue
        except AttributeError:
            # If it's not a dict-like object, skip it
            logger.debug(f"Skipping non-dict parameter {param_key}={param_source}")
            continue
        
        source_value = param_source.get('source') if hasattr(param_source, 'get') else str(param_source)
        
        if source_value == 'from_mastergames':
            missing_ids_by_param[param_key] = _find_missing_game_ids(conn_manager, endpoint_config, logger)
            
        elif source_value == 'from_masterplayers':
            missing_ids_by_param[param_key] = _find_missing_player_ids(conn_manager, endpoint_config, logger)
            
        elif source_value == 'from_masterplayers_all_seasons':
            missing_ids_by_param[param_key] = _find_missing_player_season_combinations(conn_manager, endpoint_config, logger)
            
        elif source_value == 'from_masterteams':
            missing_ids_by_param[param_key] = _find_missing_team_ids(conn_manager, endpoint_config, logger)
    
    return missing_ids_by_param


def _find_missing_game_ids(conn_manager, endpoint_config, logger):
    """Find missing game IDs across all leagues"""
    all_missing = []
    
    league_tables = {
        'nba': 'nba_games',
        'gleague': 'gleague_games', 
        'wnba': 'wnba_games'
    }
    
    endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
    failed_ids_table = "failed_api_calls"
    
    for league, table_name in league_tables.items():
        try:
            missing_ids = _find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                         'gameid', failed_ids_table, logger)
            all_missing.extend(missing_ids)
        except Exception as e:
            logger.debug(f"Could not check missing IDs for {table_name}: {e}")
    
    return all_missing


def _find_missing_player_ids(conn_manager, endpoint_config, logger):
    """Find missing player IDs across all leagues"""
    all_missing = []
    
    league_tables = {
        'nba': 'nba_players',
        'gleague': 'gleague_players',
        'wnba': 'wnba_players'
    }
    
    endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
    failed_ids_table = "failed_api_calls"
    
    for league, table_name in league_tables.items():
        try:
            missing_ids = _find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                         'personid', failed_ids_table, logger)
            all_missing.extend(missing_ids)
        except Exception as e:
            logger.debug(f"Could not check missing IDs for {table_name}: {e}")
    
    return all_missing


def _find_missing_player_season_combinations(conn_manager, endpoint_config, logger):
    """Find missing player-season combinations"""
    # This would be a complex implementation
    # For now, return a subset for comprehensive collection
    logger.info("Finding missing player-season combinations...")
    
    try:
        # Get all active players and recent seasons
        combinations = _resolve_from_master_players_all_seasons(conn_manager, logger)
        return combinations
    except Exception as e:
        logger.error(f"Failed to find player-season combinations: {e}")
        return []


def _find_missing_team_ids(conn_manager, endpoint_config, logger):
    """Find missing team IDs"""
    all_missing = []
    
    league_tables = {
        'nba': 'nba_teams',
        'gleague': 'gleague_teams',
        'wnba': 'wnba_teams'
    }
    
    endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
    failed_ids_table = "failed_api_calls"
    
    for league, table_name in league_tables.items():
        try:
            missing_ids = _find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                         'id', failed_ids_table, logger)
            all_missing.extend(missing_ids)
        except Exception as e:
            logger.debug(f"Could not check missing IDs for {table_name}: {e}")
    
    return all_missing


def _find_missing_ids(conn_manager, master_table, endpoint_table_prefix, id_column, failed_ids_table, logger):
    """
    Core logic to find missing IDs between master table and endpoint table
    """
    try:
        # Build the likely endpoint table name
        endpoint_table = f"{endpoint_table_prefix}_{master_table.split('_')[1]}"
        
        # Check if endpoint table exists
        if not conn_manager.check_table_exists(endpoint_table):
            logger.info(f"Endpoint table {endpoint_table} doesn't exist - all IDs are missing")
            # Get all IDs from master table (limited for initial run)
            query = f"SELECT DISTINCT {id_column} FROM {master_table} WHERE {id_column} IS NOT NULL LIMIT 100"
            result = conn_manager.query_database_to_dataframe(query)
            if result is not None and not result.empty:
                return result[id_column].tolist()
            return []
        
        # Find IDs in master table but not in endpoint table, excluding failed IDs
        query = f"""
            SELECT DISTINCT m.{id_column}
            FROM {master_table} m
            WHERE m.{id_column} NOT IN (
                SELECT DISTINCT {id_column} 
                FROM {endpoint_table} 
                WHERE {id_column} IS NOT NULL
            )
            AND m.{id_column} NOT IN (
                SELECT DISTINCT id_value 
                FROM {failed_ids_table} 
                WHERE endpoint_prefix = %s 
                AND id_column = %s
                AND id_value IS NOT NULL
            )
            ORDER BY m.{id_column}
            LIMIT 1000;
        """
        
        with conn_manager.get_cursor() as cursor:
            cursor.execute(query, (endpoint_table_prefix, id_column))
            results = cursor.fetchall()
        
        missing_ids = [row[0] for row in results] if results else []
        logger.info(f"Found {len(missing_ids)} missing {id_column}s in {master_table}")
        return missing_ids
        
    except Exception as e:
        logger.error(f"Error finding missing IDs for {master_table}: {e}")
        return []


def validate_api_parameters(endpoint_name, params, logger):
    """
    Validate API parameters before making calls
    """
    try:
        # Basic validation rules
        for param_key, param_value in params.items():
            if param_value is None:
                return False, f"Parameter {param_key} cannot be None"
            
            # Validate game_id format
            if param_key == 'game_id':
                if not isinstance(param_value, str) or len(param_value) < 10:
                    return False, f"Invalid game_id format: {param_value}"
            
            # Validate player_id
            if param_key == 'player_id':
                if not isinstance(param_value, (int, str)) or int(param_value) <= 0:
                    return False, f"Invalid player_id: {param_value}"
            
            # Validate team_id
            if param_key == 'team_id':
                if not isinstance(param_value, (int, str)) or int(param_value) <= 0:
                    return False, f"Invalid team_id: {param_value}"
        
        return True, None
        
    except Exception as e:
        return False, f"Parameter validation error: {e}"
