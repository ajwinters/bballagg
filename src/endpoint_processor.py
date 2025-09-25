#!/usr/bin/env python3
"""
Simplified Single Endpoint NBA Data Processor for SLURM
Processes one endpoint per job for parallel execution - streamlined version
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

# Add project paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)
sys.path.append(os.path.join(project_root, 'endpoints'))
sys.path.append(os.path.join(project_root, 'endpoints', 'config'))

import pandas as pd
import nba_api.stats.endpoints as nbaapi
from rds_connection_manager import RDSConnectionManager
from config.endpoints_config import get_endpoint_by_name
from dataframe_name_matcher import match_dataframes_to_names
from player_dashboard_enhancer import (
    is_player_dashboard_endpoint, 
    enhance_player_dashboard_dataframes,
    validate_player_dashboard_data
)

def setup_logging(node_id, log_level='INFO'):
    """Setup logging for this specific node"""
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create log filename with timestamp and node ID
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'nba_processor_{node_id}_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"=== NBA Data Processor Started ===")
    logger.info(f"Node ID: {node_id}")
    logger.info(f"Log file: {log_filename}")
    
    return logger

def load_database_config(config_path):
    """Load database configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Set environment variables for the connection manager
        os.environ['DB_HOST'] = config['host']
        os.environ['DB_NAME'] = config['name'] 
        os.environ['DB_USER'] = config['user']
        os.environ['DB_PASSWORD'] = config['password']
        os.environ['DB_PORT'] = str(config['port'])
        os.environ['DB_SSLMODE'] = config.get('ssl_mode', 'require')
        
        return config
        
    except Exception as e:
        raise Exception(f"Failed to load database config from {config_path}: {e}")

def find_missing_ids(conn_manager, master_table, endpoint_table_prefix, id_column, failed_ids_table, logger):
    """
    Find IDs from master table that aren't in endpoint tables and haven't failed before
    
    Args:
        conn_manager: Database connection manager
        master_table: Source table with all possible IDs
        endpoint_table_prefix: Prefix of endpoint tables to check against
        id_column: Column name for the ID (e.g., 'gameid', 'playerid')
        failed_ids_table: Table tracking failed API calls
        logger: Logger instance
    
    Returns:
        List of missing IDs that should be processed
    """
    try:
        with conn_manager.get_cursor() as cursor:
            # Get all IDs from master table - ORDER BY DATE DESC (latest games first)
            if 'game' in master_table:
                cursor.execute(f"""
                    SELECT DISTINCT {id_column}, gamedate
                    FROM {master_table} 
                    ORDER BY gamedate DESC, {id_column} DESC
                """)
                # Extract just the IDs but maintain the latest-first order
                all_ids_ordered = [row[0] for row in cursor.fetchall()]
                all_ids = set(all_ids_ordered)
                logger.info(f"Found {len(all_ids)} games in {master_table}, ordered latest first")
                
            elif 'player' in master_table:
                cursor.execute(f"""
                    SELECT DISTINCT {id_column} 
                    FROM {master_table} 
                    ORDER BY {id_column} DESC
                """)
                all_ids_ordered = [row[0] for row in cursor.fetchall()]
                all_ids = set(all_ids_ordered)
                
            else:  # teams
                cursor.execute(f"""
                    SELECT DISTINCT {id_column} 
                    FROM {master_table} 
                    ORDER BY {id_column} DESC
                """)
                all_ids_ordered = [row[0] for row in cursor.fetchall()]
                all_ids = set(all_ids_ordered)
            
            # Get existing IDs from endpoint tables
            existing_ids = set()
            
            # Find endpoint tables with this prefix
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE %s
            """, (f"{endpoint_table_prefix}%",))
            
            endpoint_tables = [row[0] for row in cursor.fetchall()]
            
            for table in endpoint_tables:
                try:
                    cursor.execute(f"SELECT DISTINCT {id_column} FROM {table}")
                    table_ids = set(row[0] for row in cursor.fetchall())
                    existing_ids.update(table_ids)
                except Exception as e:
                    logger.debug(f"Could not check {table}: {e}")
                    continue
            
            # Get failed IDs to exclude
            failed_ids = set()
            try:
                cursor.execute(f"""
                    SELECT {id_column} 
                    FROM {failed_ids_table} 
                    WHERE endpoint_prefix = %s
                """, (endpoint_table_prefix,))
                failed_ids = set(row[0] for row in cursor.fetchall())
            except Exception as e:
                logger.debug(f"Failed IDs table not accessible: {e}")
            
            # Calculate missing IDs and preserve latest-first order
            if 'game' in master_table:
                # For games, maintain chronological order (latest first)
                missing_ids = [game_id for game_id in all_ids_ordered 
                              if game_id not in existing_ids and game_id not in failed_ids]
                logger.info(f"Missing IDs will be processed in latest-first order")
            else:
                # For players/teams, use set operations then sort
                missing_ids_set = all_ids - existing_ids - failed_ids
                missing_ids = sorted(list(missing_ids_set), reverse=True)  # Descending order
            
            logger.info(f"ID Analysis for {master_table}:")
            logger.info(f"  Total IDs: {len(all_ids)}")
            logger.info(f"  Existing: {len(existing_ids)}")
            logger.info(f"  Failed: {len(failed_ids)}")
            logger.info(f"  Missing: {len(missing_ids)}")
            
            if missing_ids and 'game' in master_table:
                logger.info(f"  Processing order: Latest games first")
                logger.info(f"  First 5 games to process: {missing_ids[:5]}")
                logger.info(f"  Last 5 games to process: {missing_ids[-5:]}")
            
            return missing_ids
            
    except Exception as e:
        logger.error(f"Error finding missing IDs: {e}")
        return []

def record_failed_id(conn_manager, failed_ids_table, endpoint_prefix, id_column, id_value, error_message, logger):
    """Record an ID that failed to process to prevent future attempts"""
    try:
        # Create failed IDs table if it doesn't exist
        with conn_manager.get_cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {failed_ids_table} (
                    id SERIAL PRIMARY KEY,
                    endpoint_prefix VARCHAR(255) NOT NULL,
                    id_column VARCHAR(50) NOT NULL,
                    id_value VARCHAR(255) NOT NULL,
                    error_message TEXT,
                    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(endpoint_prefix, id_column, id_value)
                )
            """)
            
            # Insert failed ID
            cursor.execute(f"""
                INSERT INTO {failed_ids_table} 
                (endpoint_prefix, id_column, id_value, error_message)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (endpoint_prefix, id_column, id_value) 
                DO UPDATE SET 
                    error_message = EXCLUDED.error_message,
                    failed_at = CURRENT_TIMESTAMP
            """, (endpoint_prefix, id_column, id_value, str(error_message)[:500]))
            
        conn_manager.connection.commit()
        logger.warning(f"Recorded failed ID: {id_value} for {endpoint_prefix}")
        
    except Exception as e:
        logger.error(f"Could not record failed ID: {e}")

def resolve_parameters_comprehensive(endpoint_name, endpoint_config, conn_manager, logger):
    """
    Resolve parameters by finding ALL missing IDs from master tables
    
    Returns:
        dict: Parameter configuration with lists of missing IDs to process
    """
    """Resolve endpoint parameters"""
    parameters = endpoint_config['parameters']
    resolved_params = {}
    
    for param_key, param_source in parameters.items():
        if param_source == 'current_season':
            # For NBA, use current season logic
            current_year = datetime.now().year
            if datetime.now().month >= 10:  # Season starts in fall
                season = f"{current_year}-{str(current_year + 1)[-2:]}"
            else:
                season = f"{current_year - 1}-{str(current_year)[-2:]}"
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
            # Get game IDs from master games table
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
                        # Simplified query - just get recent game IDs without ordering by gamedate
                        query = f"SELECT DISTINCT gameid FROM {table_name} WHERE seasonid LIKE '%2023%' OR seasonid LIKE '%2024%' LIMIT 20"
                        
                        with conn_manager.get_cursor() as cursor:
                            cursor.execute(query)
                            results = cursor.fetchall()
                        
                        if results:
                            league_game_ids = [row[0] for row in results]
                            game_ids.extend(league_game_ids)
                            logger.info(f"Found {len(league_game_ids)} game IDs from {table_name}: {league_game_ids[:3]}...")
                            
                    except Exception as e:
                        logger.debug(f"Table {table_name} not found or accessible: {e}")
                        continue
                
                if game_ids:
                    # For single endpoint processing, just use the first game ID
                    resolved_params[param_key] = game_ids[0]  
                    logger.info(f"Resolved {param_key} = {resolved_params[param_key]}")
                else:
                    # Fallback: use a recent NBA game ID (this season's game)
                    fallback_game_id = "0022400001"  # 2024-25 season game
                    logger.warning(f"No master games table found, using fallback game_id: {fallback_game_id}")
                    resolved_params[param_key] = fallback_game_id
                    
            except Exception as e:
                logger.error(f"Failed to fetch game IDs: {e}")
                # Final fallback
                fallback_game_id = "0022400001"  # 2024-25 season game
                logger.warning(f"Using fallback game_id: {fallback_game_id}")
                resolved_params[param_key] = fallback_game_id
                
        elif param_source == 'from_masterplayers':
            # Get player IDs from master players table
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
                        # Simplified query - just get player IDs without complex ordering
                        query = f"SELECT DISTINCT playerid FROM {table_name} WHERE season LIKE '%2024%' LIMIT 20"
                        
                        with conn_manager.get_cursor() as cursor:
                            cursor.execute(query)
                            results = cursor.fetchall()
                        
                        if results:
                            league_player_ids = [row[0] for row in results]
                            player_ids.extend(league_player_ids)
                            logger.info(f"Found {len(league_player_ids)} player IDs from {table_name}: {league_player_ids[:3]}...")
                            
                    except Exception as e:
                        logger.debug(f"Table {table_name} not found or accessible: {e}")
                        continue
                
                if player_ids:
                    resolved_params[param_key] = player_ids[0]
                    logger.info(f"Resolved {param_key} = {resolved_params[param_key]}")
                else:
                    # Fallback: use LeBron James' player ID
                    fallback_player_id = 2544  # LeBron James
                    logger.warning(f"No master players table found, using fallback player_id: {fallback_player_id}")
                    resolved_params[param_key] = fallback_player_id
                    
            except Exception as e:
                logger.error(f"Failed to fetch player IDs: {e}")
                fallback_player_id = 2544  # LeBron James
                logger.warning(f"Using fallback player_id: {fallback_player_id}")
                resolved_params[param_key] = fallback_player_id
                
        elif param_source == 'from_masterplayers_all_seasons':
            # Get ALL player-season combinations from master tables for comprehensive collection
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
                        # Get ALL unique player-season combinations
                        query = f"""
                            SELECT DISTINCT playerid, season 
                            FROM {table_name} 
                            WHERE playerid IS NOT NULL 
                            AND season IS NOT NULL
                            ORDER BY season DESC, playerid ASC
                        """
                        
                        with conn_manager.get_cursor() as cursor:
                            cursor.execute(query)
                            results = cursor.fetchall()
                        
                        if results:
                            combinations = [(row[0], row[1]) for row in results]
                            player_season_combinations.extend(combinations)
                            logger.info(f"Found {len(combinations)} player-season combinations from {table_name}")
                            logger.info(f"Sample: {combinations[:3]}...")
                            
                    except Exception as e:
                        logger.debug(f"Table {table_name} not found or accessible: {e}")
                        continue
                
                if player_season_combinations:
                    # Store combinations for comprehensive processing 
                    # The processor will iterate through all of these
                    resolved_params['player_season_combinations'] = player_season_combinations
                    
                    # For now, set the first combination as the default
                    first_player, first_season = player_season_combinations[0]
                    if param_key == 'player_id':
                        resolved_params[param_key] = first_player
                    elif param_key == 'season':
                        resolved_params[param_key] = first_season
                        
                    logger.info(f"Found {len(player_season_combinations)} total player-season combinations")
                    logger.info(f"Will process comprehensive data for all players and all seasons")
                else:
                    # Fallback to current season approach
                    logger.warning("No player-season combinations found, falling back to current season")
                    if param_key == 'player_id':
                        resolved_params[param_key] = 2544  # LeBron James
                    elif param_key == 'season':
                        current_year = datetime.now().year
                        if datetime.now().month >= 10:
                            season = f"{current_year}-{str(current_year + 1)[-2:]}"
                        else:
                            season = f"{current_year - 1}-{str(current_year)[-2:]}"
                        resolved_params[param_key] = season
                    
            except Exception as e:
                logger.error(f"Failed to fetch player-season combinations: {e}")
                # Ultimate fallback
                if param_key == 'player_id':
                    resolved_params[param_key] = 2544
                elif param_key == 'season':
                    resolved_params[param_key] = "2024-25"
                
        elif param_source == 'from_masterteams':
            # Get team IDs from master teams table
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
                        # Simplified query - just get team IDs without ordering
                        query = f"SELECT DISTINCT teamid FROM {table_name} LIMIT 20"
                        
                        with conn_manager.get_cursor() as cursor:
                            cursor.execute(query)
                            results = cursor.fetchall()
                        
                        if results:
                            league_team_ids = [row[0] for row in results]
                            team_ids.extend(league_team_ids)
                            logger.info(f"Found {len(league_team_ids)} team IDs from {table_name}: {league_team_ids[:3]}...")
                            
                    except Exception as e:
                        logger.debug(f"Table {table_name} not found or accessible: {e}")
                        continue
                
                if team_ids:
                    resolved_params[param_key] = team_ids[0]
                    logger.info(f"Resolved {param_key} = {resolved_params[param_key]}")
                else:
                    # Fallback: use Lakers team ID
                    fallback_team_id = 1610612747  # Los Angeles Lakers
                    logger.warning(f"No master teams table found, using fallback team_id: {fallback_team_id}")
                    resolved_params[param_key] = fallback_team_id
                    
            except Exception as e:
                logger.error(f"Failed to fetch team IDs: {e}")
                fallback_team_id = 1610612747  # Los Angeles Lakers
                logger.warning(f"Using fallback team_id: {fallback_team_id}")
                resolved_params[param_key] = fallback_team_id
                
        else:
            logger.warning(f"Unknown parameter source: {param_source}")
            return None
    
    return resolved_params

def make_api_call(endpoint_class, params, rate_limit, logger):
    """Make NBA API call with intelligent retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Making API call (attempt {attempt + 1}/{max_retries}) with params: {params}")
            endpoint_instance = endpoint_class(**params)
            dataframes = endpoint_instance.get_data_frames()
            
            if dataframes is None:
                logger.warning("API returned None - no data available for these parameters")
                return None
                
            logger.info(f"API call successful - got {len(dataframes)} dataframes")
            return dataframes
            
        except Exception as e:
            error_str = str(e).lower()
            logger.warning(f"API call failed (attempt {attempt + 1}): {str(e)}")
            
            # Don't retry for parameter errors, authentication issues, or permanent failures
            permanent_error_indicators = [
                'invalid game id',
                'invalid player id', 
                'invalid team id',
                'bad request',
                '400',
                '401', 
                '403',
                'unauthorized',
                'forbidden',
                'parameter',
                'invalid parameter',
                'missing required',
                'missing 1 required positional argument',
                'nonetype',  # API returning None due to bad parameters
                'keys',      # 'NoneType' object has no attribute 'keys'
                'list index out of range'  # Empty response causing list access errors
            ]
            
            if any(indicator in error_str for indicator in permanent_error_indicators):
                logger.error(f"Permanent error detected, not retrying: {str(e)}")
                return "PERMANENT_ERROR"
            
            # Retry for temporary errors (network, rate limiting, server issues)
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"Temporary error, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All API call attempts failed: {str(e)}")
                return None
    
    return None

def process_single_endpoint_comprehensive(endpoint_name, node_id, rate_limit, logger):
    """Process a single NBA endpoint comprehensively - collect all missing data"""
    
    logger.info(f"Starting COMPREHENSIVE processing of endpoint: {endpoint_name}")
    start_time = time.time()
    
    # Tracking variables
    total_processed = 0
    total_failed = 0
    total_skipped = 0
    
    try:
        # Get endpoint configuration
        endpoint_config = get_endpoint_by_name(endpoint_name)
        if not endpoint_config:
            raise Exception(f"Endpoint '{endpoint_name}' not found in configuration")
        
        logger.info(f"Found endpoint config: {endpoint_config}")
        
        # Initialize connection manager
        logger.info("Initializing database connection...")
        conn_manager = RDSConnectionManager()
        
        if not conn_manager.ensure_connection():
            raise Exception("Failed to establish database connection")
        
        logger.info("Database connection established successfully")
        
        # Get endpoint class
        endpoint_class = getattr(nbaapi, endpoint_name)
        logger.info(f"Got endpoint class: {endpoint_class}")
        
        # Find all missing IDs that need to be processed
        missing_ids_by_param = find_all_missing_ids(endpoint_config, conn_manager, logger)
        
        if not missing_ids_by_param:
            logger.info("No missing data found - all data is up to date!")
            return {"status": "complete", "processed": 0, "failed": 0, "skipped": 0}
        
        # Log what we found
        for param_key, missing_ids in missing_ids_by_param.items():
            logger.info(f"Parameter {param_key}: {len(missing_ids)} missing IDs to process")        # Process each missing ID
        failed_ids_table = f"failed_api_calls"
        
        # Determine which parameter has IDs to iterate through
        main_param_key = None
        main_ids = []
        
        # SPECIAL CASE: Handle player-season combinations for comprehensive collection
        if 'player_season_combinations' in missing_ids_by_param:
            main_param_key = 'player_season_combinations'
            main_ids = missing_ids_by_param['player_season_combinations']
            logger.info(f"COMPREHENSIVE PLAYER-SEASON PROCESSING MODE")
            logger.info(f"Processing {len(main_ids)} player-season combinations")
            logger.info(f"This will collect historical data for all players across all seasons")
        else:
            # Standard processing - find first parameter with IDs
            for param_key, ids in missing_ids_by_param.items():
                if ids:  # Find the first parameter with actual IDs
                    main_param_key = param_key
                    main_ids = ids
                    break
        
        if not main_param_key:
            logger.warning("No missing IDs found to process")
            return {"status": "complete", "processed": 0, "failed": 0, "skipped": 0}
        
        logger.info(f"Processing {len(main_ids)} missing IDs for parameter: {main_param_key}")
        
        # Log processing strategy
        if 'game' in main_param_key:
            logger.info(f"🕒 PROCESSING STRATEGY: Latest games first, working backwards through time")
            logger.info(f"📅 This prioritizes recent games for faster data availability")
        elif main_param_key == 'player_season_combinations':
            logger.info(f"PROCESSING STRATEGY: Comprehensive player-season data collection")
            logger.info(f"DATA SCOPE: This will build complete historical player dashboard datasets")
        
        # Process each missing ID (or player-season combination)
        for i, missing_id in enumerate(main_ids):
            if main_param_key == 'player_season_combinations':
                # Handle player-season combinations specially
                player_id, season = missing_id  # Unpack the tuple
                logger.info(f"\\n--- Processing Player-Season {i+1}/{len(main_ids)}: Player {player_id}, Season {season} ---")
                
                # Build parameters for this player-season combination
                current_params = {
                    'player_id': player_id,
                    'season': season
                }
                
                # Add any additional static parameters from config
                for param_key_static, param_source_static in endpoint_config.get('parameters', {}).items():
                    if param_key_static not in ['player_id', 'season']:
                        # Handle static values that aren't player_id or season
                        if isinstance(param_source_static, str) and param_source_static not in ['from_masterplayers_all_seasons']:
                            current_params[param_key_static] = param_source_static
                        elif isinstance(param_source_static, (int, float, bool)):
                            current_params[param_key_static] = param_source_static
                
            else:
                # Standard single-parameter processing
                logger.info(f"\\n--- Processing ID {i+1}/{len(main_ids)}: {missing_id} ---")
                
                # Build parameters for this specific ID
                current_params = {}
                
                # Add the main ID we're iterating through
                current_params[main_param_key] = missing_id
                
                # Add any static parameters from config
                for param_key_static, param_source_static in endpoint_config.get('parameters', {}).items():
                    if param_key_static != main_param_key:  # Don't override our main parameter
                        # Handle direct string values
                        if isinstance(param_source_static, str):
                            if param_source_static in ['from_current_season', 'from_recent_season']:
                                current_params[param_key_static] = get_current_season()
                            elif param_source_static not in ['from_mastergames', 'from_masterplayers', 'from_masterteams']:
                                # It's a static value
                                current_params[param_key_static] = param_source_static
                    elif isinstance(param_source_static, (int, float, bool)):
                        # Handle numeric and boolean static values (like last_n_games: 30)
                        current_params[param_key_static] = param_source_static
                    else:
                        # Handle object format
                        try:
                            source_type = param_source_static.get('source', 'static')
                            if source_type == 'static':
                                current_params[param_key_static] = param_source_static.get('value')
                            elif source_type in ['from_current_season', 'from_recent_season']:
                                current_params[param_key_static] = get_current_season()
                        except AttributeError:
                            # If it's not a dict-like object, treat as static value
                            current_params[param_key_static] = param_source_static
            
            logger.info(f"Parameters for this call: {current_params}")
            
            # Validate parameters before making API call
            is_valid, validation_error = validate_api_parameters(endpoint_name, current_params, logger)
            if not is_valid:
                # Create appropriate validation error identifier
                if main_param_key == 'player_season_combinations':
                    validation_identifier = f"Player {player_id}, Season {season}"
                    record_key_value = f"{player_id}_{season}"
                else:
                    validation_identifier = f"{main_param_key}={missing_id}"
                    record_key_value = missing_id
                
                logger.error(f"Parameter validation failed for {validation_identifier}: {validation_error}")
                record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                main_param_key, record_key_value, f"Parameter validation failed: {validation_error}", logger)
                total_failed += 1
                continue
            
            # Make API call for this specific ID
            try:
                dataframes = make_api_call(endpoint_class, current_params, rate_limit, logger)
                
                if dataframes == "PERMANENT_ERROR":
                    # Create appropriate permanent error identifier
                    if main_param_key == 'player_season_combinations':
                        perm_error_identifier = f"Player {player_id}, Season {season}"
                        record_key_value = f"{player_id}_{season}"
                    else:
                        perm_error_identifier = f"{main_param_key}={missing_id}"
                        record_key_value = missing_id
                    
                    logger.error(f"Permanent API error for {perm_error_identifier} - recording as failed")
                    record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                    main_param_key, record_key_value, "Permanent API parameter error", logger)
                    total_failed += 1
                    continue
                
                if dataframes is None:
                    # Create appropriate no data identifier
                    if main_param_key == 'player_season_combinations':
                        no_data_identifier = f"Player {player_id}, Season {season}"
                        record_key_value = f"{player_id}_{season}"
                    else:
                        no_data_identifier = f"{main_param_key}={missing_id}"
                        record_key_value = missing_id
                    
                    logger.warning(f"No data returned for {no_data_identifier}")
                    record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                    main_param_key, record_key_value, "No data returned", logger)
                    total_failed += 1
                    continue
                
                if not isinstance(dataframes, list) or len(dataframes) == 0:
                    # Create appropriate empty dataframes identifier
                    if main_param_key == 'player_season_combinations':
                        empty_identifier = f"Player {player_id}, Season {season}"
                        record_key_value = f"{player_id}_{season}"
                    else:
                        empty_identifier = f"{main_param_key}={missing_id}"
                        record_key_value = missing_id
                    
                    logger.warning(f"Empty or invalid dataframes for {empty_identifier}")
                    record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                    main_param_key, record_key_value, "Empty dataframes returned", logger)
                    total_failed += 1
                    continue
                
                # Process and store the dataframes
                success_count = 0
                error_count = 0
                
                # Use advanced dataframe name matching instead of unreliable dictionary order
                try:
                    # Create endpoint instance to get dataframe metadata
                    temp_endpoint_instance = endpoint_class(**current_params)
                    
                    # Use our robust matching function to get correct names
                    dataframe_names = match_dataframes_to_names(dataframes, temp_endpoint_instance, logger)
                    logger.info(f"Matched dataframe names: {dataframe_names}")
                    
                except Exception as e:
                    logger.warning(f"Could not match dataframe names, using fallback: {e}")
                    # Fallback to simple index-based naming
                    dataframe_names = [f"dataframe_{i}" for i in range(len(dataframes))]
                
                # SPECIAL HANDLING: Player Dashboard Enhancement
                if is_player_dashboard_endpoint(endpoint_name):
                    logger.info(f"PLAYER DASHBOARD: Player Dashboard endpoint detected - adding player context")
                    
                    # Extract player_id and season from current_params
                    player_id = current_params.get('player_id')
                    season = current_params.get('season', 'unknown')
                    
                    if player_id:
                        # Enhance dataframes with player_id and season columns
                        dataframes = enhance_player_dashboard_dataframes(
                            dataframes=dataframes,
                            player_id=player_id,
                            season=season,
                            endpoint_name=endpoint_name,
                            logger=logger
                        )
                        logger.info(f"SUCCESS: Enhanced {len(dataframes)} dataframes with player context")
                    else:
                        logger.warning("WARNING: Player dashboard endpoint but no player_id found in parameters!")
                
                for df_index, df in enumerate(dataframes):
                    try:
                        # Check if dataframe is valid
                        if df is None or (hasattr(df, 'empty') and df.empty):
                            logger.warning(f"  Dataframe {df_index} is None or empty, skipping")
                            continue
                        
                        # Use matched dataframe name (should always be available now)
                        if df_index < len(dataframe_names):
                            df_name = dataframe_names[df_index]
                            table_name = f"nba_{endpoint_name.lower()}_{df_name}"
                            logger.info(f"  Processing dataframe {df_index} ({df_name}) -> {table_name}")
                        else:
                            # Extra safety fallback (shouldn't happen with our new matching)
                            table_name = f"nba_{endpoint_name.lower()}_dataframe_{df_index}"
                            logger.warning(f"  Unexpected: dataframe {df_index} has no matched name, using fallback -> {table_name}")
                        
                        logger.info(f"    Shape: {getattr(df, 'shape', 'unknown')}")
                        
                        # VALIDATION: For player dashboard endpoints, validate enhanced data
                        if is_player_dashboard_endpoint(endpoint_name):
                            player_id = current_params.get('player_id')
                            season = current_params.get('season', 'unknown')
                            
                            if not validate_player_dashboard_data(df, player_id, season, logger):
                                logger.error(f"Player dashboard data validation failed for dataframe {df_index}")
                                error_count += 1
                                continue
                            
                            logger.info(f"SUCCESS: Player dashboard data validation passed")
                        
                        # Clean dataframe (handles reserved keywords)
                        cleaned_df = conn_manager.clean_column_names(df.copy())
                        
                        # Check if table exists, create if not
                        table_exists = conn_manager.check_table_exists(table_name)
                        if not table_exists:
                            logger.info(f"Creating table: {table_name}")
                            conn_manager.create_table(table_name, cleaned_df)
                        
                        # Insert data
                        logger.info(f"Inserting {len(cleaned_df)} rows into {table_name}")
                        conn_manager.insert_dataframe_to_rds(cleaned_df, table_name)
                        logger.info(f"Successfully inserted data into {table_name}")
                        success_count += 1
                        
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed to process dataframe {df_index}: {str(e)}")
                        continue
                
                if success_count > 0:
                    total_processed += 1
                    
                    # Create appropriate success identifier
                    if main_param_key == 'player_season_combinations':
                        success_identifier = f"Player {player_id}, Season {season}"
                    else:
                        success_identifier = f"{main_param_key}={missing_id}"
                    
                    logger.info(f"Successfully processed {success_identifier} ({success_count} dataframes)")
                else:
                    total_failed += 1
                    
                    # Create appropriate failure identifier  
                    if main_param_key == 'player_season_combinations':
                        failure_identifier = f"Player {player_id}, Season {season}"
                        record_key_value = f"{player_id}_{season}"  # Use combined key for database
                    else:
                        failure_identifier = f"{main_param_key}={missing_id}"
                        record_key_value = missing_id
                    
                    record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                    main_param_key, record_key_value, "All dataframes failed to process", logger)
                    logger.error(f"All dataframes failed for {failure_identifier}")
                
            except Exception as e:
                total_failed += 1
                error_msg = str(e)
                
                # Create appropriate error identifier
                if main_param_key == 'player_season_combinations':
                    error_identifier = f"Player {player_id}, Season {season}"
                    record_key_value = f"{player_id}_{season}"  # Use combined key for database
                else:
                    error_identifier = f"{main_param_key}={missing_id}"
                    record_key_value = missing_id
                
                logger.error(f"API call failed for {error_identifier}: {error_msg}")
                record_failed_id(conn_manager, failed_ids_table, f"nba_{endpoint_name.lower()}", 
                                main_param_key, record_key_value, error_msg, logger)
                continue
            
            # Rate limiting between API calls
            if rate_limit > 0:
                logger.debug(f"Rate limiting: waiting {rate_limit} seconds...")
                time.sleep(rate_limit)
        
        # Close connection
        conn_manager.close_connection()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Final summary
        logger.info(f"COMPREHENSIVE PROCESSING COMPLETE")
        logger.info(f"Endpoint: {endpoint_name}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"IDs processed successfully: {total_processed}")
        logger.info(f"IDs failed: {total_failed}")
        logger.info(f"Total IDs attempted: {len(main_ids)}")
        
        success_rate = (total_processed / len(main_ids) * 100) if main_ids else 0
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        return {
            "status": "complete",
            "processed": total_processed,
            "failed": total_failed,
            "total_attempted": len(main_ids),
            "success_rate": success_rate
        }
        
    except Exception as e:
        logger.error(f"Critical error in comprehensive processing: {str(e)}")
        logger.exception("Full error traceback:")
        return {"status": "error", "error": str(e)}

def find_all_missing_ids(endpoint_config, conn_manager, logger):
    """
    Find all missing IDs for all parameters that need to be resolved from master tables
    
    Returns:
        dict: {parameter_name: [list_of_missing_ids]}
    """
    missing_ids_by_param = {}
    failed_ids_table = "failed_api_calls"
    
    for param_key, param_source in endpoint_config.get('parameters', {}).items():
        # Handle direct string values (e.g., 'game_id': 'from_mastergames')
        if isinstance(param_source, str):
            source_value = param_source
        elif isinstance(param_source, (int, float, bool)):
            # Skip numeric/boolean static parameters - they don't need missing ID resolution
            logger.debug(f"Skipping static parameter {param_key}={param_source}")
            continue
        else:
            # Handle object format (e.g., 'game_id': {'source': 'from_mastergames'})
            try:
                source_value = param_source.get('source', 'static')
                if source_value == 'static':
                    # Skip static parameters
                    logger.debug(f"Skipping static parameter {param_key}")
                    continue
            except AttributeError:
                # If it's not a dict-like object, skip it
                logger.debug(f"Skipping non-dict parameter {param_key}={param_source}")
                continue
        
        if source_value == 'from_mastergames':
            # Find missing game IDs across all leagues
            all_missing = []
            
            league_tables = {
                'nba': 'nba_games',
                'gleague': 'gleague_games', 
                'wnba': 'wnba_games'
            }
            
            for league, table_name in league_tables.items():
                endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
                missing_ids = find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                             'gameid', failed_ids_table, logger)
                all_missing.extend(missing_ids)
            
            missing_ids_by_param[param_key] = all_missing  # Process ALL missing IDs
            
        elif source_value == 'from_masterplayers':
            # Find missing player IDs across all leagues
            all_missing = []
            
            league_tables = {
                'nba': 'nba_players',
                'gleague': 'gleague_players', 
                'wnba': 'wnba_players'
            }
            
            for league, table_name in league_tables.items():
                endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
                missing_ids = find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                             'playerid', failed_ids_table, logger)
                all_missing.extend(missing_ids)
                
            missing_ids_by_param[param_key] = all_missing  # Process ALL missing IDs
            
        elif source_value == 'from_masterplayers_all_seasons':
            # Find ALL player-season combinations that need processing
            # This is for comprehensive historical data collection
            logger.info(f"Finding missing player-season combinations for comprehensive collection...")
            
            all_combinations = []
            
            league_tables = {
                'nba': 'nba_players',
                'gleague': 'gleague_players', 
                'wnba': 'wnba_players'
            }
            
            for league, table_name in league_tables.items():
                try:
                    logger.info(f"Processing {table_name} for all player-season combinations...")
                    
                    with conn_manager.get_cursor() as cursor:
                        # Get ALL unique player-season combinations from master table
                        cursor.execute(f"""
                            SELECT DISTINCT playerid, season 
                            FROM {table_name} 
                            WHERE playerid IS NOT NULL 
                            AND season IS NOT NULL
                            ORDER BY season DESC, playerid ASC
                        """)
                        
                        combinations = cursor.fetchall()
                        
                        if combinations:
                            logger.info(f"Found {len(combinations)} player-season combinations in {table_name}")
                            
                            # Check which combinations are missing from endpoint tables
                            endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
                            
                            # Get existing combinations from endpoint tables
                            cursor.execute("""
                                SELECT table_name 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name LIKE %s
                            """, (f"{endpoint_prefix}%",))
                            
                            endpoint_tables = [row[0] for row in cursor.fetchall()]
                            existing_combinations = set()
                            
                            for table in endpoint_tables:
                                try:
                                    cursor.execute(f"""
                                        SELECT DISTINCT player_id, season 
                                        FROM {table} 
                                        WHERE player_id IS NOT NULL 
                                        AND season IS NOT NULL
                                    """)
                                    existing = cursor.fetchall()
                                    existing_combinations.update(existing)
                                except Exception as e:
                                    logger.debug(f"Could not check existing data in {table}: {e}")
                            
                            # Find missing combinations
                            all_available = set(combinations)
                            missing_combinations = all_available - existing_combinations
                            
                            logger.info(f"Available: {len(all_available)}, Existing: {len(existing_combinations)}, Missing: {len(missing_combinations)}")
                            
                            # Convert to list format for processing
                            missing_list = list(missing_combinations)
                            all_combinations.extend(missing_list)
                            
                except Exception as e:
                    logger.error(f"Error processing {table_name} for player-season combinations: {e}")
                    continue
            
            # Store the combinations in a special format
            # The processor will iterate through these (player_id, season) tuples
            if all_combinations:
                logger.info(f"Total missing player-season combinations to process: {len(all_combinations)}")
                logger.info(f"Sample combinations: {all_combinations[:5]}...")
                missing_ids_by_param['player_season_combinations'] = all_combinations
            else:
                logger.warning("No missing player-season combinations found")
                missing_ids_by_param[param_key] = []
            
        elif source_value == 'from_masterteams':
            # Find missing team IDs across all leagues
            all_missing = []
            
            league_tables = {
                'nba': 'nba_teams',
                'gleague': 'gleague_teams', 
                'wnba': 'wnba_teams'
            }
            
            for league, table_name in league_tables.items():
                endpoint_prefix = f"nba_{endpoint_config['endpoint'].lower()}"
                missing_ids = find_missing_ids(conn_manager, table_name, endpoint_prefix, 
                                             'teamid', failed_ids_table, logger)
                all_missing.extend(missing_ids)
                
            missing_ids_by_param[param_key] = all_missing  # Process ALL missing IDs
    
    return missing_ids_by_param

def get_current_season():
    """Get current NBA season string"""
    now = datetime.now()
    if now.month >= 10:  # Season starts in October
        return f"{now.year}-{str(now.year + 1)[2:]}"
    else:
        return f"{now.year - 1}-{str(now.year)[2:]}"

def validate_api_parameters(endpoint_name, params, logger):
    """
    Validate API parameters before making calls to avoid permanent failures
    
    Returns:
        tuple: (is_valid: bool, error_message: str)
    """
    try:
        # Basic validation - check for required parameters
        if not params:
            return False, "No parameters provided"
        
        # Check for common parameter issues
        for key, value in params.items():
            if value is None:
                return False, f"Parameter {key} is None"
            
            # Validate game IDs (should be strings of digits)
            if 'game_id' in key and isinstance(value, str):
                if not value.isdigit() or len(value) < 8:
                    return False, f"Invalid game_id format: {value}"
            
            # Validate player IDs (should be integers or digit strings)
            if 'player_id' in key:
                try:
                    player_id = int(value)
                    if player_id <= 0:
                        return False, f"Invalid player_id: {value}"
                except (ValueError, TypeError):
                    return False, f"player_id must be numeric: {value}"
            
            # Validate team IDs (should be integers)
            if 'team_id' in key:
                try:
                    team_id = int(value)
                    if team_id <= 0:
                        return False, f"Invalid team_id: {value}"
                except (ValueError, TypeError):
                    return False, f"team_id must be numeric: {value}"
        
        logger.debug(f"Parameters validated successfully for {endpoint_name}")
        return True, ""
        
    except Exception as e:
        logger.warning(f"Parameter validation error: {e}")
        return False, f"Validation error: {str(e)}"
        
        # Process dataframes
        success_count = 0
        error_count = 0
        
        logger.info(f"Processing {len(dataframes)} dataframes from {endpoint_name}")
        
        # Try to get dataframe names from the endpoint if available
        try:
            endpoint_instance = endpoint_class(**resolved_params)
            if hasattr(endpoint_instance, 'data_sets'):
                df_names = [ds.lower() for ds in endpoint_instance.data_sets]
                logger.info(f"Found dataframe names: {df_names}")
            else:
                df_names = None
        except:
            df_names = None
        
        for i, df in enumerate(dataframes):
            if df is None or df.empty:
                logger.warning(f"Dataframe {i} is empty, skipping")
                continue
            
            logger.info(f"Processing dataframe {i+1}/{len(dataframes)}")
            logger.info(f"  Shape: {df.shape}")
            logger.info(f"  Columns: {list(df.columns)[:5]}...")  # Show first 5 columns
            
            try:
                # Generate table name - use actual name if available, otherwise use index
                if df_names and i < len(df_names):
                    df_name = df_names[i]
                else:
                    df_name = f"dataframe_{i}"
                
                table_name = f"nba_{endpoint_name.lower()}_{df_name}"
                logger.info(f"  Table name: {table_name}")
                
                # Clean dataframe (handles reserved keywords like 'to' -> 'turnovers')
                cleaned_df = conn_manager.clean_column_names(df.copy())
                logger.info(f"  Cleaned columns: {list(cleaned_df.columns)[:5]}...")
                
                # Check if table exists, create if not
                table_exists = conn_manager.check_table_exists(table_name)
                if not table_exists:
                    logger.info(f"Creating table: {table_name}")
                    conn_manager.create_table(table_name, cleaned_df)
                else:
                    logger.info(f"Table exists: {table_name}")
                
                # Insert data
                logger.info(f"Inserting {len(cleaned_df)} rows into {table_name}")
                conn_manager.insert_dataframe_to_rds(cleaned_df, table_name)
                logger.info(f"SUCCESS: Successfully inserted data into {table_name}")
                success_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"ERROR: Failed to process dataframe {df_index}: {str(e)}")
                continue

def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(description='NBA Endpoint Processor for SLURM')
    parser.add_argument('--endpoint', required=True, help='Endpoint name to process')
    parser.add_argument('--node-id', required=True, help='Unique node identifier')
    parser.add_argument('--rate-limit', type=float, default=0.5, help='Rate limit in seconds')
    parser.add_argument('--db-config', required=True, help='Database configuration JSON file')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.node_id, args.log_level)
    
    try:
        # Load database configuration
        logger.info(f"Loading database config from: {args.db_config}")
        db_config = load_database_config(args.db_config)
        logger.info("Database configuration loaded successfully")
        
        # Process the endpoint comprehensively
        result = process_single_endpoint_comprehensive(
            endpoint_name=args.endpoint,
            node_id=args.node_id,
            rate_limit=args.rate_limit,
            logger=logger
        )
        
        if result["status"] == "complete":
            if result["processed"] > 0:
                logger.info("SUCCESS: Comprehensive endpoint processing completed successfully!")
                logger.info(f"Final stats: {result['processed']} processed, {result['failed']} failed")
            else:
                logger.info("SUCCESS: No new data to process - everything is up to date!")
            sys.exit(0)
        else:
            logger.error("ERROR: Comprehensive endpoint processing failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == '__main__':
    main()
