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
from endpoints.collectors.rds_connection_manager import RDSConnectionManager
from endpoints.collectors import allintwo
from endpoints.config.nba_endpoints_config import get_endpoint_by_name

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

def resolve_parameters(endpoint_config, conn_manager, logger):
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
                        query = f"SELECT DISTINCT gameid FROM {table_name} WHERE seasonid LIKE '%2023%' OR seasonid LIKE '%2024%' ORDER BY gamedate DESC LIMIT 20"
                        
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
                        query = f"SELECT DISTINCT playerid FROM {table_name} WHERE season LIKE '%2024%' ORDER BY playername LIMIT 20"
                        
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
                        query = f"SELECT DISTINCT teamid FROM {table_name} ORDER BY teamname LIMIT 20"
                        
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
    """Make NBA API call with retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Making API call (attempt {attempt + 1}/{max_retries})")
            endpoint_instance = endpoint_class(**params)
            dataframes = endpoint_instance.get_data_frames()
            
            if dataframes is None:
                logger.warning("API returned None")
                return None
                
            logger.info(f"API call successful - got {len(dataframes)} dataframes")
            return dataframes
            
        except Exception as e:
            error_str = str(e)
            logger.warning(f"API call failed (attempt {attempt + 1}): {error_str}")
            
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All API call attempts failed: {error_str}")
                return None
    
    return None

def process_single_endpoint(endpoint_name, node_id, rate_limit, logger):
    """Process a single NBA endpoint"""
    
    logger.info(f"Starting processing of endpoint: {endpoint_name}")
    start_time = time.time()
    
    try:
        # Get endpoint configuration
        endpoint_config = get_endpoint_by_name(endpoint_name)
        if not endpoint_config:
            raise Exception(f"Endpoint '{endpoint_name}' not found in configuration")
        
        logger.info(f"Found endpoint config: {endpoint_config}")
        
        # Initialize connection manager
        logger.info("Initializing database connection...")
        conn_manager = RDSConnectionManager()
        
        # Test connection
        if not conn_manager.ensure_connection():
            raise Exception("Failed to establish database connection")
        
        logger.info("Database connection established successfully")
        
        # Get endpoint class
        endpoint_class = getattr(nbaapi, endpoint_name)
        logger.info(f"Got endpoint class: {endpoint_class}")
        
        # Resolve parameters
        resolved_params = resolve_parameters(endpoint_config, conn_manager, logger)
        if resolved_params is None:
            raise Exception("Failed to resolve endpoint parameters")
        
        logger.info(f"Resolved parameters: {resolved_params}")
        
        # Make API call
        dataframes = make_api_call(endpoint_class, resolved_params, rate_limit, logger)
        if dataframes is None:
            raise Exception("Failed to get data from NBA API")
        
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
                cleaned_df = allintwo.clean_column_names(df.copy())
                logger.info(f"  Cleaned columns: {list(cleaned_df.columns)[:5]}...")
                
                # Check if table exists, create if not
                table_exists = allintwo.check_table_exists(conn_manager.connection, table_name)
                if not table_exists:
                    logger.info(f"Creating table: {table_name}")
                    allintwo.create_table(conn_manager.connection, table_name, cleaned_df)
                else:
                    logger.info(f"Table exists: {table_name}")
                
                # Insert data
                logger.info(f"Inserting {len(cleaned_df)} rows into {table_name}")
                allintwo.insert_dataframe_to_rds(conn_manager.connection, cleaned_df, table_name)
                logger.info(f"✅ Successfully inserted data into {table_name}")
                success_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Failed to process dataframe {i}: {str(e)}")
                logger.exception("Full error traceback:")
                continue
        
        # Apply rate limiting
        if rate_limit > 0:
            time.sleep(rate_limit)
        
        # Close connection
        conn_manager.close_connection()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Log results
        total_operations = success_count + error_count
        success_rate = (success_count / total_operations * 100) if total_operations > 0 else 0
        
        logger.info(f"=== PROCESSING COMPLETE ===")
        logger.info(f"Endpoint: {endpoint_name}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Dataframes processed: {success_count}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        return success_count > 0, {
            'total_processed': success_count,
            'total_errors': error_count,
            'success_rate': success_rate,
            'duration': duration
        }
        
    except Exception as e:
        logger.error(f"Failed to process endpoint {endpoint_name}: {e}")
        logger.exception("Full traceback:")
        return False, {'error': str(e)}

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
        
        # Process the endpoint
        success, summary = process_single_endpoint(
            endpoint_name=args.endpoint,
            node_id=args.node_id,
            rate_limit=args.rate_limit,
            logger=logger
        )
        
        if success:
            logger.info("🎉 Endpoint processing completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Endpoint processing failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)

if __name__ == '__main__':
    main()
