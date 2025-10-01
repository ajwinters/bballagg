#!/usr/bin/env python3
"""
NBA API Data Collection Engine - New Configuration-Driven System

This system processes NBA API endpoints based on endpoint_config.json with the following features:
- Master tables processed first (endpoints with 'master' field)
- Processes high priority, latest version endpoints
- League-specific table naming (e.g., nba_{endpoint}_{dataframe})
- Dynamic table creation using expected_data matching
- Error tracking for failed API calls
- Test mode for development
- Season-aware processing
- SLURM distribution ready

Author: NBA Data Pipeline
Date: September 2025
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

# Add project paths
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

import pandas as pd
import nba_api.stats.endpoints as nbaapi
from src.rds_connection_manager import RDSConnectionManager


class NBADataProcessor:
    """
    Main NBA Data Processor - Configuration-driven endpoint processing
    """
    
    def __init__(self, league: str = 'NBA', test_mode: bool = False, 
                 max_items_per_endpoint: int = None, log_level: str = 'INFO'):
        """
        Initialize the NBA Data Processor
        
        Args:
            league: League to process (NBA, WNBA, G-League)
            test_mode: Whether to run in test mode with limited data
            max_items_per_endpoint: Maximum items to process per endpoint (for test mode)
            log_level: Logging level
        """
        self.league = league.upper()
        self.test_mode = test_mode
        self.max_items_per_endpoint = max_items_per_endpoint or (10 if test_mode else None)
        
        # Setup logging
        self.logger = self._setup_logging(log_level)
        
        # Load configurations
        self.endpoint_config = self._load_endpoint_config()
        self.league_config = self._load_league_config()
        self.database_config = self._load_database_config()
        self.parameter_mappings = self._load_parameter_mappings()
        
        # Initialize database connection
        self.db_manager = RDSConnectionManager(self.database_config)
        
        # Get current season info
        self.current_season = self._get_current_season()
        self.is_current_season = True  # Will be set per season iteration
        
        self.logger.info(f"=== NBA Data Processor Initialized ===")
        self.logger.info(f"League: {self.league}")
        self.logger.info(f"Test Mode: {self.test_mode}")
        self.logger.info(f"Current Season: {self.current_season}")
        if self.max_items_per_endpoint:
            self.logger.info(f"Max items per endpoint: {self.max_items_per_endpoint}")
    
    def _setup_logging(self, log_level: str) -> logging.Logger:
        """Setup logging configuration"""
        # Create logs directory
        logs_dir = os.path.join(project_root, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Create log filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = os.path.join(logs_dir, f'nba_processor_{self.league.lower()}_{timestamp}.log')
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        logger = logging.getLogger(__name__)
        logger.info(f"Log file: {log_filename}")
        return logger
    
    def _load_endpoint_config(self) -> dict:
        """Load endpoint configuration from JSON"""
        config_path = os.path.join(project_root, 'config', 'endpoint_config.json')
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Loaded {len(config['endpoints'])} endpoint configurations")
            return config
        except Exception as e:
            self.logger.error(f"Failed to load endpoint config: {e}")
            raise
    
    def _load_league_config(self) -> dict:
        """Load league configuration"""
        config_path = os.path.join(project_root, 'config', 'leagues_config.json')
        try:
            with open(config_path, 'r') as f:
                leagues = json.load(f)
            
            # Find our league
            league_config = None
            for league in leagues:
                if league['name'].upper() == self.league:
                    league_config = league
                    break
            
            if not league_config:
                raise ValueError(f"League {self.league} not found in configuration")
                
            self.logger.info(f"Loaded league config for {league_config['full_name']}")
            return league_config
            
        except Exception as e:
            self.logger.error(f"Failed to load league config: {e}")
            raise
    
    def _load_database_config(self) -> dict:
        """Load database configuration"""
        config_path = os.path.join(project_root, 'config', 'database_config.json')
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return {
                'host': config['host'],
                'database': config['name'],
                'user': config['user'],
                'password': config['password'],
                'port': int(config['port']),
                'sslmode': config.get('ssl_mode', 'require'),
                'connect_timeout': 60
            }
        except Exception as e:
            self.logger.error(f"Failed to load database config: {e}")
            raise
    
    def _load_parameter_mappings(self) -> dict:
        """Load parameter mappings for consistent column naming"""
        config_path = os.path.join(project_root, 'config', 'parameter_mappings.json')
        try:
            with open(config_path, 'r') as f:
                mappings_config = json.load(f)
            self.logger.info("Loaded parameter mappings for consistent column naming")
            return mappings_config
        except FileNotFoundError:
            self.logger.warning("Parameter mappings file not found - using default column names")
            return {"mappings": {}, "variant_groups": {}}
        except Exception as e:
            self.logger.error(f"Failed to load parameter mappings: {e}")
            return {"mappings": {}, "variant_groups": {}}
    
    def _get_current_season(self) -> str:
        """
        Determine current NBA season based on date
        Returns season in the format expected by the league
        """
        current_date = datetime.now()
        current_year = current_date.year
        
        if self.league_config['season_format'] == 'two_year':
            # NBA/G-League: 2024-25 format
            if current_date.month >= 10:  # Season starts in October
                season = f"{current_year}-{str(current_year + 1)[-2:]}"
            else:
                season = f"{current_year - 1}-{str(current_year)[-2:]}"
        else:
            # WNBA: 2024 format
            if current_date.month >= 5:  # WNBA season roughly May-Oct
                season = str(current_year)
            else:
                season = str(current_year - 1)
        
        return season
    
    def get_master_endpoints(self) -> List[Tuple[str, dict]]:
        """
        Get endpoints that have a 'master' field - these must be processed first
        Returns list of (endpoint_name, config) tuples
        """
        master_endpoints = []
        
        for endpoint_name, config in self.endpoint_config['endpoints'].items():
            if 'master' in config:
                master_endpoints.append((endpoint_name, config))
        
        self.logger.info(f"Found {len(master_endpoints)} master endpoints")
        for endpoint_name, config in master_endpoints:
            self.logger.info(f"  - {endpoint_name}: master for {config['master']}")
        
        return master_endpoints
    
    def get_processable_endpoints(self) -> List[Tuple[str, dict]]:
        """
        Get endpoints that should be processed based on priority and latest_version
        Excludes master endpoints (they're processed separately)
        Returns list of (endpoint_name, config) tuples
        """
        processable_endpoints = []
        
        for endpoint_name, config in self.endpoint_config['endpoints'].items():
            # Skip if it's a master endpoint
            if 'master' in config:
                continue
                
            # Check priority and latest_version criteria
            if (config.get('priority') == 'high' and 
                config.get('latest_version') == True):
                processable_endpoints.append((endpoint_name, config))
        
        self.logger.info(f"Found {len(processable_endpoints)} processable endpoints")
        return processable_endpoints
    
    def get_table_prefix(self) -> str:
        """Get table prefix for current league"""
        return self.league.lower()
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean column names for PostgreSQL compatibility with special handling
        Apply parameter mappings for consistent naming
        """
        # Special case mappings
        special_mappings = {
            'to': 'turnovers',
            'from': 'from_field',
            'order': 'order_field',
            'group': 'group_field',
            'select': 'select_field',
            'where': 'where_field',
            'having': 'having_field',
            'union': 'union_field',
            'user': 'user_field',
            'rank': None  # Remove rank columns entirely
        }
        
        # Preserve these column names exactly (system metadata columns)
        preserve_columns = {
            'failed_reason', 'data_collected_date'
        }
        
        cleaned_columns = []
        columns_to_drop = []
        
        for i, col in enumerate(df.columns):
            # Convert to lowercase for consistency
            col_lower = str(col).lower()
            
            # Preserve certain system columns exactly
            if col_lower in preserve_columns:
                cleaned = col_lower
            else:
                # Apply parameter mappings first (for standardized naming)
                parameter_mappings = self.parameter_mappings.get("mappings", {})
                if col_lower in parameter_mappings:
                    mapped_name = parameter_mappings[col_lower]
                    # Clean the mapped name but preserve underscores for IDs
                    if '_id' in mapped_name.lower():
                        cleaned = mapped_name.lower()
                    else:
                        cleaned = ''.join(c.lower() if c.isalnum() else '' for c in mapped_name)
                else:
                    # Preserve underscores for ID columns, remove other special chars
                    if '_id' in col_lower or col_lower.endswith('id'):
                        cleaned = col_lower
                    else:
                        # Convert to lowercase and remove special characters
                        cleaned = ''.join(c.lower() if c.isalnum() else '' for c in col_lower)
                
                # Handle special cases
                if cleaned in special_mappings:
                    if special_mappings[cleaned] is None:
                        # Mark for removal
                        columns_to_drop.append(i)
                        continue
                    else:
                        cleaned = special_mappings[cleaned]
            
            # Ensure no duplicates by adding suffix if needed
            original_cleaned = cleaned
            suffix = 1
            while cleaned in cleaned_columns:
                cleaned = f"{original_cleaned}_{suffix}"
                suffix += 1
            
            cleaned_columns.append(cleaned)
        
        # Drop columns marked for removal (like 'rank' columns)
        if columns_to_drop:
            df = df.drop(df.columns[columns_to_drop], axis=1)
            self.logger.debug(f"Dropped {len(columns_to_drop)} rank columns")
        
        # Apply cleaned column names
        df.columns = cleaned_columns
        return df
    
    def add_missing_id_columns(self, df: pd.DataFrame, required_params: List[str], 
                              param_values: dict) -> pd.DataFrame:
        """
        Add ID columns to the front of DataFrame if they're missing
        Uses parameter mappings for consistent column naming
        
        Args:
            df: DataFrame to modify
            required_params: List of required parameter names
            param_values: Dict of parameter values used in API call
        """
        for param in required_params:
            # Use parameter mappings for standardized column name
            mappings = self.parameter_mappings.get("mappings", {})
            standard_param = mappings.get(param, param)
            
            # Clean the standardized parameter name for column naming
            clean_param = ''.join(c.lower() if c.isalnum() else '' for c in standard_param)
            
            # Check if this parameter column already exists
            if clean_param not in [col.lower() for col in df.columns]:
                # Add the column to the front
                df.insert(0, clean_param, param_values.get(param))
                self.logger.debug(f"Added missing ID column: {clean_param} = {param_values.get(param)} (from API param: {param})")
        
        return df
    
    def add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata columns to DataFrame (only if not already present)
        """
        # Add date column for when data was collected (only if not present)
        if 'data_collected_date' not in df.columns:
            df['data_collected_date'] = datetime.now()
        
        # Add failed_reason column for unified error tracking (NULL for successful records)
        if 'failed_reason' not in df.columns:
            df['failed_reason'] = None
        
        return df

    def match_dataframes_to_expected_data(self, endpoint_name: str, dataframes: List[pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Match DataFrames returned from get_data_frames() to their expected names
        Uses the expected_data attribute from the endpoint to determine correct names
        
        Args:
            endpoint_name: Name of the endpoint
            dataframes: List of DataFrames returned from get_data_frames()
            
        Returns:
            Dictionary mapping expected names to DataFrames
        """
        try:
            # Get the endpoint class
            endpoint_class = getattr(nbaapi, endpoint_name)
            
            # Get expected data if available
            if hasattr(endpoint_class, 'expected_data'):
                expected_data = endpoint_class.expected_data
                self.logger.debug(f"Expected data for {endpoint_name}: {list(expected_data.keys())}")
            else:
                # Fallback to generic names if no expected_data
                self.logger.warning(f"No expected_data found for {endpoint_name}, using generic names")
                expected_data = {f"data_{i}": {} for i in range(len(dataframes))}
            
            matched_data = {}
            
            # Strategy 1: Try to match by number of columns (when unique)
            if len(dataframes) == len(expected_data):
                # Simple case: same number of dataframes and expected datasets
                for i, (expected_name, df) in enumerate(zip(expected_data.keys(), dataframes)):
                    if df is not None and not df.empty:
                        matched_data[expected_name] = df
                        self.logger.debug(f"Matched {expected_name}: {df.shape}")
            else:
                # Complex case: Try to match by column patterns or content
                self.logger.warning(f"DataFrame count mismatch for {endpoint_name}: "
                                  f"{len(dataframes)} DFs vs {len(expected_data)} expected")
                
                # Use generic names for now
                for i, df in enumerate(dataframes):
                    if df is not None and not df.empty:
                        name = f"dataset_{i}"
                        matched_data[name] = df
            
            self.logger.info(f"Matched {len(matched_data)} datasets for {endpoint_name}")
            return matched_data
            
        except Exception as e:
            self.logger.error(f"Error matching DataFrames for {endpoint_name}: {e}")
            # Fallback to generic naming
            matched_data = {}
            for i, df in enumerate(dataframes):
                if df is not None and not df.empty:
                    matched_data[f"dataset_{i}"] = df
            return matched_data
    
    def create_table_if_needed(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Create table if it doesn't exist, using DataFrame structure
        
        Args:
            table_name: Name of the table to create
            df: Sample DataFrame to base structure on (should already be processed)
            
        Returns:
            True if table exists or was created successfully
        """
        try:
            if self.db_manager.check_table_exists(table_name):
                self.logger.debug(f"Table {table_name} already exists")
                return True
            
            # Use the DataFrame as-is since it should already be processed
            self.db_manager.create_table(table_name, df)
            self.logger.info(f"Created table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def insert_dataframe_to_table(self, df: pd.DataFrame, table_name: str, 
                                 required_params: List[str], param_values: dict) -> bool:
        """
        Insert DataFrame to database table with proper preprocessing
        
        Args:
            df: DataFrame to insert
            table_name: Target table name
            required_params: Required parameters for the endpoint
            param_values: Values used in the API call
            
        Returns:
            True if insertion successful
        """
        try:
            if df.empty:
                self.logger.warning(f"Empty DataFrame for {table_name}, skipping insert")
                return False
            
            # Preprocess the DataFrame
            processed_df = df.copy()
            
            # Add missing ID columns if needed
            processed_df = self.add_missing_id_columns(processed_df, required_params, param_values)
            
            # Clean column names
            processed_df = self.clean_column_names(processed_df)
            
            # Add metadata columns
            processed_df = self.add_metadata_columns(processed_df)
            
            # Ensure table exists
            if not self.create_table_if_needed(table_name, processed_df):
                return False
            
            # Insert the data
            self.db_manager.insert_dataframe_to_rds(processed_df, table_name)
            self.logger.info(f"Inserted {len(processed_df)} rows into {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to insert data into {table_name}: {e}")
            return False
    
    def create_failed_records(self, endpoint_name: str, config: dict, param_values: dict, error_message: str):
        """
        Create failed records in the main data tables with NULL data but preserved ID columns
        
        Args:
            endpoint_name: Name of the endpoint that failed
            config: Endpoint configuration
            param_values: Parameter values used in the failed call
            error_message: Error message from the API call
        """
        try:
            # Get expected data structure from endpoint configuration
            expected_data = config.get('expected_data', {})
            
            if not expected_data:
                # If no expected data, create a single generic failed record
                expected_data = {'failed_data': {}}
            
            # Create failed records for each expected dataset
            for dataset_name, dataset_info in expected_data.items():
                table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}_{dataset_name.lower()}"
                
                # Create a DataFrame with ID columns from parameters and failed_reason
                failed_record = {}
                
                # Add ID columns from API call parameters
                required_params = config.get('required_params', [])
                for param in required_params:
                    # Use parameter mappings for standardized column name
                    mappings = self.parameter_mappings.get("mappings", {})
                    standard_param = mappings.get(param, param)
                    clean_param = ''.join(c.lower() if c.isalnum() else '' for c in standard_param)
                    failed_record[clean_param] = param_values.get(param)
                
                # Add metadata columns
                failed_record['data_collected_date'] = datetime.now()
                failed_record['failed_reason'] = str(error_message)[:500]  # Truncate long messages
                
                # Create DataFrame
                failed_df = pd.DataFrame([failed_record])
                
                # Clean column names
                failed_df = self.clean_column_names(failed_df)
                
                # Try to create/insert the failed record
                try:
                    # Check if table exists, if not we need to create with proper structure
                    if not self.db_manager.check_table_exists(table_name):
                        # Create table with just the failed record structure
                        # The table will expand when successful records are added later
                        self.db_manager.create_table(table_name, failed_df)
                        self.logger.info(f"Created table {table_name} with failed record structure")
                    
                    # Insert the failed record
                    self.db_manager.insert_dataframe_to_rds(failed_df, table_name)
                    self.logger.info(f"Recorded failed API call in {table_name}: {error_message[:100]}")
                    
                except Exception as insert_error:
                    # If insertion fails, it might be due to column mismatch with existing table
                    self.logger.warning(f"Could not insert failed record to {table_name}: {insert_error}")
                    # Continue with other datasets
                    continue
            
        except Exception as e:
            self.logger.error(f"Failed to create failed records for {endpoint_name}: {e}")

    def get_failed_parameter_combinations(self, endpoint_name: str) -> List[Dict[str, Any]]:
        """
        Get all failed parameter combinations for an endpoint to enable retry logic
        
        Args:
            endpoint_name: Name of the endpoint to check for failures
            
        Returns:
            List of dictionaries containing failed parameter combinations
        """
        failed_combinations = []
        
        try:
            # Find all tables that match this endpoint pattern
            table_pattern = f"{self.get_table_prefix()}_{endpoint_name.lower()}_%"
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE %s
                """, (table_pattern,))
                
                matching_tables = [row[0] for row in cursor.fetchall()]
                self.logger.debug(f"Found {len(matching_tables)} tables matching pattern {table_pattern}")
            
            # Check each matching table for failed records
            for table_name in matching_tables:
                try:
                    with self.db_manager.get_cursor() as cursor:
                        # Get all records with failed_reason (handle both possible column names)
                        cursor.execute(f"""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = '{table_name}' 
                            AND column_name IN ('failed_reason', 'failedreason')
                        """)
                        
                        failed_reason_col = cursor.fetchone()
                        if not failed_reason_col:
                            self.logger.debug(f"No failed_reason column in {table_name}")
                            continue
                        
                        failed_reason_col = failed_reason_col[0]
                        
                        cursor.execute(f"""
                            SELECT DISTINCT * FROM {table_name} 
                            WHERE {failed_reason_col} IS NOT NULL
                        """)
                        
                        failed_records = cursor.fetchall()
                        colnames = [desc[0] for desc in cursor.description]
                        
                        for record in failed_records:
                            record_dict = dict(zip(colnames, record))
                            
                            # Extract parameter values (exclude metadata columns)
                            param_combo = {}
                            for key, value in record_dict.items():
                                if key not in ['datacollecteddate', 'failedreason', 'failed_reason'] and value is not None:
                                    param_combo[key] = value
                            
                            if param_combo:  # Only add if we have parameters
                                param_combo['failed_reason'] = record_dict.get(failed_reason_col)
                                param_combo['table_name'] = table_name
                                failed_combinations.append(param_combo)
                
                except Exception as e:
                    self.logger.warning(f"Could not query failed records from {table_name}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error getting failed parameter combinations for {endpoint_name}: {e}")
        
        return failed_combinations
    
    def get_missing_ids_for_endpoint(self, endpoint_name: str, config: dict) -> List[Any]:
        """
        Get missing IDs for an endpoint by comparing master tables to endpoint tables
        
        Args:
            endpoint_name: Name of the endpoint
            config: Endpoint configuration
            
        Returns:
            List of missing IDs to process
        """
        try:
            # Check if this is a master endpoint
            is_master_endpoint = 'master' in config
            
            # For master endpoints, always process them to ensure base data exists
            if is_master_endpoint:
                if endpoint_name == 'CommonAllPlayers':
                    # Get current players (no season parameter needed)
                    return [{}]
                elif endpoint_name == 'LeagueGameFinder':
                    # Get ALL NBA seasons for comprehensive game history
                    seasons = []
                    # NBA seasons from 1996-97 (when current format started) to current
                    for year in range(1996, 2025):  # Will get 1996-97 through 2024-25
                        season_str = f"{year}-{str(year+1)[2:]}"  # e.g., "2023-24"
                        seasons.append({'season_nullable': season_str})
                    
                    # In test mode, limit to recent seasons
                    if self.test_mode:
                        seasons = seasons[-3:]  # Last 3 seasons only
                        self.logger.info(f"Test mode: Limited LeagueGameFinder to {len(seasons)} recent seasons")
                    else:
                        self.logger.info(f"Processing LeagueGameFinder for {len(seasons)} seasons (comprehensive)")
                    
                    return seasons
                elif endpoint_name == 'LeagueGameLog':
                    # Get ALL NBA seasons for comprehensive game log history
                    seasons = []
                    for year in range(1996, 2025):
                        season_str = f"{year}-{str(year+1)[2:]}"
                        seasons.append({'season': season_str})
                    
                    if self.test_mode:
                        seasons = seasons[-2:]  # Last 2 seasons only
                        self.logger.info(f"Test mode: Limited LeagueGameLog to {len(seasons)} recent seasons")
                    
                    return seasons
                else:
                    # Other master endpoints
                    return [{}]
            
            required_params = config.get('required_params', [])
            
            if not required_params:
                # No parameters needed, return empty list (endpoint will be called once)
                return [{}]
            
            missing_ids = []
            
            # Handle different parameter types - check for combinations first
            if 'game_id' in required_params:
                # Game-based endpoints
                master_table = f"{self.get_table_prefix()}_games"
                missing_ids = self._get_missing_game_ids(endpoint_name, master_table)
                
            elif 'player_id' in required_params and 'season' in required_params:
                # Player + Season combination endpoints (most comprehensive backfill)
                master_table = f"{self.get_table_prefix()}_players"
                missing_ids = self._get_missing_player_season_combinations(endpoint_name, master_table, required_params)
                
            elif 'player_id' in required_params:
                # Player-only endpoints
                master_table = f"{self.get_table_prefix()}_players"
                missing_ids = self._get_missing_player_ids(endpoint_name, master_table)
                
            elif 'team_id' in required_params and 'season' in required_params:
                # Team + Season combination endpoints
                missing_ids = self._get_missing_team_season_combinations(endpoint_name, required_params)
                
            elif 'team_id' in required_params:
                # Team-only endpoints
                missing_ids = self._get_missing_team_ids(endpoint_name, "")
                
            elif 'season' in required_params:
                # Season-based endpoints (like LeagueGameLog, PlayerGameLogs)
                missing_ids = self._get_missing_season_data(endpoint_name, required_params)
                
            else:
                # Fallback - log what parameters we're missing handlers for
                self.logger.warning(f"No specific handler for required params {required_params} in {endpoint_name}")
                missing_ids = []
            
            # Apply test mode limits
            if self.test_mode and missing_ids:
                original_count = len(missing_ids)
                missing_ids = missing_ids[:self.max_items_per_endpoint]
                self.logger.info(f"Test mode: Limited {endpoint_name} from {original_count} to {len(missing_ids)} items")
            
            return missing_ids
            
        except Exception as e:
            self.logger.error(f"Error getting missing IDs for {endpoint_name}: {e}")
            return []
    
    def _get_missing_game_ids(self, endpoint_name: str, master_table: str) -> List[dict]:
        """Get missing game IDs for game-based endpoints by comparing master table vs endpoint table"""
        try:
            with self.db_manager.get_cursor() as cursor:
                # For test mode, return some sample game IDs to test the system
                if self.test_mode:
                    # Get some real game IDs from master table for testing
                    cursor.execute(f"SELECT DISTINCT game_id FROM {master_table} LIMIT %s", (self.max_items_per_endpoint,))
                    game_rows = cursor.fetchall()
                    
                    if game_rows:
                        sample_game_ids = [row[0] for row in game_rows]
                        self.logger.info(f"Test mode: Using {len(sample_game_ids)} game IDs from master table for {endpoint_name}")
                        return [{'game_id': game_id} for game_id in sample_game_ids]
                    else:
                        self.logger.warning(f"No game IDs found in master table {master_table}")
                        return []
                
                # Production mode: Find games in master table that are NOT in endpoint table
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                # Check if endpoint table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Table doesn't exist - all games are missing (first run)
                    self.logger.info(f"Endpoint table {endpoint_table_name} doesn't exist - processing ALL games from master table")
                    cursor.execute(f"SELECT DISTINCT game_id FROM {master_table} ORDER BY game_id")
                    all_games = cursor.fetchall()
                    missing_games = [{'game_id': row[0]} for row in all_games]
                    
                else:
                    # Table exists - find missing games
                    cursor.execute(f"""
                        SELECT DISTINCT m.game_id 
                        FROM {master_table} m
                        LEFT JOIN {endpoint_table_name} e ON m.game_id = e.game_id
                        WHERE e.game_id IS NULL
                        ORDER BY m.game_id
                    """)
                    
                    missing_rows = cursor.fetchall()
                    missing_games = [{'game_id': row[0]} for row in missing_rows]
                
                self.logger.info(f"Found {len(missing_games)} missing games for {endpoint_name}")
                return missing_games
                
        except Exception as e:
            self.logger.error(f"Error getting missing game IDs for {endpoint_name}: {e}")
            return []
    
    def _get_missing_player_ids(self, endpoint_name: str, master_table: str) -> List[dict]:
        """Get missing player IDs for player-based endpoints by comparing master table vs endpoint table"""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Find the player_id column in master table
                possible_columns = ['player_id', 'playerid', 'person_id', 'personid', 'id']
                player_column = None
                
                # Check which column exists in master table
                cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = ANY(%s)
                """, (master_table.split('_', 1)[1], possible_columns))
                
                result = cursor.fetchone()
                if not result:
                    self.logger.warning(f"No player_id column found in {master_table}")
                    return []
                
                player_column = result[0]
                
                # For test mode, just get some player IDs from the master table
                if self.test_mode:
                    cursor.execute(f"SELECT DISTINCT {player_column} FROM {master_table} LIMIT %s", (self.max_items_per_endpoint,))
                    player_rows = cursor.fetchall()
                    
                    if player_rows:
                        sample_player_ids = [row[0] for row in player_rows]
                        self.logger.info(f"Test mode: Using {len(sample_player_ids)} player IDs from master table for {endpoint_name}")
                        return [{'player_id': player_id} for player_id in sample_player_ids]
                    else:
                        return []
                
                # Production mode: Find players in master table that are NOT in endpoint table
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                # Check if endpoint table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Table doesn't exist - all players are missing (first run)
                    self.logger.info(f"Endpoint table {endpoint_table_name} doesn't exist - processing ALL players from master table")
                    cursor.execute(f"SELECT DISTINCT {player_column} FROM {master_table} ORDER BY {player_column}")
                    all_players = cursor.fetchall()
                    missing_players = [{'player_id': row[0]} for row in all_players]
                    
                else:
                    # Table exists - find missing players
                    cursor.execute(f"""
                        SELECT DISTINCT m.{player_column} 
                        FROM {master_table} m
                        LEFT JOIN {endpoint_table_name} e ON m.{player_column} = e.player_id
                        WHERE e.player_id IS NULL
                        ORDER BY m.{player_column}
                    """)
                    
                    missing_rows = cursor.fetchall()
                    missing_players = [{'player_id': row[0]} for row in missing_rows]
                
                self.logger.info(f"Found {len(missing_players)} missing players for {endpoint_name}")
                return missing_players
                        
        except Exception as e:
            self.logger.error(f"Error getting missing player IDs for {endpoint_name}: {e}")
            return []
    
    def _get_missing_team_ids(self, endpoint_name: str, master_table: str) -> List[dict]:
        """Get missing team IDs for team-based endpoints by comparing master table vs endpoint table"""
        try:
            with self.db_manager.get_cursor() as cursor:
                # For now, use a standard set of NBA team IDs (30 teams)
                # This could be enhanced to use actual team master table
                nba_team_ids = [
                    1610612737, 1610612738, 1610612751, 1610612766, 1610612741, 1610612739,
                    1610612742, 1610612743, 1610612765, 1610612744, 1610612745, 1610612754,
                    1610612746, 1610612747, 1610612763, 1610612748, 1610612749, 1610612750,
                    1610612740, 1610612752, 1610612760, 1610612753, 1610612755, 1610612756,
                    1610612757, 1610612758, 1610612759, 1610612761, 1610612762, 1610612764
                ]
                
                # For test mode, just return a few team IDs
                if self.test_mode:
                    test_teams = nba_team_ids[:self.max_items_per_endpoint or 5]
                    self.logger.info(f"Test mode: Using {len(test_teams)} team IDs for {endpoint_name}")
                    return [{'team_id': team_id} for team_id in test_teams]
                
                # Production mode: Find teams that are NOT in endpoint table
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                # Check if endpoint table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Table doesn't exist - all teams are missing (first run)
                    self.logger.info(f"Endpoint table {endpoint_table_name} doesn't exist - processing ALL teams")
                    missing_teams = [{'team_id': team_id} for team_id in nba_team_ids]
                    
                else:
                    # Table exists - find missing teams
                    placeholders = ','.join(['%s'] * len(nba_team_ids))
                    cursor.execute(f"""
                        SELECT t.team_id
                        FROM (VALUES {','.join([f'({team_id})' for team_id in nba_team_ids])}) AS t(team_id)
                        LEFT JOIN {endpoint_table_name} e ON t.team_id = e.team_id
                        WHERE e.team_id IS NULL
                        ORDER BY t.team_id
                    """)
                    
                    missing_rows = cursor.fetchall()
                    missing_teams = [{'team_id': row[0]} for row in missing_rows]
                
                self.logger.info(f"Found {len(missing_teams)} missing teams for {endpoint_name}")
                return missing_teams
                        
        except Exception as e:
            self.logger.error(f"Error getting missing team IDs for {endpoint_name}: {e}")
            return []
    
    def _get_missing_player_season_combinations(self, endpoint_name: str, master_table: str, required_params: List[str]) -> List[dict]:
        """Get missing player + season combinations for comprehensive historical backfill"""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Find the player_id column in master table
                possible_columns = ['player_id', 'playerid', 'person_id', 'personid', 'id']
                player_column = None
                
                cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = ANY(%s)
                """, (master_table.split('_', 1)[1], possible_columns))
                
                result = cursor.fetchone()
                if not result:
                    self.logger.warning(f"No player_id column found in {master_table}")
                    return []
                
                player_column = result[0]
                
                # Get all seasons (comprehensive historical range)
                seasons = []
                for year in range(1996, 2025):  # 1996-97 through 2024-25
                    if self.league == 'WNBA':
                        seasons.append(str(year))  # WNBA uses single year format
                    else:
                        seasons.append(f"{year}-{str(year+1)[2:]}")  # NBA uses 1996-97 format
                
                # For test mode, limit seasons and players
                if self.test_mode:
                    seasons = seasons[-3:]  # Last 3 seasons only
                    cursor.execute(f"SELECT DISTINCT {player_column} FROM {master_table} LIMIT %s", (5,))
                    player_rows = cursor.fetchall()
                    
                    if not player_rows:
                        return []
                    
                    # Create combinations
                    combinations = []
                    for player_row in player_rows:
                        player_id = player_row[0]
                        for season in seasons:
                            combinations.append({
                                'player_id': player_id,
                                'season': season
                            })
                    
                    self.logger.info(f"Test mode: Generated {len(combinations)} player-season combinations for {endpoint_name}")
                    return combinations
                
                # Production mode: Get all players from master table
                cursor.execute(f"SELECT DISTINCT {player_column} FROM {master_table} ORDER BY {player_column}")
                all_players = cursor.fetchall()
                
                if not all_players:
                    self.logger.warning(f"No players found in master table {master_table}")
                    return []
                
                # Check what combinations already exist in endpoint table
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Table doesn't exist - generate ALL combinations
                    self.logger.info(f"Endpoint table {endpoint_table_name} doesn't exist - generating ALL player-season combinations")
                    combinations = []
                    for player_row in all_players:
                        player_id = player_row[0]
                        for season in seasons:
                            combinations.append({
                                'player_id': player_id,
                                'season': season
                            })
                    
                    self.logger.info(f"Generated {len(combinations)} total player-season combinations ({len(all_players)} players × {len(seasons)} seasons)")
                    return combinations
                
                else:
                    # Table exists - find missing combinations
                    self.logger.info(f"Finding missing player-season combinations in {endpoint_table_name}")
                    
                    # Create temp table with all possible combinations
                    cursor.execute("""
                        CREATE TEMP TABLE temp_player_seasons AS
                        SELECT p.player_id, s.season
                        FROM (SELECT UNNEST(%s::bigint[]) AS player_id) p
                        CROSS JOIN (SELECT UNNEST(%s::text[]) AS season) s
                    """, ([row[0] for row in all_players], seasons))
                    
                    # Find missing combinations
                    cursor.execute(f"""
                        SELECT t.player_id, t.season
                        FROM temp_player_seasons t
                        LEFT JOIN {endpoint_table_name} e ON t.player_id = e.player_id AND t.season = e.season
                        WHERE e.player_id IS NULL
                        ORDER BY t.player_id, t.season
                    """)
                    
                    missing_rows = cursor.fetchall()
                    combinations = [{'player_id': row[0], 'season': row[1]} for row in missing_rows]
                    
                    # Clean up temp table
                    cursor.execute("DROP TABLE temp_player_seasons")
                    
                    self.logger.info(f"Found {len(combinations)} missing player-season combinations for {endpoint_name}")
                    return combinations
                        
        except Exception as e:
            self.logger.error(f"Error getting missing player-season combinations for {endpoint_name}: {e}")
            return []
    
    def _get_missing_team_season_combinations(self, endpoint_name: str, required_params: List[str]) -> List[dict]:
        """Get missing team + season combinations for comprehensive historical backfill"""
        try:
            # NBA team IDs
            nba_team_ids = [
                1610612737, 1610612738, 1610612751, 1610612766, 1610612741, 1610612739,
                1610612742, 1610612743, 1610612765, 1610612744, 1610612745, 1610612754,
                1610612746, 1610612747, 1610612763, 1610612748, 1610612749, 1610612750,
                1610612740, 1610612752, 1610612760, 1610612753, 1610612755, 1610612756,
                1610612757, 1610612758, 1610612759, 1610612761, 1610612762, 1610612764
            ]
            
            # Get all seasons
            seasons = []
            for year in range(1996, 2025):
                if self.league == 'WNBA':
                    seasons.append(str(year))
                else:
                    seasons.append(f"{year}-{str(year+1)[2:]}")
            
            # For test mode, limit teams and seasons
            if self.test_mode:
                seasons = seasons[-2:]  # Last 2 seasons
                test_teams = nba_team_ids[:3]  # 3 teams
                
                combinations = []
                for team_id in test_teams:
                    for season in seasons:
                        combinations.append({
                            'team_id': team_id,
                            'season': season
                        })
                
                self.logger.info(f"Test mode: Generated {len(combinations)} team-season combinations for {endpoint_name}")
                return combinations
            
            # Production mode: Generate all combinations and check what's missing
            with self.db_manager.get_cursor() as cursor:
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # Generate all combinations
                    combinations = []
                    for team_id in nba_team_ids:
                        for season in seasons:
                            combinations.append({
                                'team_id': team_id,
                                'season': season
                            })
                    
                    self.logger.info(f"Generated {len(combinations)} total team-season combinations ({len(nba_team_ids)} teams × {len(seasons)} seasons)")
                    return combinations
                
                else:
                    # Find missing combinations
                    cursor.execute("""
                        CREATE TEMP TABLE temp_team_seasons AS
                        SELECT t.team_id, s.season
                        FROM (SELECT UNNEST(%s::bigint[]) AS team_id) t
                        CROSS JOIN (SELECT UNNEST(%s::text[]) AS season) s
                    """, (nba_team_ids, seasons))
                    
                    cursor.execute(f"""
                        SELECT t.team_id, t.season
                        FROM temp_team_seasons t
                        LEFT JOIN {endpoint_table_name} e ON t.team_id = e.team_id AND t.season = e.season
                        WHERE e.team_id IS NULL
                        ORDER BY t.team_id, t.season
                    """)
                    
                    missing_rows = cursor.fetchall()
                    combinations = [{'team_id': row[0], 'season': row[1]} for row in missing_rows]
                    
                    cursor.execute("DROP TABLE temp_team_seasons")
                    
                    self.logger.info(f"Found {len(combinations)} missing team-season combinations for {endpoint_name}")
                    return combinations
                        
        except Exception as e:
            self.logger.error(f"Error getting missing team-season combinations for {endpoint_name}: {e}")
            return []
    
    def _get_missing_season_data(self, endpoint_name: str, required_params: List[str]) -> List[dict]:
        """Get missing season data for season-based endpoints like LeagueGameLog, PlayerGameLogs"""
        try:
            # Get all seasons
            seasons = []
            for year in range(1996, 2025):
                if self.league == 'WNBA':
                    seasons.append(str(year))
                else:
                    seasons.append(f"{year}-{str(year+1)[2:]}")
            
            # For test mode, limit to recent seasons
            if self.test_mode:
                seasons = seasons[-2:]  # Last 2 seasons
                self.logger.info(f"Test mode: Using {len(seasons)} seasons for {endpoint_name}")
                return [{'season': season} for season in seasons]
            
            # Production mode: Check what seasons are missing
            with self.db_manager.get_cursor() as cursor:
                endpoint_table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}"
                
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (endpoint_table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if not table_exists:
                    # All seasons are missing
                    self.logger.info(f"Processing all {len(seasons)} seasons for {endpoint_name}")
                    return [{'season': season} for season in seasons]
                
                else:
                    # Find missing seasons
                    cursor.execute(f"""
                        SELECT s.season
                        FROM (SELECT UNNEST(%s::text[]) AS season) s
                        LEFT JOIN {endpoint_table_name} e ON s.season = e.season
                        WHERE e.season IS NULL
                        ORDER BY s.season
                    """, (seasons,))
                    
                    missing_rows = cursor.fetchall()
                    missing_seasons = [{'season': row[0]} for row in missing_rows]
                    
                    self.logger.info(f"Found {len(missing_seasons)} missing seasons for {endpoint_name}")
                    return missing_seasons
                        
        except Exception as e:
            self.logger.error(f"Error getting missing season data for {endpoint_name}: {e}")
            return []
    
    def process_single_endpoint(self, endpoint_name: str, config: dict) -> bool:
        """
        Process a single endpoint - main processing logic
        
        Args:
            endpoint_name: Name of the endpoint to process
            config: Endpoint configuration dictionary
            
        Returns:
            True if processing completed successfully
        """
        self.logger.info(f"Processing endpoint: {endpoint_name}")
        
        try:
            # Get the endpoint class
            endpoint_class = getattr(nbaapi, endpoint_name)
            
            # Get missing IDs for this endpoint
            missing_ids = self.get_missing_ids_for_endpoint(endpoint_name, config)
            
            if not missing_ids:
                self.logger.info(f"No missing data for {endpoint_name}")
                return True
            
            self.logger.info(f"Processing {len(missing_ids)} items for {endpoint_name}")
            
            # Process each set of parameters
            for i, param_values in enumerate(missing_ids):
                try:
                    self.logger.debug(f"Processing item {i+1}/{len(missing_ids)} for {endpoint_name}")
                    
                    # Add league and season parameters
                    api_params = param_values.copy()
                    
                    # Add league parameter (check for different parameter names)
                    import inspect
                    sig = inspect.signature(endpoint_class.__init__)
                    if 'league_id' in sig.parameters:
                        api_params['league_id'] = self.league_config['id']
                    elif 'league_id_nullable' in sig.parameters:
                        api_params['league_id_nullable'] = self.league_config['id']
                    
                    # Add season parameter (preserve from param_values if present, otherwise use current)
                    if 'season' in sig.parameters:
                        api_params['season'] = api_params.get('season', self.current_season)
                    elif 'season_nullable' in sig.parameters:
                        api_params['season_nullable'] = api_params.get('season_nullable', self.current_season)
                    
                    # Make API call
                    self.logger.debug(f"API call: {endpoint_name}({api_params})")
                    endpoint_instance = endpoint_class(**api_params)
                    
                    # Get DataFrames
                    dataframes = endpoint_instance.get_data_frames()
                    
                    if not dataframes:
                        self.logger.warning(f"No DataFrames returned for {endpoint_name}")
                        continue
                    
                    # Match DataFrames to expected names
                    matched_data = self.match_dataframes_to_expected_data(endpoint_name, dataframes)
                    
                    # Insert each DataFrame into its respective table
                    for dataset_name, df in matched_data.items():
                        if df is not None and not df.empty:
                            table_name = f"{self.get_table_prefix()}_{endpoint_name.lower()}_{dataset_name.lower()}"
                            
                            success = self.insert_dataframe_to_table(
                                df, table_name, 
                                config.get('required_params', []), 
                                api_params  # Use the actual API parameters, not the original param_values
                            )
                            
                            if not success:
                                self.logger.warning(f"Failed to insert {dataset_name} for {endpoint_name}")
                    
                    # Rate limiting
                    time.sleep(0.6)  # 100 requests per minute limit
                    
                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"API call failed for {endpoint_name}: {error_msg}")
                    
                    # Create failed records in main data tables
                    self.create_failed_records(endpoint_name, config, param_values, error_msg)
                    
                    # Continue with next item
                    continue
            
            self.logger.info(f"Completed processing {endpoint_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error processing {endpoint_name}: {e}")
            return False
    
    def run_master_endpoints(self) -> bool:
        """
        Run all master endpoints first - these populate the master tables
        
        Returns:
            True if all master endpoints completed successfully
        """
        self.logger.info("Processing Master Endpoints")
        
        master_endpoints = self.get_master_endpoints()
        
        if not master_endpoints:
            self.logger.warning("No master endpoints found")
            return True
        
        success_count = 0
        
        for endpoint_name, config in master_endpoints:
            self.logger.info(f"Processing master endpoint: {endpoint_name}")
            
            if self.process_single_endpoint(endpoint_name, config):
                success_count += 1
            else:
                self.logger.error(f"ERROR: Master endpoint failed: {endpoint_name}")
        
        self.logger.info(f"Master endpoints completed: {success_count}/{len(master_endpoints)} successful")
        return success_count == len(master_endpoints)
    
    def run_processable_endpoints(self) -> bool:
        """
        Run all processable endpoints (high priority, latest version)
        
        Returns:
            True if processing completed
        """
        self.logger.info("STARTING: Processing Regular Endpoints")
        
        processable_endpoints = self.get_processable_endpoints()
        
        if not processable_endpoints:
            self.logger.warning("No processable endpoints found")
            return True
        
        success_count = 0
        
        for endpoint_name, config in processable_endpoints:
            if self.process_single_endpoint(endpoint_name, config):
                success_count += 1
        
        self.logger.info(f"Regular endpoints completed: {success_count}/{len(processable_endpoints)} successful")
        return True
    
    def run_full_collection(self) -> bool:
        """
        Run the complete data collection process
        1. Master endpoints first
        2. Regular endpoints second
        
        Returns:
            True if collection completed successfully
        """
        self.logger.info("STARTING: Full NBA Data Collection")
        
        start_time = datetime.now()
        
        # Step 1: Process master endpoints
        self.logger.info("STEP 1: Processing Master Endpoints")
        if not self.run_master_endpoints():
            self.logger.error("ERROR: Master endpoints failed - stopping collection")
            return False
        
        # Step 2: Process regular endpoints
        self.logger.info("STEP 2: Processing Regular Endpoints")
        self.run_processable_endpoints()
        
        # Summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        self.logger.info("COMPLETE: NBA Data Collection Complete")
        self.logger.info(f"Duration: {duration}")
        self.logger.info(f"League: {self.league}")
        self.logger.info(f"Test Mode: {self.test_mode}")
        
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='NBA Data Collection Engine')
    parser.add_argument('--league', default='NBA', choices=['NBA', 'WNBA', 'G-League'],
                       help='League to process')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode with limited data')
    parser.add_argument('--max-items', type=int,
                       help='Maximum items to process per endpoint (for testing)')
    parser.add_argument('--log-level', default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    parser.add_argument('--endpoint', 
                       help='Process a specific endpoint only')
    parser.add_argument('--single-endpoint',
                       help='Process a single specific endpoint (distributed mode)')
    parser.add_argument('--connection-timeout', type=int, default=60,
                       help='Database connection timeout in seconds')
    parser.add_argument('--retry-attempts', type=int, default=3,
                       help='Number of retry attempts for failed operations')
    parser.add_argument('--run-full', action='store_true',
                       help='Run full data collection process')
    parser.add_argument('--masters-only', action='store_true',
                       help='Run master endpoints only')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = NBADataProcessor(
        league=args.league,
        test_mode=args.test_mode,
        max_items_per_endpoint=args.max_items,
        log_level=args.log_level
    )
    
    # Execute based on arguments
    if args.run_full:
        # Run complete data collection
        processor.run_full_collection()
    elif args.masters_only:
        # Run master endpoints only
        processor.run_master_endpoints()
    elif args.endpoint or args.single_endpoint:
        # Process specific endpoint
        endpoint_name = args.endpoint or args.single_endpoint
        endpoint_config = processor.endpoint_config['endpoints'].get(endpoint_name)
        if endpoint_config:
            processor.process_single_endpoint(endpoint_name, endpoint_config)
        else:
            print(f"Error: Endpoint '{endpoint_name}' not found")
    else:
        # Default: Show available endpoints
        master_endpoints = processor.get_master_endpoints()
        print(f"\nMaster endpoints for {args.league}:")
        for name, config in master_endpoints:
            print(f"  - {name}: master for {config['master']}")
        
        processable_endpoints = processor.get_processable_endpoints()
        print(f"\nProcessable endpoints for {args.league}: {len(processable_endpoints)}")
        print("First 10:")
        for name, config in processable_endpoints[:10]:
            print(f"  - {name}")
        
        print(f"\nUsage examples:")
        print(f"  python {sys.argv[0]} --masters-only --test-mode")
        print(f"  python {sys.argv[0]} --endpoint CommonAllPlayers --test-mode")
        print(f"  python {sys.argv[0]} --run-full --test-mode")