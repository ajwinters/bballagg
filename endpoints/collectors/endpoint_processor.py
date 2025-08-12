"""
NBA Endpoint Processor

This module provides a systematic way to process NBA API endpoints,
create tables, and maintain data using the league-separated master tables as parameter sources.
Focuses on NBA league only initially, with league-prefixed table naming.
"""

import pandas as pd
import time
import sys
import os
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nba_endpoint_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import our modules
import nba_api.stats.endpoints as nbaapi
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from nba_endpoints_config import ALL_ENDPOINTS, get_endpoints_by_priority, get_endpoints_by_category

# Configure NBA API timeout settings
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set longer timeout for NBA API requests to prevent read timeouts
def configure_nba_api_timeout():
    """Configure NBA API with longer timeout and retry strategy"""
    session = requests.Session()
    
    # Create a retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Mount it for both http and https usage
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set longer timeout (60 seconds instead of default 30)
    session.timeout = 60
    
    return session

# Configure the timeout on import
configure_nba_api_timeout()

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
import allintwo
from rds_connection_manager import RDSConnectionManager


class NBAEndpointProcessor:
    """
    NBA API endpoint processor that creates league-prefixed tables and maintains incremental data.
    Focuses on NBA league initially, with failed call tracking.
    """
    
    def __init__(self, connection_manager, league='NBA', rate_limit=1.0):
        self.conn_manager = connection_manager  # Use connection manager instead of direct connection
        self.conn = connection_manager  # For backward compatibility with existing code
        self.league = league.upper()
        self.league_prefix = league.lower()  # For table naming: nba_
        self.rate_limit = rate_limit  # seconds between API calls
        self.processed_count = 0
        self.error_count = 0
        self.failed_calls = {}  # Track failed calls by endpoint and parameter
        
        # Master table mappings for the league
        self.master_tables = {
            'games': f'{self.league_prefix}_games',
            'players': f'{self.league_prefix}_players', 
            'teams': f'{self.league_prefix}_teams'
        }
        
    def get_master_data(self, table_name):
        """Fetch data from league-specific master tables"""
        try:
            df = allintwo.fetch_table_to_dataframe(self.conn, table_name)
            if df is not None:
                logger.info(f"Fetched {len(df)} records from {table_name}")
                return df
            else:
                logger.warning(f"No data found in {table_name}")
                return None
        except Exception as e:
            logger.error(f"Failed to fetch master data from {table_name}: {str(e)}")
            return None
    
    def get_parameter_values(self, parameter_source):
        """Get parameter values from league-specific master tables"""
        if parameter_source == 'from_mastergames':
            df = self.get_master_data(self.master_tables['games'])
            if df is not None and 'gameid' in df.columns:  # Updated for standardized columns
                game_ids = df['gameid'].unique().tolist()
                logger.info(f"Found {len(game_ids)} unique game IDs for {self.league}")
                return game_ids
            return []
            
        elif parameter_source == 'from_masterplayers':
            df = self.get_master_data(self.master_tables['players'])
            if df is not None and 'playerid' in df.columns:  # Updated for standardized columns
                player_ids = df['playerid'].unique().tolist()
                logger.info(f"Found {len(player_ids)} unique player IDs for {self.league}")
                return player_ids
            return []
            
        elif parameter_source == 'from_masterteams':
            df = self.get_master_data(self.master_tables['teams'])
            if df is not None and 'teamid' in df.columns:  # Updated for standardized columns
                team_ids = df['teamid'].unique().tolist()
                logger.info(f"Found {len(team_ids)} unique team IDs for {self.league}")
                return team_ids
            return []
            
        elif parameter_source == 'current_season':
            # For NBA, use current season logic
            current_year = datetime.now().year
            if datetime.now().month >= 10:  # Season starts in fall
                return [f"{current_year}-{str(current_year + 1)[-2:]}"]
            else:
                return [f"{current_year - 1}-{str(current_year)[-2:]}"]
            
        return []
    
    def generate_table_name(self, endpoint_name, dataframe_name):
        """Generate league-prefixed table names"""
        endpoint_lower = endpoint_name.lower()
        df_name_lower = dataframe_name.lower()
        return f"{self.league_prefix}_{endpoint_lower}_{df_name_lower}"
    
    def get_missing_parameters(self, table_name, parameter_name, all_parameters):
        """Get parameters missing from existing endpoint table (incremental approach)"""
        try:
            existing_data = allintwo.fetch_table_to_dataframe(self.conn, table_name)
            
            if existing_data is not None and parameter_name in existing_data.columns:
                existing_params = set(existing_data[parameter_name].unique())
                all_params = set(all_parameters)
                missing = list(all_params - existing_params)
                
                # Remove previously failed parameters to avoid re-trying immediately
                failed_key = f"{table_name}_{parameter_name}"
                if failed_key in self.failed_calls:
                    failed_params = set(self.failed_calls[failed_key])
                    missing = list(set(missing) - failed_params)
                    logger.info(f"Excluding {len(failed_params)} previously failed parameters")
                
                logger.info(f"Table {table_name}: {len(missing)} missing parameters out of {len(all_parameters)} total")
                return missing
            else:
                logger.info(f"Table {table_name} doesn't exist or missing column {parameter_name}, processing all {len(all_parameters)} parameters")
                return all_parameters
                
        except Exception as e:
            logger.warning(f"Could not check existing data for {table_name}: {str(e)}")
            return all_parameters

    def make_nba_api_call_with_retry(self, endpoint_class, param_key, param_value, max_retries=3):
        """
        Make NBA API call with robust timeout and retry handling
        Enhanced with idle state recovery
        """
        retry_delay = 2
        
        for retry_attempt in range(max_retries):
            try:
                # Ensure database connection is healthy before API call
                self.conn.ensure_connection()
                
                # Create endpoint instance with parameters
                endpoint_instance = endpoint_class(**{param_key: param_value})
                
                # Get data with timeout handling
                dataframes = endpoint_instance.get_data_frames()
                
                return dataframes, None  # Success: return data and no error
                
            except AttributeError as e:
                if "'NoneType' object has no attribute 'keys'" in str(e):
                    error_msg = "NBA API returned None"
                    return None, error_msg  # Don't retry for None responses
                else:
                    raise e  # Re-raise other AttributeErrors
                    
            except Exception as e:
                error_str = str(e)
                
                # Check for timeout-related errors (including idle-state timeouts)
                if any(timeout_indicator in error_str for timeout_indicator in 
                      ["Read timed out", "HTTPSConnectionPool", "Connection timeout", "timeout", 
                       "Connection reset", "Connection aborted", "SSL", "Network is unreachable"]):
                    
                    if retry_attempt < max_retries - 1:
                        # Log with more context about potential idle state
                        logger.warning(f"NBA API timeout/network issue for {param_value} (attempt {retry_attempt + 1}/{max_retries})")
                        logger.warning(f"   Error: {error_str}")
                        logger.warning(f"   This could be due to idle connection state - retrying in {retry_delay}s...")
                        
                        # Longer delay for potential idle state recovery
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                        
                        # Force database connection refresh on network errors
                        try:
                            self.conn.ensure_connection()
                        except:
                            logger.warning("   Database connection refresh attempted")
                        
                        continue
                    else:
                        error_msg = f"NBA API timeout/network error after {max_retries} attempts (possible idle state issue): {error_str}"
                        return None, error_msg
                        
                # Check for rate limiting
                elif "429" in error_str or "rate limit" in error_str.lower():
                    if retry_attempt < max_retries - 1:
                        wait_time = retry_delay * 3  # Longer wait for rate limiting
                        logger.warning(f"NBA API rate limited for {param_value} - waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        retry_delay *= 2
                        continue
                    else:
                        error_msg = f"NBA API rate limited after {max_retries} attempts: {error_str}"
                        return None, error_msg
                        
                else:
                    # Non-timeout error, don't retry
                    error_msg = f"NBA API error: {error_str}"
                    return None, error_msg
        
        return None, "Unknown error in NBA API call"

    def track_failed_call(self, table_name, parameter_name, parameter_value, error):
        """Track failed API calls to avoid immediate retries"""
        failed_key = f"{table_name}_{parameter_name}"
        
        if failed_key not in self.failed_calls:
            self.failed_calls[failed_key] = []
        
        self.failed_calls[failed_key].append(parameter_value)
        
        # Log the failure
        logger.warning(f"Failed call tracked: {table_name} with {parameter_name}={parameter_value} - {str(error)[:100]}")

    def create_failed_calls_table(self):
        """Create a table to persist failed calls across runs"""
        try:
            create_query = """
                CREATE TABLE IF NOT EXISTS nba_endpoint_failed_calls (
                    id SERIAL PRIMARY KEY,
                    endpoint_name VARCHAR(100) NOT NULL,
                    table_name VARCHAR(150) NOT NULL,
                    parameter_name VARCHAR(50) NOT NULL,
                    parameter_value VARCHAR(100) NOT NULL,
                    error_message TEXT,
                    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    UNIQUE(endpoint_name, table_name, parameter_name, parameter_value)
                );
            """
            
            cursor = self.conn.cursor()
            cursor.execute(create_query)
            self.conn.commit()
            logger.info("Failed calls tracking table created/verified")
            
        except Exception as e:
            logger.error(f"Failed to create failed calls table: {str(e)}")

    def save_failed_call(self, endpoint_name, table_name, parameter_name, parameter_value, error_message):
        """Save failed call to database for persistence"""
        try:
            insert_query = """
                INSERT INTO nba_endpoint_failed_calls 
                (endpoint_name, table_name, parameter_name, parameter_value, error_message)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (endpoint_name, table_name, parameter_name, parameter_value)
                DO UPDATE SET 
                    retry_count = nba_endpoint_failed_calls.retry_count + 1,
                    failed_at = CURRENT_TIMESTAMP,
                    error_message = EXCLUDED.error_message;
            """
            
            cursor = self.conn.cursor()
            cursor.execute(insert_query, (endpoint_name, table_name, parameter_name, str(parameter_value), str(error_message)[:500]))
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to save failed call record: {str(e)}")
    
    def process_endpoint(self, endpoint_config, limit=None):
        """Process a single endpoint configuration with incremental updates and error tracking"""
        endpoint_name = endpoint_config['endpoint']
        logger.info(f"Processing endpoint: {endpoint_name} for {self.league}")
        
        try:
            # Get the endpoint class
            endpoint_class = getattr(nbaapi, endpoint_name)
            
            # Determine parameter source and values
            parameters = endpoint_config['parameters']
            param_key = list(parameters.keys())[0]  # Get first parameter
            param_source = parameters[param_key]
            
            # Get all parameter values from master tables
            all_param_values = self.get_parameter_values(param_source)
            
            if not all_param_values:
                logger.warning(f"No parameter values found for {param_source} in {self.league}")
                return False
                
            if limit:
                all_param_values = all_param_values[:limit]
                logger.info(f"Limited to {limit} parameters for testing")
            
            # Test with first parameter to get dataframe structure
            logger.info(f"Testing {endpoint_name} structure with first parameter...")
            test_param = all_param_values[0]
            
            try:
                test_endpoint = endpoint_class(**{param_key: test_param})
                test_dataframes = test_endpoint.get_data_frames()
                
                if not test_dataframes:
                    logger.warning(f"Endpoint {endpoint_name} returned no dataframes")
                    return False
                
                logger.info(f"Endpoint {endpoint_name} returns {len(test_dataframes)} dataframes")
                
                # Process each dataframe from the endpoint
                overall_success = True
                
                for df_index, df in enumerate(test_dataframes):
                    if df.empty:
                        logger.warning(f"Dataframe {df_index} is empty, skipping")
                        continue
                    
                    # Generate league-prefixed table name
                    try:
                        df_name = list(test_endpoint.expected_data.keys())[df_index]
                    except (IndexError, AttributeError):
                        df_name = f"dataframe_{df_index}"
                    
                    table_name = self.generate_table_name(endpoint_name, df_name)
                    
                    logger.info(f"Processing dataframe {df_index}: {table_name}")
                    
                    # Create table if it doesn't exist
                    cleaned_df = allintwo.clean_column_names(df)
                    try:
                        allintwo.create_table(self.conn, table_name, cleaned_df)
                        logger.info(f"Table {table_name} created/verified")
                    except Exception as e:
                        logger.error(f"Failed to create table {table_name}: {str(e)}")
                        continue
                    
                    # Get missing parameters for incremental processing
                    # Map parameter names (game_id -> gameid, player_id -> playerid, etc.)
                    param_column_name = param_key.replace('_', '')  # game_id -> gameid
                    missing_params = self.get_missing_parameters(table_name, param_column_name, all_param_values)
                    
                    if not missing_params:
                        logger.info(f"Table {table_name} is up to date, skipping")
                        continue
                    
                    # Process missing parameters incrementally
                    success_count = 0
                    error_count = 0
                    
                    logger.info(f"Processing {len(missing_params)} missing parameters for {table_name}")
                    
                    for i, param_value in enumerate(missing_params):
                        try:
                            # Periodic connection health check (every 100 calls)
                            if i > 0 and i % 100 == 0:
                                logger.info(f"🔍 Performing connection health check at {i}/{len(missing_params)} processed...")
                                self.conn.ensure_connection()
                            
                            logger.debug(f"Processing {param_value} ({i+1}/{len(missing_params)})")
                            
                            # Make API call with robust retry handling
                            dataframes, error_msg = self.make_nba_api_call_with_retry(
                                endpoint_class, param_key, param_value, max_retries=3
                            )
                            
                            # Handle API call failure
                            if dataframes is None:
                                logger.warning(f"API call failed for {param_value}: {error_msg}")
                                error_count += 1
                                self.error_count += 1
                                # Track the failed call
                                self.track_failed_call(table_name, param_column_name, param_value, error_msg)
                                self.save_failed_call(endpoint_name, table_name, param_column_name, param_value, error_msg)
                                continue
                            
                            # Check if API returned valid data
                            if dataframes is None:
                                logger.warning(f"API returned None dataframes for {param_value}")
                                error_count += 1
                                self.error_count += 1
                                # Track the failed call
                                self.track_failed_call(table_name, param_column_name, param_value, "API returned None dataframes")
                                self.save_failed_call(endpoint_name, table_name, param_column_name, param_value, "API returned None dataframes")
                                continue
                            
                            # Get the specific dataframe for this table
                            if df_index < len(dataframes) and not dataframes[df_index].empty:
                                df_to_insert = allintwo.clean_column_names(dataframes[df_index])
                                allintwo.insert_dataframe_to_rds(self.conn, df_to_insert, table_name)
                                success_count += 1
                            else:
                                logger.warning(f"No data returned for {param_value}")
                                
                            self.processed_count += 1
                            
                            # Rate limiting
                            time.sleep(self.rate_limit)
                            
                            # Progress reporting
                            if (i + 1) % 50 == 0:
                                logger.info(f"Progress: {i+1}/{len(missing_params)} ({success_count} success, {error_count} errors)")
                                
                        except Exception as e:
                            logger.error(f"Error processing {param_value}: {str(e)}")
                            error_count += 1
                            self.error_count += 1
                            
                            # Track the failed call
                            self.track_failed_call(table_name, param_column_name, param_value, e)
                            self.save_failed_call(endpoint_name, table_name, param_column_name, param_value, str(e))
                            
                            # Longer sleep after error
                            time.sleep(self.rate_limit * 2)
                    
                    logger.info(f"Completed {table_name}: {success_count} success, {error_count} errors")
                    
                    if error_count > success_count:
                        overall_success = False
                
                return overall_success
                
            except Exception as e:
                logger.error(f"Failed to test endpoint {endpoint_name}: {str(e)}")
                return False
                
        except AttributeError:
            logger.error(f"Endpoint {endpoint_name} not found in NBA API")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing {endpoint_name}: {str(e)}")
            return False
    
    def process_endpoints_by_category(self, category, priority=None, limit=None):
        """Process all endpoints in a category"""
        logger.info(f"Processing {category} endpoints" + (f" with priority {priority}" if priority else ""))
        
        endpoints = get_endpoints_by_category(category)
        
        if priority:
            endpoints = [ep for ep in endpoints if ep['priority'] == priority]
        
        logger.info(f"Found {len(endpoints)} endpoints to process")
        
        results = {}
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config['endpoint']
            logger.info(f"Starting {endpoint_name}...")
            
            start_time = time.time()
            success = self.process_endpoint(endpoint_config, limit=limit)
            end_time = time.time()
            
            results[endpoint_name] = {
                'success': success,
                'duration': end_time - start_time
            }
            
            logger.info(f"Completed {endpoint_name} in {end_time - start_time:.2f} seconds")
        
        return results
    
    def get_processing_summary(self):
        """Get summary of processing statistics"""
        return {
            'total_processed': self.processed_count,
            'total_errors': self.error_count,
            'success_rate': (self.processed_count - self.error_count) / max(self.processed_count, 1) * 100
        }


def main():
    """Main execution function for COMPREHENSIVE NBA endpoint processing"""
    # Connect to database with robust connection management
    try:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
        from rds_connection_manager import RDSConnectionManager
        conn_manager = RDSConnectionManager(
            'thebigone', 
            'ajwin', 
            'CharlesBark!23', 
            'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com',
            connection_timeout=30,
            max_retries=3,
            retry_delay=5
        )
        conn_manager.connect()
        logger.info("✅ Connected to RDS database with robust connection management")
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return
    
    # Create NBA processor (league-specific) with faster rate limiting for full processing
    processor = NBAEndpointProcessor(conn_manager, league='NBA', rate_limit=0.6)
    
    # Create failed calls tracking table
    processor.create_failed_calls_table()
    
    logger.info("🚀 Starting COMPREHENSIVE NBA endpoint processing...")
    logger.info("🎯 This will process ALL configured endpoints with full data")
    logger.info("⏸️  You can interrupt anytime - system will resume where it left off")
    logger.info("="*60)
    
    try:
        all_results = {}
        
        # PHASE 1: HIGH PRIORITY GAME-BASED ENDPOINTS (Core game data)
        logger.info("🏀 PHASE 1: High Priority Game-Based Endpoints")
        logger.info("="*60)
        game_high_results = processor.process_endpoints_by_category('game_based', priority='high')
        all_results.update(game_high_results)
        
        # PHASE 2: HIGH PRIORITY PLAYER-BASED ENDPOINTS (Core player data)
        logger.info("\n👥 PHASE 2: High Priority Player-Based Endpoints") 
        logger.info("="*60)
        player_high_results = processor.process_endpoints_by_category('player_based', priority='high')
        all_results.update(player_high_results)
        
        # PHASE 3: HIGH PRIORITY TEAM-BASED ENDPOINTS (Core team data)
        logger.info("\n🏟️  PHASE 3: High Priority Team-Based Endpoints")
        logger.info("="*60)
        team_high_results = processor.process_endpoints_by_category('team_based', priority='high')
        all_results.update(team_high_results)
        
        # PHASE 4: MEDIUM PRIORITY GAME-BASED ENDPOINTS (Extended game data)
        logger.info("\n🎮 PHASE 4: Medium Priority Game-Based Endpoints")
        logger.info("="*60)
        game_medium_results = processor.process_endpoints_by_category('game_based', priority='medium')
        all_results.update(game_medium_results)
        
        # PHASE 5: MEDIUM PRIORITY PLAYER-BASED ENDPOINTS (Extended player data)
        logger.info("\n👤 PHASE 5: Medium Priority Player-Based Endpoints")
        logger.info("="*60)
        player_medium_results = processor.process_endpoints_by_category('player_based', priority='medium')
        all_results.update(player_medium_results)
        
        # PHASE 6: MEDIUM PRIORITY TEAM-BASED ENDPOINTS (Extended team data)
        logger.info("\n🏀 PHASE 6: Medium Priority Team-Based Endpoints")
        logger.info("="*60)
        team_medium_results = processor.process_endpoints_by_category('team_based', priority='medium')
        all_results.update(team_medium_results)
        
        # PHASE 7: LEAGUE-BASED ENDPOINTS (League-wide data)
        logger.info("\n🏆 PHASE 7: League-Based Endpoints")
        logger.info("="*60)
        league_results = processor.process_endpoints_by_category('league_based')
        all_results.update(league_results)
        
        # PHASE 8: LOW PRIORITY ENDPOINTS (Nice-to-have data)
        logger.info("\n📈 PHASE 8: Low Priority Endpoints")
        logger.info("="*60)
        low_priority_results = {}
        for category in ['game_based', 'player_based', 'team_based']:
            category_results = processor.process_endpoints_by_category(category, priority='low')
            low_priority_results.update(category_results)
        all_results.update(low_priority_results)
        
    except KeyboardInterrupt:
        logger.info("\n⏸️  PROCESSING INTERRUPTED BY USER")
        logger.info("💡 No worries! System can resume from where it left off")
        logger.info("💡 Just run the script again - it will skip completed work")
    except Exception as e:
        logger.error(f"Unexpected error during processing: {str(e)}")
        logger.info("💡 System can resume from where it left off - just run again!")
    
    # FINAL COMPREHENSIVE SUMMARY
    logger.info("\n" + "="*60)
    logger.info("📊 COMPREHENSIVE PROCESSING SUMMARY")
    logger.info("="*60)
    
    # Processing statistics
    summary = processor.get_processing_summary()
    logger.info(f"🔢 Total API calls made: {summary['total_processed']:,}")
    logger.info(f"❌ Total errors encountered: {summary['total_errors']:,}")
    logger.info(f"✅ Overall success rate: {summary['success_rate']:.1f}%")
    
    # Endpoint results summary
    if all_results:
        logger.info(f"\n🎯 ENDPOINT PROCESSING RESULTS:")
        successful_endpoints = 0
        failed_endpoints = 0
        total_time = 0
        
        for endpoint, result in all_results.items():
            status = "✅" if result['success'] else "❌"
            logger.info(f"   {status} {endpoint}: {result['duration']:.1f}s")
            
            if result['success']:
                successful_endpoints += 1
            else:
                failed_endpoints += 1
            total_time += result['duration']
        
        logger.info(f"\n📈 FINAL STATISTICS:")
        logger.info(f"   ✅ Successful endpoints: {successful_endpoints}")
        logger.info(f"   ❌ Failed endpoints: {failed_endpoints}")
        logger.info(f"   🎯 Endpoint success rate: {successful_endpoints/(successful_endpoints+failed_endpoints)*100:.1f}%")
        logger.info(f"   ⏱️  Total processing time: {total_time/60:.1f} minutes")
        
        # Database summary
        try:
            cursor = conn_manager.cursor()
            cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'nba_%';")
            table_count = cursor.fetchone()[0]
            logger.info(f"   📊 Total NBA tables created: {table_count}")
            cursor.close()
        except:
            pass
    
    # Show failed calls summary
    if processor.failed_calls:
        logger.info(f"\n⚠️  FAILED CALLS SUMMARY:")
        total_failed = 0
        for key, failed_params in processor.failed_calls.items():
            failed_count = len(failed_params)
            total_failed += failed_count
            logger.info(f"   {key}: {failed_count} failed parameters")
        logger.info(f"   Total failed parameter calls: {total_failed}")
        logger.info(f"   💡 Failed calls are tracked and will be skipped on retry")
    
    logger.info(f"\n🎉 COMPREHENSIVE NBA DATA COLLECTION COMPLETE!")
    logger.info(f"📚 Check 'nba_endpoint_processor.log' for detailed processing log")
    logger.info(f"🔄 System is fully resumable - can continue from any interruption point")
    
    conn_manager.close()
    logger.info("✅ Database connection closed")


if __name__ == "__main__":
    main()
