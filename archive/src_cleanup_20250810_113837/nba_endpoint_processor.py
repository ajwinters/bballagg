"""
NBA Endpoint Processor

This module provides a systematic way to process NBA API endpoints,
create tables, and maintain data using the master tables as parameter sources.
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
        logging.FileHandler('nba_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import our modules
import nba_api.stats.endpoints as nbaapi
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from nba_endpoints_config import ALL_ENDPOINTS, get_endpoints_by_priority, get_endpoints_by_category

sys.path.append(os.path.join(os.path.dirname(__file__)))
import allintwo


class NBAEndpointProcessor:
    """
    Systematic NBA API endpoint processor that creates tables and maintains data
    """
    
    def __init__(self, db_connection, rate_limit=1.0):
        self.conn = db_connection
        self.rate_limit = rate_limit  # seconds between API calls
        self.processed_count = 0
        self.error_count = 0
        
    def get_master_data(self, table_name):
        """Fetch data from master tables"""
        try:
            return allintwo.fetch_table_to_dataframe(self.conn, table_name)
        except Exception as e:
            logger.error(f"Failed to fetch master data from {table_name}: {str(e)}")
            return None
    
    def get_parameter_values(self, parameter_source):
        """Get parameter values from master tables"""
        if parameter_source == 'from_mastergames':
            df = self.get_master_data('mastergames')
            return df['gameid'].unique().tolist() if df is not None else []
            
        elif parameter_source == 'from_masterplayers':
            df = self.get_master_data('masterplayers') 
            return df['playerid'].unique().tolist() if df is not None else []
            
        elif parameter_source == 'from_masterteams':
            df = self.get_master_data('masterteams')
            return df['id'].unique().tolist() if df is not None else []
            
        elif parameter_source == 'current_season':
            # Get current season from masterseasons
            df = self.get_master_data('masterseasons')
            if df is not None:
                return [df.iloc[0]['season']]  # Most recent season first
            return ['2024-25']  # fallback
            
        return []
    
    def generate_table_name(self, endpoint_name, dataframe_name):
        """Generate standardized table names"""
        endpoint_lower = endpoint_name.lower()
        df_name_lower = dataframe_name.lower()
        return f"{endpoint_lower}_{df_name_lower}"
    
    def get_missing_parameters(self, table_name, parameter_name, all_parameters):
        """Get parameters missing from existing table"""
        try:
            existing_data = allintwo.fetch_table_to_dataframe(self.conn, table_name)
            if existing_data is not None and parameter_name in existing_data.columns:
                existing_params = set(existing_data[parameter_name].unique())
                all_params = set(all_parameters)
                missing = list(all_params - existing_params)
                logger.info(f"Table {table_name}: {len(missing)} missing parameters out of {len(all_parameters)} total")
                return missing
            else:
                logger.info(f"Table {table_name} doesn't exist or missing column {parameter_name}, processing all {len(all_parameters)} parameters")
                return all_parameters
        except Exception as e:
            logger.warning(f"Could not check existing data for {table_name}: {str(e)}")
            return all_parameters
    
    def process_endpoint(self, endpoint_config, limit=None):
        """Process a single endpoint configuration"""
        endpoint_name = endpoint_config['endpoint']
        logger.info(f"Processing endpoint: {endpoint_name}")
        
        try:
            # Get the endpoint class
            endpoint_class = getattr(nbaapi, endpoint_name)
            
            # Determine parameter source and values
            parameters = endpoint_config['parameters']
            param_key = list(parameters.keys())[0]  # Get first parameter
            param_source = parameters[param_key]
            
            # Get all parameter values
            all_param_values = self.get_parameter_values(param_source)
            
            if not all_param_values:
                logger.warning(f"No parameter values found for {param_source}")
                return False
                
            if limit:
                all_param_values = all_param_values[:limit]
                logger.info(f"Limited to {limit} parameters for testing")
            
            # Test with first parameter to get dataframe structure
            logger.info(f"Testing {endpoint_name} with first parameter...")
            test_param = all_param_values[0]
            
            try:
                test_endpoint = endpoint_class(**{param_key: test_param})
                test_dataframes = test_endpoint.get_data_frames()
                
                logger.info(f"Endpoint {endpoint_name} returns {len(test_dataframes)} dataframes")
                
                # Process each dataframe
                for df_index, df in enumerate(test_dataframes):
                    if df.empty:
                        logger.warning(f"Dataframe {df_index} is empty, skipping")
                        continue
                        
                    # Generate table name
                    df_name = list(test_endpoint.expected_data.keys())[df_index]
                    table_name = self.generate_table_name(endpoint_name, df_name)
                    
                    logger.info(f"Processing dataframe {df_index}: {table_name}")
                    
                    # Create table if it doesn't exist
                    cleaned_df = allintwo.clean_column_names(df)
                    allintwo.create_table(self.conn, table_name, cleaned_df)
                    
                    # Get missing parameters for this specific table
                    missing_params = self.get_missing_parameters(table_name, param_key.replace('_id', 'id'), all_param_values)
                    
                    if not missing_params:
                        logger.info(f"Table {table_name} is up to date, skipping")
                        continue
                    
                    # Process missing parameters
                    success_count = 0
                    error_count = 0
                    
                    for i, param_value in enumerate(missing_params):
                        try:
                            logger.debug(f"Processing {param_value} ({i+1}/{len(missing_params)})")
                            
                            # Make API call
                            endpoint_instance = endpoint_class(**{param_key: param_value})
                            dataframes = endpoint_instance.get_data_frames()
                            
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
                            if (i + 1) % 100 == 0:
                                logger.info(f"Progress: {i+1}/{len(missing_params)} ({success_count} success, {error_count} errors)")
                                
                        except Exception as e:
                            logger.error(f"Error processing {param_value}: {str(e)}")
                            error_count += 1
                            self.error_count += 1
                            
                            # Longer sleep after error
                            time.sleep(self.rate_limit * 2)
                    
                    logger.info(f"Completed {table_name}: {success_count} success, {error_count} errors")
                
                return True
                
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
    """Main execution function for testing"""
    # Connect to database
    conn = allintwo.connect_to_rds('thebigone', 'ajwin', 'CharlesBark!23', 'nba-rds-instance.c9wwc0ukkiu5.us-east-1.rds.amazonaws.com')
    
    if not conn:
        logger.error("Failed to connect to database")
        return
    
    # Create processor
    processor = NBAEndpointProcessor(conn, rate_limit=0.6)
    
    # Process high priority game-based endpoints with limit for testing
    logger.info("Starting NBA endpoint processing...")
    results = processor.process_endpoints_by_category('game_based', priority='high', limit=10)
    
    # Print results
    summary = processor.get_processing_summary()
    logger.info(f"Processing complete: {summary}")
    
    for endpoint, result in results.items():
        status = "✓" if result['success'] else "✗"
        logger.info(f"{status} {endpoint}: {result['duration']:.2f}s")


if __name__ == "__main__":
    main()
