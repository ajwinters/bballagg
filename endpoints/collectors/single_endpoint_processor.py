#!/usr/bin/env python3
"""
Single Endpoint NBA Data Processor for SLURM
Processes one endpoint per job for parallel execution
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config'))

from collectors.rds_connection_manager import RDSConnectionManager
from collectors.endpoint_processor import NBAEndpointProcessor
from config.nba_endpoints_config import get_endpoint_by_name

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
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
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
        os.environ['DB_PORT'] = config['port']
        os.environ['DB_SSLMODE'] = config.get('ssl_mode', 'require')
        
        return config
        
    except Exception as e:
        raise Exception(f"Failed to load database config from {config_path}: {e}")

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
        
        # Initialize processor
        processor = NBAEndpointProcessor(
            connection_manager=conn_manager,
            league='NBA',
            rate_limit=rate_limit
        )
        
        # Create failed calls table
        processor.create_failed_calls_table()
        
        # Process the endpoint
        logger.info(f"Processing endpoint {endpoint_name} with rate limit {rate_limit}s...")
        
        success = processor.process_endpoint(endpoint_config)
        
        # Get processing summary
        summary = processor.get_processing_summary()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Log results
        logger.info(f"=== PROCESSING COMPLETE ===")
        logger.info(f"Endpoint: {endpoint_name}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Success: {success}")
        logger.info(f"Total processed: {summary['total_processed']}")
        logger.info(f"Total errors: {summary['total_errors']}")
        logger.info(f"Success rate: {summary['success_rate']:.1f}%")
        
        # Close connection
        conn_manager.close_connection()
        
        return success, summary
        
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
