#!/usr/bin/env python3
"""
Distributed NBA Endpoint Processing Runner

This script helps coordinate distributed processing across multiple nodes.
Each node can run different endpoints or parameter ranges to avoid API rate limits.
"""

import subprocess
import sys
import os
import json
import argparse
from datetime import datetime

def load_node_config(config_file):
    """Load node configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config {config_file}: {str(e)}")
        return None

def run_endpoint_processor(config, db_config_file=None, dry_run=False):
    """Run the endpoint processor with the given configuration"""
    
    cmd = ["python", "endpoint_processor.py"]
    
    # Add endpoints
    if 'endpoints' in config:
        cmd.extend(["--endpoints"] + config['endpoints'])
    
    # Add rate limiting
    if 'rate_limit' in config:
        cmd.extend(["--rate-limit", str(config['rate_limit'])])
    
    # Add node ID
    if 'node_id' in config:
        cmd.extend(["--node-id", config['node_id']])
    
    # Add parameter configuration
    if 'parameter_config' in config:
        param_config = config['parameter_config']
        if 'start_index' in param_config:
            cmd.extend(["--param-start", str(param_config['start_index'])])
        if 'limit' in param_config:
            cmd.extend(["--param-limit", str(param_config['limit'])])
        if 'end_index' in param_config:
            cmd.extend(["--param-end", str(param_config['end_index'])])
    
    # Add database config if provided
    if db_config_file:
        cmd.extend(["--db-config", db_config_file])
    
    # Add dry run flag
    if dry_run:
        cmd.append("--dry-run")
    
    print(f"[{datetime.now()}] Running command: {' '.join(cmd)}")
    print(f"Description: {config.get('description', 'N/A')}")
    print("-" * 60)
    
    if dry_run:
        print("DRY RUN - Command would be executed but not actually run")
        return True
    
    # Execute the command
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"[{datetime.now()}] Successfully completed node: {config.get('node_id', 'unknown')}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] Error running node {config.get('node_id', 'unknown')}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Distributed NBA Endpoint Processing Runner')
    parser.add_argument('--config', required=True, help='Node configuration file (JSON)')
    parser.add_argument('--db-config', help='Database configuration file (JSON)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would run without executing')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_node_config(args.config)
    if not config:
        sys.exit(1)
    
    print(f"Starting distributed processing for: {config.get('description', 'Unknown node')}")
    print(f"Node ID: {config.get('node_id', 'N/A')}")
    print(f"Endpoints to process: {len(config.get('endpoints', []))}")
    print("=" * 60)
    
    # Run the processor
    success = run_endpoint_processor(config, args.db_config, args.dry_run)
    
    if success:
        print("\n" + "=" * 60)
        print("DISTRIBUTED PROCESSING COMPLETED SUCCESSFULLY")
    else:
        print("\n" + "=" * 60)
        print("DISTRIBUTED PROCESSING FAILED")
        sys.exit(1)

if __name__ == "__main__":
    main()
