#!/usr/bin/env python3
"""
NBA Endpoint Processor - Distribution Test

This script demonstrates and tests the distributed processing capabilities.
It shows how different nodes can process different endpoints or parameter ranges.
"""

import subprocess
import sys
import os
import time

def run_test_command(description, command, dry_run=True):
    """Run a test command and show the results"""
    print(f"\n{'='*60}")
    print(f"TEST: {description}")
    print(f"{'='*60}")
    
    if dry_run:
        command.append("--dry-run")
    
    print(f"Command: {' '.join(command)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Return code: {result.returncode}")
    except subprocess.TimeoutExpired:
        print("Command timed out (normal for dry-run)")
    except Exception as e:
        print(f"Error running command: {e}")

def main():
    print("NBA Endpoint Processor - Distribution Testing")
    print("This demonstrates how endpoints can be distributed across nodes")
    
    # Change to the endpoints directory
    try:
        os.chdir("collectors")
        print(f"Changed to directory: {os.getcwd()}")
    except FileNotFoundError:
        print("Run this script from the endpoints folder")
        sys.exit(1)
    
    # Test 1: Single endpoint processing
    run_test_command(
        "Single Endpoint Processing",
        ["python", "endpoint_processor.py", "--endpoint", "BoxScoreAdvancedV3", "--param-limit", "5", "--node-id", "test_node_1"]
    )
    
    # Test 2: Multiple endpoints processing
    run_test_command(
        "Multiple Endpoints Processing", 
        ["python", "endpoint_processor.py", "--endpoints", "BoxScoreAdvancedV3", "BoxScoreFourFactorsV3", "--param-limit", "3", "--node-id", "test_node_2"]
    )
    
    # Test 3: Parameter range processing
    run_test_command(
        "Parameter Range Processing",
        ["python", "endpoint_processor.py", "--endpoint", "PlayerGameLogs", "--param-start", "0", "--param-end", "10", "--node-id", "test_node_3"]
    )
    
    # Test 4: Category-based processing
    run_test_command(
        "Category-Based Processing",
        ["python", "endpoint_processor.py", "--category", "game_based", "--priority", "high", "--param-limit", "2", "--node-id", "test_node_4"]
    )
    
    # Test 5: Configuration file processing
    run_test_command(
        "Configuration File Processing",
        ["python", "../scripts/distributed_runner.py", "--config", "../config/node1_config.json"]
    )
    
    print(f"\n{'='*60}")
    print("DISTRIBUTION TESTING COMPLETE")
    print(f"{'='*60}")
    print("\nKey Points Demonstrated:")
    print("1. Each endpoint processes ALL its dataframes in one call")
    print("2. Parameter ranges can split work across nodes (different game_ids, player_ids)")
    print("3. Different nodes can process different endpoints simultaneously")
    print("4. Each node can have different IP addresses to avoid rate limits")
    print("5. Configuration files make deployment across nodes easy")
    
    print(f"\nNext Steps for Production:")
    print("1. Deploy this code to servers with different IP addresses")
    print("2. Use different parameter ranges or endpoints per server")
    print("3. Run with --dry-run first to verify configuration")
    print("4. Remove --dry-run for actual data processing")

if __name__ == "__main__":
    main()
