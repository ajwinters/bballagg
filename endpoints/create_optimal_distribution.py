#!/usr/bin/env python3
"""
NBA Endpoint Distribution - Simple Deployment Script

This script shows the OPTIMAL way to distribute NBA endpoint processing:
ONE ENDPOINT PER NODE

Each node gets its own IP address and processes one complete endpoint.
"""

import json
import os

# Define the key endpoints and their optimal node assignments
ENDPOINT_ASSIGNMENTS = [
    {
        "node_id": "node_1_boxscore_advanced",
        "endpoint": "BoxScoreAdvancedV3",
        "description": "Game advanced stats - highest volume",
        "rate_limit": 0.4,
        "priority": "high"
    },
    {
        "node_id": "node_2_four_factors", 
        "endpoint": "BoxScoreFourFactorsV3",
        "description": "Game four factors stats",
        "rate_limit": 0.4,
        "priority": "high"
    },
    {
        "node_id": "node_3_player_tracking",
        "endpoint": "BoxScorePlayerTrackV3", 
        "description": "Game player tracking stats",
        "rate_limit": 0.4,
        "priority": "high"
    },
    {
        "node_id": "node_4_matchups",
        "endpoint": "BoxScoreMatchupsV3",
        "description": "Game matchup data",
        "rate_limit": 0.5,
        "priority": "medium"
    },
    {
        "node_id": "node_5_misc",
        "endpoint": "BoxScoreMiscV3",
        "description": "Game miscellaneous stats", 
        "rate_limit": 0.5,
        "priority": "high"
    },
    {
        "node_id": "node_6_player_gamelogs",
        "endpoint": "PlayerGameLogs",
        "description": "Player game logs - high volume",
        "rate_limit": 0.3,
        "priority": "high"
    },
    {
        "node_id": "node_7_player_career",
        "endpoint": "PlayerCareerStats",
        "description": "Player career statistics",
        "rate_limit": 0.5,
        "priority": "medium"
    },
    {
        "node_id": "node_8_team_gamelogs", 
        "endpoint": "TeamGameLogs",
        "description": "Team game logs",
        "rate_limit": 0.5,
        "priority": "medium"
    }
]

def create_node_configs():
    """Create individual node configuration files"""
    config_dir = "config"
    
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    
    for assignment in ENDPOINT_ASSIGNMENTS:
        config = {
            "description": f"{assignment['node_id']}: {assignment['description']}",
            "node_id": assignment['node_id'],
            "endpoints": [assignment['endpoint']],
            "rate_limit": assignment['rate_limit'],
            "priority": assignment['priority'],
            "parameter_config": {
                "comment": f"Process ALL parameters for {assignment['endpoint']}. Each call returns multiple dataframes."
            }
        }
        
        filename = f"{config_dir}/{assignment['node_id']}_config.json"
        with open(filename, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"Created: {filename}")

def generate_deployment_commands():
    """Generate the deployment commands for each node"""
    
    print("\n" + "="*60)
    print("OPTIMAL DEPLOYMENT COMMANDS")
    print("="*60)
    print("Deploy each command on a server with a different IP address:")
    print()
    
    for i, assignment in enumerate(ENDPOINT_ASSIGNMENTS, 1):
        print(f"# Server {i} (IP: x.x.x.{i})")
        print(f"python collectors/endpoint_processor.py --endpoint {assignment['endpoint']} --rate-limit {assignment['rate_limit']} --node-id {assignment['node_id']}")
        print(f"# OR use config file:")
        print(f"python scripts/distributed_runner.py --config config/{assignment['node_id']}_config.json")
        print()
    
    print("="*60)
    print("BENEFITS OF THIS APPROACH:")
    print("- Each endpoint gets its own IP address") 
    print("- Maximum rate limit utilization")
    print("- Simple deployment (one command per server)")
    print("- No coordination needed between nodes")
    print("- Easy to monitor and troubleshoot")
    print("="*60)

def generate_batch_file():
    """Generate a Windows batch file for easy deployment"""
    
    batch_content = """@echo off
REM NBA Endpoint Processing - Optimal Distribution
REM Deploy each node on a server with different IP address

echo ===============================================
echo NBA ENDPOINT PROCESSING - OPTIMAL DISTRIBUTION  
echo ===============================================
echo Each node should run on a server with different IP

"""
    
    for i, assignment in enumerate(ENDPOINT_ASSIGNMENTS, 1):
        batch_content += f"""
echo.
echo Node {i}: {assignment['description']}
echo Command: python collectors/endpoint_processor.py --endpoint {assignment['endpoint']} --node-id {assignment['node_id']}
echo Config:  python scripts/distributed_runner.py --config config/{assignment['node_id']}_config.json
"""
    
    batch_content += """
echo.
echo ===============================================
echo Deploy each command above on different servers!
echo ===============================================
pause
"""
    
    with open("deploy_optimal.bat", 'w') as f:
        f.write(batch_content)
    
    print("Created: deploy_optimal.bat")

def main():
    print("NBA Endpoint Distribution - Optimal Deployment Generator")
    print("This creates configs and commands for ONE ENDPOINT PER NODE")
    print()
    
    # Create individual node configs
    create_node_configs()
    
    # Generate deployment commands
    generate_deployment_commands()
    
    # Generate batch file
    generate_batch_file()
    
    print("\nREADY FOR DEPLOYMENT!")
    print("Each node config processes one complete endpoint with all its parameters.")

if __name__ == "__main__":
    main()
