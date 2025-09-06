#!/usr/bin/env python3
"""
Simple script to get endpoint lists for SLURM jobs
"""

import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.endpoints_config import list_all_endpoint_names, get_endpoints_by_priority

def get_endpoints_for_profile(profile_name):
    """Get endpoints for a given profile"""
    
    # Load run config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'run_config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    if profile_name not in config['collection_profiles']:
        print(f"Error: Profile '{profile_name}' not found", file=sys.stderr)
        sys.exit(1)
    
    profile = config['collection_profiles'][profile_name]
    
    # If specific endpoints are listed, use those
    if 'endpoints' in profile:
        return profile['endpoints']
    
    # Otherwise use filter
    filter_type = profile.get('filter', 'all')
    
    if filter_type == 'all':
        return list_all_endpoint_names()
    elif filter_type.startswith('priority:'):
        priority = filter_type.split(':')[1]
        return [ep['endpoint'] for ep in get_endpoints_by_priority(priority)]
    else:
        print(f"Error: Unknown filter type '{filter_type}'", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python get_endpoints.py <profile_name>")
        print("Available profiles: test, high_priority, full")
        sys.exit(1)
    
    profile = sys.argv[1]
    endpoints = get_endpoints_for_profile(profile)
    
    # Print each endpoint on a new line (for bash array creation)
    for endpoint in endpoints:
        print(endpoint)
