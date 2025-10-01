#!/usr/bin/env python3
"""
Simple script to get endpoint lists for SLURM jobs
"""

import json
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)
os.chdir(project_root)

def list_all_endpoint_names(latest_only=True):
    """Get all endpoint names from endpoint_config.json, optionally filtering for latest versions only"""
    with open('config/endpoint_config.json', 'r') as f:
        config = json.load(f)
    
    if not latest_only:
        return list(config['endpoints'].keys())
    
    # Filter for latest versions only
    latest_endpoints = []
    for name, endpoint_config in config['endpoints'].items():
        if endpoint_config.get('latest_version', False):
            latest_endpoints.append(name)
    
    return latest_endpoints

def get_endpoints_by_priority(priority, latest_only=True):
    """Get endpoints by priority level, optionally filtering for latest versions only"""
    with open('config/endpoint_config.json', 'r') as f:
        config = json.load(f)
    
    endpoints = []
    for name, endpoint_config in config['endpoints'].items():
        # Check priority match
        if endpoint_config.get('priority') == priority:
            # If latest_only is True, only include endpoints marked as latest_version
            if not latest_only or endpoint_config.get('latest_version', False):
                endpoints.append({'endpoint': name})
    return endpoints

def get_endpoints_for_profile(profile_name):
    """Get endpoints for a given profile"""
    
    # Load run config (using project root path)
    config_path = os.path.join(project_root, 'config', 'run_config.json')
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
    
    # Check if we should include all versions or latest only
    latest_only = profile.get('latest_only', True)  # Default to latest only
    
    if filter_type == 'all':
        return list_all_endpoint_names(latest_only=latest_only)
    elif filter_type.startswith('priority:'):
        priority = filter_type.split(':')[1]
        return [ep['endpoint'] for ep in get_endpoints_by_priority(priority, latest_only=latest_only)]
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
