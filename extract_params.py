#!/usr/bin/env python3
"""
Script to extract required parameters from NBA endpoint discovery report
and add them to the endpoint priority review file.
"""

import json
import os

def extract_required_params():
    """Extract required parameters from discovery report and add to priority review."""
    
    # Load the detailed discovery report
    try:
        with open('nba_endpoint_discovery_report.json', 'r') as f:
            discovery_data = json.load(f)
    except FileNotFoundError:
        print("❌ Discovery report not found!")
        return
    
    # Load the priority review file
    try:
        with open('endpoint_priority_review.json', 'r') as f:
            priority_data = json.load(f)
    except FileNotFoundError:
        print("❌ Priority review file not found!")
        return
    
    print("🔍 Extracting required parameters for each endpoint...")
    
    updated_count = 0
    
    # Process each endpoint in the priority review
    for endpoint_name in priority_data['endpoints']:
        if endpoint_name in discovery_data['endpoints']:
            endpoint_info = discovery_data['endpoints'][endpoint_name]
            
            # Extract required parameters
            required_params = []
            if 'parameters' in endpoint_info:
                for param in endpoint_info['parameters']:
                    if isinstance(param, dict) and param.get('required') == True:
                        param_name = param.get('name', 'unknown_param')
                        required_params.append(param_name)
            
            # Add required_params to the priority data
            priority_data['endpoints'][endpoint_name]['required_params'] = required_params
            updated_count += 1
            
            if required_params:
                print(f"✅ {endpoint_name}: {required_params}")
            else:
                print(f"⚪ {endpoint_name}: No required parameters")
        else:
            print(f"⚠️  {endpoint_name}: Not found in discovery report")
    
    # Save the updated priority file
    try:
        with open('endpoint_priority_review.json', 'w') as f:
            json.dump(priority_data, f, indent=2)
        print(f"\n✅ Updated {updated_count} endpoints with required parameters")
        print("📁 Saved to: endpoint_priority_review.json")
    except Exception as e:
        print(f"❌ Error saving file: {e}")

if __name__ == "__main__":
    extract_required_params()
