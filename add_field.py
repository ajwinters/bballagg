import json
import re

# Load the endpoint configuration
with open('endpoint_priority_review.json', 'r') as f:
    config = json.load(f)

def analyze_endpoint_versions():
    """Analyze endpoints to determine which are the latest versions"""
    
    # Group endpoints by base name (without version suffix)
    endpoint_families = {}
    
    for endpoint_name in config['endpoints'].keys():
        # Extract base name and version
        match = re.match(r'^(.+?)(V[1-3])?$', endpoint_name)
        if match:
            base_name = match.group(1)
            version = match.group(2) if match.group(2) else 'V0'  # V0 for no suffix
            
            if base_name not in endpoint_families:
                endpoint_families[base_name] = []
            endpoint_families[base_name].append((endpoint_name, version))
    
    # Determine latest version for each family
    latest_versions = {}
    for base_name, versions in endpoint_families.items():
        # Sort by version (V3 > V2 > V1 > V0)
        version_order = {'V0': 0, 'V1': 1, 'V2': 2, 'V3': 3}
        sorted_versions = sorted(versions, key=lambda x: version_order.get(x[1], 0), reverse=True)
        
        # The first one is the latest
        latest_endpoint = sorted_versions[0][0]
        latest_versions[latest_endpoint] = True
        
        # Mark others as not latest
        for endpoint_name, _ in sorted_versions[1:]:
            latest_versions[endpoint_name] = False
    
    return latest_versions

# Analyze versions (only if latest_version field doesn't exist)
latest_versions = analyze_endpoint_versions()

# Add fields to every endpoint
updated_count = 0
fields_added = []
skip_updates = 0
league_dash_updates = 0

# Define specific endpoints to update
league_dash_endpoints = [
    'LeagueDashOppPtShot',
    'LeagueDashPlayerBioStats',
    'LeagueDashPlayerShotLocations',
    'LeagueDashPtDefend',
    'LeagueDashPtStats',
    'LeagueDashPtTeamDefend',
    'LeagueDashTeamPtShot',
    'LeagueDashTeamShotLocations',
    'LeagueDashTeamStats'
]

for endpoint_name, endpoint_info in config['endpoints'].items():
    # Add latest_version field
    if 'latest_version' not in endpoint_info:
        endpoint_info['latest_version'] = latest_versions.get(endpoint_name, True)
        updated_count += 1
        if 'latest_version' not in fields_added:
            fields_added.append('latest_version')
    
    # Add policy field
    if 'policy' not in endpoint_info:
        endpoint_info['policy'] = ""  # Empty string for policy instructions
        updated_count += 1
        if 'policy' not in fields_added:
            fields_added.append('policy')
    
    # Add frequency field
    if 'frequency' not in endpoint_info:
        endpoint_info['frequency'] = ""  # Empty string for frequency instructions
        updated_count += 1
        if 'frequency' not in fields_added:
            fields_added.append('frequency')
    
    # Update endpoints with priority 'skip'
    if endpoint_info.get('priority') == 'skip':
        endpoint_info['usable'] = False
        endpoint_info['priority'] = ''
        skip_updates += 1
    
    # Update specific League Dash endpoints
    if endpoint_name in league_dash_endpoints:
        endpoint_info['frequency'] = 'daily'
        endpoint_info['priority'] = 'medium'
        endpoint_info['policy'] = 'replacing'
        league_dash_updates += 1

print(f"Added {len(fields_added)} field(s) to endpoints: {', '.join(fields_added)}")
print(f"Total field additions: {updated_count}")

if 'latest_version' in fields_added:
    # Show some examples of version analysis
    print("\nVersion analysis examples:")
    version_families = {}
    for endpoint_name, endpoint_info in config['endpoints'].items():
        match = re.match(r'^(.+?)(V[1-3])?$', endpoint_name)
        if match:
            base_name = match.group(1)
            if base_name not in version_families:
                version_families[base_name] = []
            version_families[base_name].append(endpoint_name)

    # Show families with multiple versions
    multi_version_families = {k: v for k, v in version_families.items() if len(v) > 1}
    print(f"Found {len(multi_version_families)} endpoint families with multiple versions:")

    for base_name, endpoints in sorted(list(multi_version_families.items())[:5]):  # Show first 5
        print(f"\n{base_name}:")
        for endpoint in sorted(endpoints):
            is_latest = config['endpoints'][endpoint]['latest_version']
            status = "✅ LATEST" if is_latest else "❌ OLD"
            print(f"  {endpoint}: {status}")

    if len(multi_version_families) > 5:
        print(f"\n... and {len(multi_version_families) - 5} more families")

if 'policy' in fields_added:
    print(f"\n📋 Added 'policy' field to all {len(config['endpoints'])} endpoints")
    print("Policy field initialized as empty string - ready for data collection instructions")

if 'frequency' in fields_added:
    print(f"\n📅 Added 'frequency' field to all {len(config['endpoints'])} endpoints")
    print("Frequency field initialized as empty string - ready for collection frequency instructions")
    print("Example values: 'daily', 'weekly', 'monthly', 'yearly', 'historical_once', 'manual'")

if skip_updates > 0:
    print(f"\n🚫 Updated {skip_updates} endpoints with priority 'skip':")
    print("   - Set usable = False")
    print("   - Set priority = '' (empty)")

if league_dash_updates > 0:
    print(f"\n📊 Updated {league_dash_updates} League Dash endpoints:")
    print("   - Set frequency = 'daily'")
    print("   - Set priority = 'medium'") 
    print("   - Set policy = 'replacing'")
    print("   Updated endpoints:")
    for endpoint in league_dash_endpoints:
        if endpoint in config['endpoints']:
            print(f"     • {endpoint}")

# Save the updated configuration back to the file
with open('endpoint_priority_review.json', 'w') as f:
    json.dump(config, f, indent=2)

print(f"\nUpdated endpoint_priority_review.json with new field(s)")
