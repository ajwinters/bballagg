import json

# Load the endpoint configuration
with open('endpoint_priority_review.json', 'r') as f:
    config = json.load(f)

# Track changes
priority_updates = 0
usable_removals = 0

for endpoint_name, endpoint_info in config['endpoints'].items():
    # Update priority to 'None' for endpoints where usable = False
    if endpoint_info.get('usable') == False:
        endpoint_info['priority'] = 'None'
        priority_updates += 1
    
    # Remove 'usable' field from all endpoints
    if 'usable' in endpoint_info:
        del endpoint_info['usable']
        usable_removals += 1

print("=== ENDPOINT CONFIGURATION UPDATE ===")
print(f"Total endpoints processed: {len(config['endpoints'])}")
print()
print(f"� Updated priority to 'None' for {priority_updates} endpoints (previously usable = False)")
print(f"�️  Removed 'usable' field from {usable_removals} endpoints")

# Save the updated configuration back to the file
with open('endpoint_priority_review.json', 'w') as f:
    json.dump(config, f, indent=2)

print(f"\n✅ Updated endpoint_priority_review.json successfully")
