#!/usr/bin/env python3
"""
Endpoint Version Analysis Tool
Shows version conflicts and validates latest_version filtering
"""

import json
import sys
import os

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(script_dir))
sys.path.append(project_root)
os.chdir(project_root)

def analyze_endpoint_versions():
    """Analyze endpoint versions and show filtering results"""
    
    with open('config/endpoint_config.json', 'r') as f:
        config = json.load(f)
    
    print("🔍 NBA ENDPOINT VERSION ANALYSIS")
    print("=" * 50)
    
    # Group endpoints by base name
    endpoint_groups = {}
    for name, cfg in config['endpoints'].items():
        # Remove version suffixes to group endpoints
        base_name = name
        for suffix in ['V2', 'V3', 'V4']:
            if base_name.endswith(suffix):
                base_name = base_name.replace(suffix, '')
                break
        
        if base_name not in endpoint_groups:
            endpoint_groups[base_name] = []
        endpoint_groups[base_name].append({
            'name': name,
            'latest': cfg.get('latest_version', False),
            'priority': cfg.get('priority', 'medium')
        })
    
    # Find version conflicts
    conflicts = []
    for base_name, versions in endpoint_groups.items():
        if len(versions) > 1:
            latest_count = sum(1 for v in versions if v['latest'])
            if latest_count != 1:  # Should have exactly 1 latest version
                conflicts.append({
                    'base_name': base_name,
                    'versions': versions,
                    'latest_count': latest_count
                })
    
    if conflicts:
        print("\n⚠️  VERSION CONFLICTS DETECTED:")
        print("-" * 30)
        for conflict in conflicts:
            print(f"\n{conflict['base_name']} ({conflict['latest_count']} marked as latest):")
            for version in conflict['versions']:
                status = "✅ LATEST" if version['latest'] else "❌ OLD"
                print(f"  {version['name']} - {status} ({version['priority']} priority)")
    else:
        print("\n✅ NO VERSION CONFLICTS - All endpoint families have exactly 1 latest version")
    
    # Show filtering impact by priority
    priorities = ['high', 'medium', 'low']
    
    print(f"\n📊 FILTERING IMPACT BY PRIORITY:")
    print("-" * 35)
    
    for priority in priorities:
        all_endpoints = [name for name, cfg in config['endpoints'].items() 
                        if cfg.get('priority') == priority]
        latest_endpoints = [name for name, cfg in config['endpoints'].items() 
                           if cfg.get('priority') == priority and cfg.get('latest_version', False)]
        
        if all_endpoints:
            reduction = len(all_endpoints) - len(latest_endpoints)
            pct = (reduction / len(all_endpoints) * 100) if len(all_endpoints) > 0 else 0
            print(f"{priority.upper()} Priority:")
            print(f"  Total: {len(all_endpoints)} → Latest Only: {len(latest_endpoints)}")
            print(f"  Reduction: {reduction} endpoints ({pct:.1f}%)")
            print()
    
    return conflicts

def show_removed_endpoints():
    """Show which endpoints are filtered out"""
    
    with open('config/endpoint_config.json', 'r') as f:
        config = json.load(f)
    
    print("🗑️  ENDPOINTS FILTERED OUT (Old Versions):")
    print("-" * 45)
    
    removed_by_priority = {}
    for name, cfg in config['endpoints'].items():
        if not cfg.get('latest_version', False):
            priority = cfg.get('priority', 'medium')
            if priority not in removed_by_priority:
                removed_by_priority[priority] = []
            removed_by_priority[priority].append(name)
    
    for priority in ['high', 'medium', 'low']:
        if priority in removed_by_priority:
            print(f"\n{priority.upper()} Priority ({len(removed_by_priority[priority])} removed):")
            for endpoint in sorted(removed_by_priority[priority]):
                print(f"  ❌ {endpoint}")

if __name__ == '__main__':
    conflicts = analyze_endpoint_versions()
    print()
    show_removed_endpoints()
    
    if conflicts:
        print(f"\n⚠️  Found {len(conflicts)} version conflicts that need attention!")
        sys.exit(1)
    else:
        print(f"\n✅ Version filtering is properly configured!")
        sys.exit(0)