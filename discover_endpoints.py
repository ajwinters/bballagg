#!/usr/bin/env python3
"""
NBA API Endpoint Discovery Script
Systematically discovers and documents all available NBA API endpoints
"""

import sys
import os
import json
import inspect
from datetime import datetime

# Add src to path for our utilities
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

import nba_api.stats.endpoints as nbaapi

def discover_all_endpoints():
    """Discover all available NBA API endpoints"""
    print("🔍 Discovering NBA API Endpoints...")
    print("=" * 60)
    
    # Get all endpoint classes
    endpoint_classes = []
    for name in dir(nbaapi):
        obj = getattr(nbaapi, name)
        if inspect.isclass(obj) and hasattr(obj, 'get_data_frames'):
            endpoint_classes.append((name, obj))
    
    print(f"Found {len(endpoint_classes)} total endpoints")
    print()
    
    return endpoint_classes

def categorize_endpoints(endpoint_classes):
    """Categorize endpoints by likely data type"""
    categories = {
        'game_based': [],
        'player_based': [],
        'team_based': [],
        'league_based': [],
        'other': []
    }
    
    for name, cls in endpoint_classes:
        name_lower = name.lower()
        
        if any(keyword in name_lower for keyword in ['boxscore', 'playbyplay', 'game']):
            categories['game_based'].append(name)
        elif any(keyword in name_lower for keyword in ['player', 'person']):
            categories['player_based'].append(name)
        elif any(keyword in name_lower for keyword in ['team']):
            categories['team_based'].append(name)
        elif any(keyword in name_lower for keyword in ['league', 'draft', 'season']):
            categories['league_based'].append(name)
        else:
            categories['other'].append(name)
    
    return categories

def analyze_endpoint_parameters(endpoint_name, endpoint_class):
    """Analyze the parameters an endpoint accepts"""
    try:
        # Get the __init__ method signature
        init_signature = inspect.signature(endpoint_class.__init__)
        parameters = []
        
        for param_name, param in init_signature.parameters.items():
            if param_name == 'self':
                continue
                
            param_info = {
                'name': param_name,
                'required': param.default == inspect.Parameter.empty,
                'default': param.default if param.default != inspect.Parameter.empty else None
            }
            parameters.append(param_info)
        
        return parameters
        
    except Exception as e:
        return [{'error': str(e)}]

def test_endpoint_call(endpoint_name, endpoint_class):
    """Test calling an endpoint with default parameters to see data structure"""
    try:
        # Try to create instance with minimal parameters
        if 'player' in endpoint_name.lower():
            # Try with LeBron James ID
            instance = endpoint_class(player_id=2544)
        elif 'team' in endpoint_name.lower():
            # Try with Lakers ID  
            instance = endpoint_class(team_id=1610612747)
        elif 'game' in endpoint_name.lower() or 'boxscore' in endpoint_name.lower():
            # Try with a recent game ID
            instance = endpoint_class(game_id="0022400001")
        else:
            # Try with no parameters or defaults
            instance = endpoint_class()
        
        # Get data frames
        dataframes = instance.get_data_frames()
        
        df_info = []
        for i, df in enumerate(dataframes):
            if df is not None and not df.empty:
                df_info.append({
                    'index': i,
                    'shape': df.shape,
                    'columns': list(df.columns)[:10]  # First 10 columns
                })
        
        return {
            'success': True,
            'dataframes': df_info,
            'total_dataframes': len(dataframes)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

def generate_endpoint_report():
    """Generate a comprehensive endpoint discovery report"""
    print("🚀 NBA API Endpoint Discovery Report")
    print("Generated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 80)
    
    # Discover endpoints
    endpoint_classes = discover_all_endpoints()
    categories = categorize_endpoints(endpoint_classes)
    
    # Create comprehensive report
    report = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'total_endpoints': len(endpoint_classes),
            'categories': {cat: len(eps) for cat, eps in categories.items()}
        },
        'endpoints': {}
    }
    
    print("\n📊 CATEGORY BREAKDOWN:")
    for category, endpoints in categories.items():
        print(f"  {category.replace('_', ' ').title()}: {len(endpoints)} endpoints")
    
    print("\n🔍 DETAILED ANALYSIS:")
    print("-" * 80)
    
    for category, endpoint_names in categories.items():
        print(f"\n📁 {category.replace('_', ' ').title()} Endpoints ({len(endpoint_names)}):")
        
        for endpoint_name in sorted(endpoint_names):
            print(f"  🔸 {endpoint_name}")
            
            # Get the class
            endpoint_class = getattr(nbaapi, endpoint_name)
            
            # Analyze parameters
            parameters = analyze_endpoint_parameters(endpoint_name, endpoint_class)
            
            # Test endpoint (optional - can be slow)
            # test_result = test_endpoint_call(endpoint_name, endpoint_class)
            
            # Add to report
            report['endpoints'][endpoint_name] = {
                'category': category,
                'parameters': parameters,
                'class_name': endpoint_class.__name__,
                'module': endpoint_class.__module__
            }
            
            # Show key parameters
            required_params = [p['name'] for p in parameters if p.get('required', False)]
            if required_params:
                print(f"     Required params: {', '.join(required_params)}")
            
            optional_params = [p['name'] for p in parameters if not p.get('required', False)]
            if optional_params:
                print(f"     Optional params: {', '.join(optional_params[:5])}...")
    
    # Save report
    report_file = 'nba_endpoint_discovery_report.json'
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n💾 Full report saved to: {report_file}")
    
    return report

def suggest_high_priority_endpoints(categories):
    """Suggest which endpoints should be prioritized"""
    print("\n🎯 RECOMMENDED HIGH-PRIORITY ENDPOINTS:")
    print("-" * 50)
    
    high_priority = {
        'Game Stats': [
            'BoxScoreAdvancedV3', 'BoxScoreTraditionalV3', 'BoxScoreMiscV3',
            'BoxScoreFourFactorsV3', 'BoxScoreUsageV3', 'PlayByPlayV3'
        ],
        'Player Stats': [
            'PlayerDashboardByGeneralSplits', 'PlayerDashboardByShootingSplits',
            'PlayerGameLog', 'CommonPlayerInfo', 'PlayerDashboardByShotChartDetail'
        ],
        'Team Stats': [
            'TeamDashboardByGeneralSplits', 'TeamGameLog', 'CommonTeamRoster'
        ],
        'League Stats': [
            'LeagueDashPlayerStats', 'LeagueDashTeamStats', 'LeagueGameFinder'
        ]
    }
    
    for category, endpoints in high_priority.items():
        print(f"\n{category}:")
        for endpoint in endpoints:
            if any(endpoint in cat_endpoints for cat_endpoints in categories.values()):
                print(f"  ✅ {endpoint}")
            else:
                print(f"  ❓ {endpoint} (verify availability)")
    
    return high_priority

def main():
    """Main discovery function"""
    try:
        # Generate full report
        report = generate_endpoint_report()
        
        # Get categories for suggestions
        endpoint_classes = discover_all_endpoints()
        categories = categorize_endpoints(endpoint_classes)
        
        # Suggest priorities
        high_priority = suggest_high_priority_endpoints(categories)
        
        print("\n" + "=" * 80)
        print("✅ Endpoint discovery complete!")
        print("\nNext steps:")
        print("1. Review nba_endpoint_discovery_report.json")
        print("2. Select high-priority endpoints for configuration")
        print("3. Update config/endpoints_config.py with selected endpoints")
        print("4. Test endpoints with: ./nba_jobs.sh submit validation")
        
        return True
        
    except Exception as e:
        print(f"❌ Discovery failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
