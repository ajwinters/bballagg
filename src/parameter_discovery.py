"""
NBA API Parameter Discovery System

This script discovers the correct parameter names for all NBA API endpoints
by testing different parameter name variants (e.g., league_id vs league_id_nullable)
and creates a standardized mapping for consistent database column naming.

Author: Your System
Date: 2025-09-21
"""

import json
import logging
import inspect
from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import importlib

# NBA API imports
from nba_api.stats.endpoints import *


@dataclass
class ParameterVariant:
    """Represents a parameter name variant and its standardized form"""
    actual_name: str  # What the API actually accepts
    standard_name: str  # What we'll use in our database columns
    variant_group: str  # Group of related variants (e.g., 'league_id')


class ParameterDiscovery:
    """Discovers correct parameter names for all NBA API endpoints"""
    
    def __init__(self):
        self.setup_logging()
        self.endpoint_config = self.load_endpoint_config()
        self.parameter_mappings = {}
        
        # Define parameter variant groups
        self.parameter_variants = {
            'league_id': [
                'league_id',
                'league_id_nullable', 
                'leagueid',
                'leagueid_nullable'
            ],
            'season': [
                'season',
                'season_nullable',
                'season_id',
                'season_id_nullable',
                'seasonid',
                'seasonid_nullable'
            ],
            'season_type': [
                'season_type',
                'season_type_nullable',
                'seasontype',
                'seasontype_nullable'
            ],
            'team_id': [
                'team_id',
                'team_id_nullable',
                'teamid',
                'teamid_nullable'
            ],
            'player_id': [
                'player_id',
                'player_id_nullable',
                'playerid',
                'playerid_nullable'
            ],
            'game_id': [
                'game_id',
                'game_id_nullable',
                'gameid',
                'gameid_nullable'
            ]
        }
        
        # Standard names for database columns (without nullable)
        self.standard_names = {
            'league_id': 'league_id',
            'season': 'season',
            'season_type': 'season_type',
            'team_id': 'team_id',
            'player_id': 'player_id',
            'game_id': 'game_id'
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('parameter_discovery.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def load_endpoint_config(self) -> Dict:
        """Load the endpoint configuration file"""
        config_path = Path(__file__).parent.parent / 'config' / 'endpoint_config.json'
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Extract just the endpoints dictionary
                return config.get('endpoints', {})
        except Exception as e:
            self.logger.error(f"Failed to load endpoint config: {e}")
            return {}
    
    def get_endpoint_class(self, endpoint_name: str):
        """Dynamically import and get the endpoint class"""
        try:
            # Import from nba_api.stats.endpoints
            module = importlib.import_module('nba_api.stats.endpoints')
            endpoint_class = getattr(module, endpoint_name)
            return endpoint_class
        except (ImportError, AttributeError) as e:
            self.logger.warning(f"Could not import endpoint {endpoint_name}: {e}")
            return None
    
    def analyze_endpoint_signature(self, endpoint_name: str) -> Dict[str, Any]:
        """Analyze the parameter signature of an endpoint"""
        endpoint_class = self.get_endpoint_class(endpoint_name)
        if not endpoint_class:
            return {}
        
        try:
            # Get the __init__ method signature
            signature = inspect.signature(endpoint_class.__init__)
            parameters = {}
            
            for param_name, param in signature.parameters.items():
                if param_name == 'self':
                    continue
                
                param_info = {
                    'name': param_name,
                    'default': param.default if param.default != inspect.Parameter.empty else None,
                    'annotation': str(param.annotation) if param.annotation != inspect.Parameter.empty else None,
                    'required': param.default == inspect.Parameter.empty
                }
                
                # Determine which variant group this parameter belongs to
                variant_group = self.identify_variant_group(param_name)
                if variant_group:
                    param_info['variant_group'] = variant_group
                    param_info['standard_name'] = self.standard_names[variant_group]
                
                parameters[param_name] = param_info
            
            return parameters
            
        except Exception as e:
            self.logger.error(f"Error analyzing signature for {endpoint_name}: {e}")
            return {}
    
    def identify_variant_group(self, parameter_name: str) -> Optional[str]:
        """Identify which variant group a parameter belongs to"""
        param_lower = parameter_name.lower()
        
        for group_name, variants in self.parameter_variants.items():
            if param_lower in [v.lower() for v in variants]:
                return group_name
        
        return None
    
    def discover_all_parameters(self) -> Dict[str, Dict]:
        """Discover parameters for all endpoints in the configuration"""
        self.logger.info("Starting parameter discovery for all endpoints...")
        
        discovered_parameters = {}
        total_endpoints = len(self.endpoint_config)
        
        for i, (endpoint_name, config) in enumerate(self.endpoint_config.items(), 1):
            self.logger.info(f"Analyzing {endpoint_name} ({i}/{total_endpoints})")
            
            parameters = self.analyze_endpoint_signature(endpoint_name)
            if parameters:
                discovered_parameters[endpoint_name] = parameters
            
            # Log progress every 10 endpoints
            if i % 10 == 0:
                self.logger.info(f"Progress: {i}/{total_endpoints} endpoints analyzed")
        
        self.logger.info(f"Parameter discovery complete. Analyzed {len(discovered_parameters)} endpoints.")
        return discovered_parameters
    
    def generate_parameter_report(self, discovered_parameters: Dict) -> Dict:
        """Generate a comprehensive report of parameter findings"""
        report = {
            'summary': {
                'total_endpoints': len(discovered_parameters),
                'endpoints_with_variants': 0,
                'variant_groups_found': set(),
                'parameter_statistics': {}
            },
            'variant_analysis': {},
            'endpoint_details': discovered_parameters,
            'recommended_updates': {}
        }
        
        # Analyze variant usage
        for endpoint_name, parameters in discovered_parameters.items():
            endpoint_variants = []
            
            for param_name, param_info in parameters.items():
                if 'variant_group' in param_info:
                    endpoint_variants.append({
                        'parameter': param_name,
                        'group': param_info['variant_group'],
                        'standard_name': param_info['standard_name'],
                        'required': param_info['required']
                    })
                    report['summary']['variant_groups_found'].add(param_info['variant_group'])
            
            if endpoint_variants:
                report['summary']['endpoints_with_variants'] += 1
                report['variant_analysis'][endpoint_name] = endpoint_variants
        
        # Convert set to list for JSON serialization
        report['summary']['variant_groups_found'] = list(report['summary']['variant_groups_found'])
        
        # Generate recommended updates for endpoint_config.json
        for endpoint_name, variants in report['variant_analysis'].items():
            current_config = self.endpoint_config.get(endpoint_name, {})
            current_params = current_config.get('required_params', [])
            
            # Convert list to dict if needed for comparison
            if isinstance(current_params, list):
                current_params_dict = {param: "" for param in current_params}
            else:
                current_params_dict = current_params
            
            recommended_params = current_params_dict.copy()
            
            for variant in variants:
                group = variant['group']
                actual_param = variant['parameter']
                standard_name = variant['standard_name']
                
                # Check if we should update this parameter
                found_old_variant = False
                for old_param in list(recommended_params.keys()):
                    old_group = self.identify_variant_group(old_param)
                    if old_group == group:
                        # Replace old variant with actual parameter name
                        value = recommended_params.pop(old_param)
                        recommended_params[actual_param] = value
                        found_old_variant = True
                        break
                
                # If it's a required parameter and not found, add it
                if variant['required'] and not found_old_variant and actual_param not in recommended_params:
                    recommended_params[actual_param] = ""
            
            if recommended_params != current_params_dict:
                report['recommended_updates'][endpoint_name] = {
                    'current_parameters': current_params,
                    'recommended_parameters': recommended_params
                }
        
        return report
    
    def save_discovery_results(self, discovered_parameters: Dict, report: Dict):
        """Save the discovery results to files"""
        # Save raw parameter data
        parameters_file = Path(__file__).parent.parent / 'parameter_discovery_results.json'
        with open(parameters_file, 'w', encoding='utf-8') as f:
            json.dump(discovered_parameters, f, indent=2, default=str)
        
        # Save analysis report
        report_file = Path(__file__).parent.parent / 'parameter_analysis_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Results saved to {parameters_file} and {report_file}")
    
    def update_endpoint_config(self, report: Dict) -> bool:
        """Update the endpoint_config.json file with discovered parameter names"""
        if not report.get('recommended_updates'):
            self.logger.info("No updates needed for endpoint configuration")
            return True

        # Load the full config structure
        config_path = Path(__file__).parent.parent / 'config' / 'endpoint_config.json'
        with open(config_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)
        
        updated_endpoints = full_config['endpoints'].copy()
        updates_applied = 0
        
        for endpoint_name, updates in report['recommended_updates'].items():
            if endpoint_name in updated_endpoints:
                updated_endpoints[endpoint_name]['required_params'] = updates['recommended_parameters']
                updates_applied += 1
                self.logger.info(f"Updated parameters for {endpoint_name}")
        
        # Update the full config structure
        full_config['endpoints'] = updated_endpoints
        
        # Backup original config
        backup_path = config_path.with_suffix('.json.backup')
        
        try:
            # Create backup
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(full_config, f, indent=2)
            
            # Save updated config
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(full_config, f, indent=2)
            
            self.logger.info(f"Updated endpoint configuration with {updates_applied} changes")
            self.logger.info(f"Backup saved to {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update endpoint configuration: {e}")
            return False
    
    def run_discovery(self, update_config: bool = False) -> Dict:
        """Run the complete parameter discovery process"""
        self.logger.info("Starting NBA API parameter discovery...")
        
        # Discover parameters for all endpoints
        discovered_parameters = self.discover_all_parameters()
        
        # Generate analysis report
        report = self.generate_parameter_report(discovered_parameters)
        
        # Save results
        self.save_discovery_results(discovered_parameters, report)
        
        # Print summary
        self.print_summary(report)
        
        # Optionally update configuration
        if update_config:
            self.update_endpoint_config(report)
        
        return report
    
    def print_summary(self, report: Dict):
        """Print a summary of the discovery results"""
        summary = report['summary']
        
        print("\n" + "="*60)
        print("NBA API PARAMETER DISCOVERY SUMMARY")
        print("="*60)
        print(f"Total Endpoints Analyzed: {summary['total_endpoints']}")
        print(f"Endpoints with Parameter Variants: {summary['endpoints_with_variants']}")
        print(f"Variant Groups Found: {', '.join(summary['variant_groups_found'])}")
        print(f"Recommended Updates: {len(report.get('recommended_updates', {}))}")
        
        if report.get('recommended_updates'):
            print("\nEndpoints requiring parameter updates:")
            for endpoint_name in report['recommended_updates'].keys():
                print(f"  - {endpoint_name}")
        
        print("="*60)


def main():
    """Main function to run parameter discovery"""
    discovery = ParameterDiscovery()
    
    # Run discovery without updating config initially
    report = discovery.run_discovery(update_config=False)
    
    # Ask user if they want to update the configuration
    if report.get('recommended_updates'):
        print(f"\nFound {len(report['recommended_updates'])} endpoints that need parameter updates.")
        response = input("Do you want to update the endpoint_config.json file? (y/N): ").lower()
        
        if response == 'y':
            discovery.update_endpoint_config(report)
            print("Configuration updated successfully!")
        else:
            print("Configuration not updated. You can review the results in parameter_analysis_report.json")
    
    return report


if __name__ == "__main__":
    main()