"""
NBA API Parameter Configuration Updater

This script reads the parameter discovery results and creates an updated 
endpoint_config.json file with the correct parameter names discovered from
the NBA API endpoint signatures.

Author: Your System
Date: 2025-09-21
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set


class ParameterConfigUpdater:
    """Updates endpoint configuration with discovered parameter names"""
    
    def __init__(self):
        self.setup_logging()
        self.parameter_mappings = self.create_standard_mappings()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def create_standard_mappings(self) -> Dict[str, str]:
        """Create mappings from API parameter names to our standard column names"""
        return {
            # League ID variants
            'league_id': 'league_id',
            'league_id_nullable': 'league_id',
            'leagueid': 'league_id',
            'leagueid_nullable': 'league_id',
            
            # Season variants
            'season': 'season',
            'season_nullable': 'season',
            'season_id': 'season',
            'season_id_nullable': 'season',
            'seasonid': 'season',
            'seasonid_nullable': 'season',
            
            # Season Type variants
            'season_type': 'season_type',
            'season_type_nullable': 'season_type',
            'seasontype': 'season_type',
            'seasontype_nullable': 'season_type',
            
            # Team ID variants
            'team_id': 'team_id',
            'team_id_nullable': 'team_id',
            'teamid': 'team_id',
            'teamid_nullable': 'team_id',
            
            # Player ID variants
            'player_id': 'player_id',
            'player_id_nullable': 'player_id',
            'playerid': 'player_id',
            'playerid_nullable': 'player_id',
            
            # Game ID variants
            'game_id': 'game_id',
            'game_id_nullable': 'game_id',
            'gameid': 'game_id',
            'gameid_nullable': 'game_id'
        }
    
    def load_current_config(self) -> Dict:
        """Load the current endpoint configuration"""
        config_path = Path(__file__).parent.parent / 'config' / 'endpoint_config.json'
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_discovery_results(self) -> Dict:
        """Load the parameter discovery results"""
        results_path = Path(__file__).parent.parent / 'parameter_analysis_report.json'
        with open(results_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def update_endpoint_parameters(self, current_config: Dict, discovery_results: Dict) -> Dict:
        """Update endpoint parameters based on discovery results"""
        updated_config = current_config.copy()
        variant_analysis = discovery_results.get('variant_analysis', {})
        updates_made = 0
        
        for endpoint_name, variants in variant_analysis.items():
            if endpoint_name not in updated_config['endpoints']:
                self.logger.warning(f"Endpoint {endpoint_name} not found in current config")
                continue
            
            current_params = updated_config['endpoints'][endpoint_name].get('required_params', [])
            
            # Convert current params to a set for easier manipulation
            if isinstance(current_params, list):
                current_param_set = set(current_params)
            else:
                # If it's a dict, convert to set of keys
                current_param_set = set(current_params.keys())
            
            updated_param_set = current_param_set.copy()
            
            # Process each discovered variant
            for variant in variants:
                api_param_name = variant['parameter']
                param_group = variant['group']
                is_required = variant['required']
                standard_name = self.parameter_mappings.get(api_param_name, api_param_name)
                
                # Check if we have any variants of this parameter in current config
                has_variant_in_config = False
                old_variants_to_remove = set()
                
                for current_param in current_param_set:
                    current_standard = self.parameter_mappings.get(current_param, current_param)
                    if current_standard == standard_name:
                        has_variant_in_config = True
                        # If it's not the exact API parameter name, mark for removal
                        if current_param != api_param_name:
                            old_variants_to_remove.add(current_param)
                
                # Remove old variants and add the correct API parameter name
                if has_variant_in_config:
                    updated_param_set -= old_variants_to_remove
                    updated_param_set.add(api_param_name)
                elif is_required:
                    # Add required parameters that aren't in config yet
                    updated_param_set.add(api_param_name)
            
            # Update the configuration if changes were made
            if updated_param_set != current_param_set:
                updated_config['endpoints'][endpoint_name]['required_params'] = sorted(list(updated_param_set))
                updates_made += 1
                
                self.logger.info(f"Updated {endpoint_name}:")
                self.logger.info(f"  Old: {sorted(current_param_set)}")
                self.logger.info(f"  New: {sorted(updated_param_set)}")
        
        self.logger.info(f"Total endpoints updated: {updates_made}")
        return updated_config
    
    def create_parameter_mapping_file(self) -> None:
        """Create a file with parameter mappings for the processor to use"""
        mapping_data = {
            "description": "Parameter mappings from API parameter names to standard database column names",
            "mappings": self.parameter_mappings,
            "variant_groups": {
                "league_id": ["league_id", "league_id_nullable", "leagueid", "leagueid_nullable"],
                "season": ["season", "season_nullable", "season_id", "season_id_nullable", "seasonid", "seasonid_nullable"],
                "season_type": ["season_type", "season_type_nullable", "seasontype", "seasontype_nullable"],
                "team_id": ["team_id", "team_id_nullable", "teamid", "teamid_nullable"],
                "player_id": ["player_id", "player_id_nullable", "playerid", "playerid_nullable"],
                "game_id": ["game_id", "game_id_nullable", "gameid", "gameid_nullable"]
            }
        }
        
        mapping_path = Path(__file__).parent.parent / 'config' / 'parameter_mappings.json'
        with open(mapping_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2)
        
        self.logger.info(f"Parameter mappings saved to {mapping_path}")
    
    def save_updated_config(self, updated_config: Dict) -> bool:
        """Save the updated configuration with backup"""
        config_path = Path(__file__).parent.parent / 'config' / 'endpoint_config.json'
        backup_path = config_path.with_suffix('.json.backup')
        
        try:
            # Create backup of original
            current_config = self.load_current_config()
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(current_config, f, indent=2)
            
            # Save updated config
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(updated_config, f, indent=2)
            
            self.logger.info(f"Updated configuration saved to {config_path}")
            self.logger.info(f"Backup saved to {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save updated configuration: {e}")
            return False
    
    def run_update(self) -> bool:
        """Run the complete parameter update process"""
        self.logger.info("Starting parameter configuration update...")
        
        try:
            # Load current configuration and discovery results
            current_config = self.load_current_config()
            discovery_results = self.load_discovery_results()
            
            # Update parameters
            updated_config = self.update_endpoint_parameters(current_config, discovery_results)
            
            # Create parameter mapping file
            self.create_parameter_mapping_file()
            
            # Save updated configuration
            success = self.save_updated_config(updated_config)
            
            if success:
                self.logger.info("Parameter configuration update completed successfully!")
            else:
                self.logger.error("Parameter configuration update failed!")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error during parameter update: {e}")
            return False
    
    def print_summary(self) -> None:
        """Print a summary of parameter standardization"""
        print("\n" + "="*70)
        print("NBA API PARAMETER STANDARDIZATION SUMMARY")
        print("="*70)
        print("Standard parameter names for database columns:")
        print()
        
        groups = {
            "League ID": ["league_id", "league_id_nullable", "leagueid", "leagueid_nullable"],
            "Season": ["season", "season_nullable", "season_id", "season_id_nullable", "seasonid", "seasonid_nullable"],
            "Season Type": ["season_type", "season_type_nullable", "seasontype", "seasontype_nullable"],
            "Team ID": ["team_id", "team_id_nullable", "teamid", "teamid_nullable"],
            "Player ID": ["player_id", "player_id_nullable", "playerid", "playerid_nullable"],
            "Game ID": ["game_id", "game_id_nullable", "gameid", "gameid_nullable"]
        }
        
        for group_name, variants in groups.items():
            standard = variants[0].replace('_nullable', '').replace('id', '_id') if 'id' in variants[0] else variants[0].replace('_nullable', '')
            print(f"{group_name:12} -> {standard:12} (API variants: {', '.join(variants)})")
        
        print("\nThis ensures consistent column naming in database tables!")
        print("="*70)


def main():
    """Main function to run parameter configuration update"""
    updater = ParameterConfigUpdater()
    
    # Print summary first
    updater.print_summary()
    
    # Ask user for confirmation
    print(f"\nThis will update your endpoint_config.json file.")
    response = input("Do you want to proceed? (y/N): ").lower()
    
    if response == 'y':
        success = updater.run_update()
        if success:
            print("✅ Configuration updated successfully!")
            print("📋 Parameter mappings created in config/parameter_mappings.json")
            print("🔄 You can now run your NBA data processor with standardized parameters")
        else:
            print("❌ Configuration update failed!")
    else:
        print("Configuration update cancelled.")
    
    return updater


if __name__ == "__main__":
    main()