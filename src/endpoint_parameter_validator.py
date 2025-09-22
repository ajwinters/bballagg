"""
NBA API Endpoint Parameter Validation System

This script tests all endpoints with their discovered parameter names to ensure
the new required_params configuration works correctly. It validates that:
- League ID variants (league_id vs league_id_nullable) work
- Season parameters are handled properly  
- Master table dependencies are resolved
- All parameter combinations produce successful API calls

Author: Your System
Date: 2025-09-21
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
import importlib
import inspect
import pandas as pd

# Add project root to path for imports
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

# NBA API imports
from nba_api.stats.endpoints import *
from src.rds_connection_manager import RDSConnectionManager


class EndpointParameterValidator:
    """Comprehensive validation system for NBA API endpoints with new parameter names"""
    
    def __init__(self, league: str = "NBA", test_mode: bool = True):
        self.league = league
        self.test_mode = test_mode
        self.setup_logging()
        
        # Load configurations
        self.endpoint_config = self._load_endpoint_config()
        self.league_config = self._load_league_config()
        self.parameter_mappings = self._load_parameter_mappings()
        
        # Initialize database connection
        self.db_config = self._load_database_config()
        self.db_manager = RDSConnectionManager(self.db_config)
        
        # Master table data cache
        self.master_data_cache = {}
        
        # Results tracking
        self.test_results = {
            'successful': [],
            'failed': [],
            'skipped': [],
            'parameter_issues': [],
            'total_tested': 0
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory
        logs_dir = project_root / 'logs'
        logs_dir.mkdir(exist_ok=True)
        
        # Create log filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = logs_dir / f'endpoint_validation_{timestamp}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Log file: {log_filename}")
    
    def _load_endpoint_config(self) -> Dict:
        """Load endpoint configuration"""
        config_path = project_root / 'config' / 'endpoint_config.json'
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        self.logger.info(f"Loaded {len(config['endpoints'])} endpoint configurations")
        return config
    
    def _load_league_config(self) -> Dict:
        """Load league configuration"""
        config_path = project_root / 'config' / 'leagues_config.json'
        with open(config_path, 'r', encoding='utf-8') as f:
            leagues = json.load(f)
        
        # Find our league
        for league in leagues:
            if league['name'].upper() == self.league:
                self.logger.info(f"Loaded league config for {league['full_name']}")
                return league
        
        raise ValueError(f"League {self.league} not found in configuration")
    
    def _load_database_config(self) -> Dict:
        """Load database configuration"""
        config_path = project_root / 'config' / 'database_config.json'
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return {
            'host': config['host'],
            'database': config['name'],
            'user': config['user'],
            'password': config['password'],
            'port': int(config['port']),
            'sslmode': config.get('ssl_mode', 'require'),
            'connect_timeout': 60
        }
    
    def _load_parameter_mappings(self) -> Dict:
        """Load parameter mappings"""
        config_path = project_root / 'config' / 'parameter_mappings.json'
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning("Parameter mappings file not found")
            return {"mappings": {}, "variant_groups": {}}
    
    def get_current_season(self) -> str:
        """Get current season in the format expected by the league"""
        current_date = datetime.now()
        current_year = current_date.year
        
        if self.league_config['season_format'] == 'two_year':
            # NBA/G-League: 2024-25 format
            if current_date.month >= 10:  # Season starts in October
                season = f"{current_year}-{str(current_year + 1)[-2:]}"
            else:
                season = f"{current_year - 1}-{str(current_year)[-2:]}"
        else:
            # WNBA: 2024 format
            if current_date.month >= 5:  # WNBA season roughly May-Oct
                season = str(current_year)
            else:
                season = str(current_year - 1)
        
        return season
    
    def load_master_table_data(self) -> None:
        """Load data from master tables for parameter resolution"""
        self.logger.info("Loading master table data for parameter resolution...")
        
        master_tables = {
            'game_id': 'nba_leaguegamefinder_leaguegamefinderresults',
            'player_id': 'nba_commonallplayers_commonallplayers'
        }
        
        for id_type, table_name in master_tables.items():
            try:
                with self.db_manager.get_cursor() as cursor:
                    # Get sample IDs (limited for testing)
                    limit = 5 if self.test_mode else 100
                    cursor.execute(f"""
                        SELECT DISTINCT {id_type}
                        FROM {table_name}
                        WHERE {id_type} IS NOT NULL
                        ORDER BY {id_type}
                        LIMIT {limit}
                    """)
                    
                    ids = [row[0] for row in cursor.fetchall()]
                    self.master_data_cache[id_type] = ids
                    self.logger.info(f"Loaded {len(ids)} {id_type} values from {table_name}")
                    
            except Exception as e:
                self.logger.warning(f"Could not load {id_type} from {table_name}: {e}")
                # Provide fallback values
                if id_type == 'game_id':
                    self.master_data_cache[id_type] = ['0022400001', '0022400002']
                elif id_type == 'player_id':
                    self.master_data_cache[id_type] = ['2544', '1628369']  # LeBron, Luka
    
    def resolve_parameter_values(self, endpoint_name: str, required_params: List[str]) -> Dict[str, Any]:
        """Resolve parameter values for an endpoint"""
        current_season = self.get_current_season()
        league_id = self.league_config['id']
        
        param_values = {}
        
        for param in required_params:
            param_lower = param.lower()
            
            # League ID variants
            if 'league' in param_lower and 'id' in param_lower:
                param_values[param] = league_id
            
            # Season variants  
            elif 'season' in param_lower and 'type' not in param_lower:
                param_values[param] = current_season
            
            # Season type variants
            elif 'season' in param_lower and 'type' in param_lower:
                param_values[param] = "Regular Season"
            
            # Game ID variants
            elif 'game' in param_lower and 'id' in param_lower:
                if 'game_id' in self.master_data_cache and self.master_data_cache['game_id']:
                    param_values[param] = self.master_data_cache['game_id'][0]
                else:
                    param_values[param] = '0022400001'  # Fallback
            
            # Player ID variants
            elif 'player' in param_lower and 'id' in param_lower and 'list' not in param_lower:
                if 'player_id' in self.master_data_cache and self.master_data_cache['player_id']:
                    param_values[param] = self.master_data_cache['player_id'][0]
                else:
                    param_values[param] = '2544'  # LeBron as fallback
            
            # Team ID variants
            elif 'team' in param_lower and 'id' in param_lower:
                param_values[param] = '1610612747'  # Lakers as example
            
            # Special cases
            elif param == 'minutes_min':
                param_values[param] = 5
            elif param == 'college':
                param_values[param] = 'Duke'
            elif 'list' in param_lower:
                # For player_id_list, vs_player_id_list etc.
                if 'player_id' in self.master_data_cache and self.master_data_cache['player_id']:
                    param_values[param] = self.master_data_cache['player_id'][:2]
                else:
                    param_values[param] = ['2544', '1628369']
            elif 'game_ids' in param:
                if 'game_id' in self.master_data_cache and self.master_data_cache['game_id']:
                    param_values[param] = self.master_data_cache['game_id'][:3]
                else:
                    param_values[param] = ['0022400001', '0022400002']
            else:
                # Generic fallback
                param_values[param] = ''
        
        return param_values
    
    def get_endpoint_class(self, endpoint_name: str):
        """Get the NBA API endpoint class"""
        try:
            module = importlib.import_module('nba_api.stats.endpoints')
            return getattr(module, endpoint_name)
        except (ImportError, AttributeError) as e:
            self.logger.warning(f"Could not import endpoint {endpoint_name}: {e}")
            return None
    
    def test_endpoint(self, endpoint_name: str, config: Dict) -> Dict[str, Any]:
        """Test a single endpoint with its configured parameters"""
        result = {
            'endpoint': endpoint_name,
            'status': 'unknown',
            'error': None,
            'parameters_used': {},
            'response_size': 0,
            'execution_time': 0
        }
        
        try:
            # Get endpoint class
            endpoint_class = self.get_endpoint_class(endpoint_name)
            if not endpoint_class:
                result['status'] = 'skipped'
                result['error'] = 'Could not import endpoint class'
                return result
            
            # Get required parameters
            required_params = config.get('required_params', [])
            if not required_params:
                self.logger.info(f"  No required params for {endpoint_name}")
                required_params = []
            
            # Resolve parameter values
            param_values = self.resolve_parameter_values(endpoint_name, required_params)
            result['parameters_used'] = param_values
            
            self.logger.info(f"  Testing {endpoint_name} with params: {param_values}")
            
            # Make API call
            start_time = time.time()
            
            if param_values:
                endpoint_instance = endpoint_class(**param_values)
            else:
                endpoint_instance = endpoint_class()
            
            # Get the data (this triggers the API call)
            dataframes = endpoint_instance.get_data_frames()
            
            end_time = time.time()
            result['execution_time'] = round(end_time - start_time, 2)
            
            # Count response size
            total_rows = sum(len(df) for df in dataframes) if dataframes else 0
            result['response_size'] = total_rows
            result['status'] = 'success'
            
            self.logger.info(f"  ✅ SUCCESS: {endpoint_name} - {total_rows} total rows in {result['execution_time']}s")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            self.logger.error(f"  ❌ FAILED: {endpoint_name} - {e}")
        
        return result
    
    def test_endpoints_by_category(self, category_filter: Optional[str] = None, 
                                 priority_filter: Optional[str] = None,
                                 max_endpoints: Optional[int] = None) -> None:
        """Test endpoints with optional filtering"""
        
        endpoints_to_test = []
        
        for endpoint_name, config in self.endpoint_config['endpoints'].items():
            # Apply filters
            if priority_filter and config.get('priority') != priority_filter:
                continue
            
            if category_filter:
                if category_filter.lower() not in endpoint_name.lower():
                    continue
            
            # Skip endpoints with no required params for this test
            required_params = config.get('required_params', [])
            if not required_params:
                continue
            
            endpoints_to_test.append((endpoint_name, config))
        
        # Limit number of endpoints if specified
        if max_endpoints:
            endpoints_to_test = endpoints_to_test[:max_endpoints]
        
        self.logger.info(f"Testing {len(endpoints_to_test)} endpoints...")
        
        for i, (endpoint_name, config) in enumerate(endpoints_to_test, 1):
            self.logger.info(f"[{i}/{len(endpoints_to_test)}] Testing {endpoint_name}...")
            
            result = self.test_endpoint(endpoint_name, config)
            
            # Categorize result
            if result['status'] == 'success':
                self.test_results['successful'].append(result)
            elif result['status'] == 'failed':
                self.test_results['failed'].append(result)
            else:
                self.test_results['skipped'].append(result)
            
            self.test_results['total_tested'] += 1
            
            # Small delay to be respectful to API
            time.sleep(0.5)
    
    def run_comprehensive_test(self, sample_size: int = 20) -> None:
        """Run comprehensive endpoint parameter validation"""
        self.logger.info("=== NBA API Endpoint Parameter Validation ===")
        self.logger.info(f"League: {self.league}")
        self.logger.info(f"Test Mode: {self.test_mode}")
        self.logger.info(f"Sample Size: {sample_size}")
        
        # Load master table data first
        self.load_master_table_data()
        
        # Test high priority endpoints first
        self.logger.info("\n🔥 Testing HIGH PRIORITY endpoints...")
        self.test_endpoints_by_category(priority_filter="high", max_endpoints=sample_size//2)
        
        # Test medium priority endpoints
        self.logger.info("\n📊 Testing MEDIUM PRIORITY endpoints...")
        self.test_endpoints_by_category(priority_filter="medium", max_endpoints=sample_size//4)
        
        # Test some low priority endpoints
        self.logger.info("\n📈 Testing LOW PRIORITY endpoints...")
        self.test_endpoints_by_category(priority_filter="low", max_endpoints=sample_size//4)
        
        # Print comprehensive results
        self.print_test_results()
    
    def print_test_results(self) -> None:
        """Print comprehensive test results"""
        successful = len(self.test_results['successful'])
        failed = len(self.test_results['failed'])
        skipped = len(self.test_results['skipped'])
        total = self.test_results['total_tested']
        
        print("\n" + "="*80)
        print("NBA API ENDPOINT PARAMETER VALIDATION RESULTS")
        print("="*80)
        print(f"Total Endpoints Tested: {total}")
        print(f"✅ Successful: {successful} ({successful/total*100:.1f}%)")
        print(f"❌ Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"⏭️ Skipped: {skipped} ({skipped/total*100:.1f}%)")
        print()
        
        if self.test_results['successful']:
            print("🎉 SUCCESSFUL ENDPOINTS:")
            for result in self.test_results['successful'][:10]:  # Show first 10
                params_str = ", ".join(f"{k}={v}" for k, v in result['parameters_used'].items())
                print(f"  ✅ {result['endpoint']} - {result['response_size']} rows ({result['execution_time']}s)")
                print(f"     Params: {params_str}")
            if len(self.test_results['successful']) > 10:
                print(f"     ... and {len(self.test_results['successful']) - 10} more")
        
        print()
        
        if self.test_results['failed']:
            print("❌ FAILED ENDPOINTS:")
            for result in self.test_results['failed']:
                params_str = ", ".join(f"{k}={v}" for k, v in result['parameters_used'].items())
                print(f"  ❌ {result['endpoint']}")
                print(f"     Params: {params_str}")
                print(f"     Error: {result['error']}")
        
        print("="*80)
        
        # Save detailed results
        results_file = project_root / f'endpoint_validation_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, indent=2, default=str)
        
        self.logger.info(f"Detailed results saved to {results_file}")


def main():
    """Main function to run endpoint parameter validation"""
    validator = EndpointParameterValidator(league="NBA", test_mode=True)
    
    print("🏀 NBA API Endpoint Parameter Validation System")
    print("This will test endpoints with their new parameter configurations")
    print("to ensure all parameter variants (league_id vs league_id_nullable) work correctly.\n")
    
    sample_size = 30  # Test 30 endpoints total
    validator.run_comprehensive_test(sample_size=sample_size)
    
    return validator


if __name__ == "__main__":
    main()