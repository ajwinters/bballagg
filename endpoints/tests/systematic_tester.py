"""
Systematic NBA Endpoint Testing System

This system uses the master tables to systematically test and collect data from
all NBA API endpoints of interest. It prioritizes V3 versions where available
and provides comprehensive testing before full data collection.
"""

import pandas as pd
import time
import os
import json
import sys
from datetime import datetime, timedelta
from nba_api.stats import endpoints

# Add parent directory to path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.nba_endpoints_config import ALL_ENDPOINTS


class SystematicEndpointTester:
    """Tests all configured endpoints systematically using master tables"""
    
    def __init__(self, data_dir='data', test_output_dir='test_output'):
        self.data_dir = data_dir
        self.test_output_dir = test_output_dir
        os.makedirs(test_output_dir, exist_ok=True)
        
        # Load master tables
        self.master_games = pd.read_csv(f'{data_dir}/comprehensive_master_games.csv')
        self.master_players = pd.read_csv(f'{data_dir}/comprehensive_master_players.csv') 
        self.master_teams = pd.read_csv(f'{data_dir}/master_teams.csv')
        
        # Test results tracking
        self.test_results = []
        self.successful_tests = []
        self.failed_tests = []
        
        print(f"📊 Loaded Master Tables:")
        print(f"   Games: {len(self.master_games):,}")
        print(f"   Players: {len(self.master_players):,}")
        print(f"   Teams: {len(self.master_teams):,}")
    
    def get_test_samples(self, category, sample_size=5):
        """Get test samples for each endpoint category"""
        if category == 'game_based':
            # Get recent games from different leagues
            recent_games = self.master_games.sample(min(sample_size, len(self.master_games)))
            return recent_games['GAME_ID'].tolist()
        
        elif category == 'player_based':
            # Get active players from different leagues
            active_players = self.master_players.sample(min(sample_size, len(self.master_players)))
            return active_players['PLAYER_ID'].tolist()
            
        elif category == 'team_based':
            # Get teams (limit to first few for testing)
            return self.master_teams['id'].head(sample_size).tolist()
            
        elif category == 'league_based':
            # Return current season for testing
            return ['2023-24']
    
    def test_endpoint(self, endpoint_config, test_params):
        """Test a single endpoint with given parameters"""
        endpoint_name = endpoint_config['endpoint']
        
        try:
            # Get the endpoint class from nba_api with proper case handling
            # Try different variations to find the correct class
            endpoint_variations = [
                endpoint_name,
                endpoint_name.lower(),
                ''.join(word.capitalize() for word in endpoint_name.split('_')),
            ]
            
            endpoint_class = None
            for variation in endpoint_variations:
                try:
                    endpoint_class = getattr(endpoints, variation)
                    if callable(endpoint_class):
                        break
                except AttributeError:
                    continue
            
            if not endpoint_class or not callable(endpoint_class):
                raise AttributeError(f"Endpoint class {endpoint_name} not found or not callable in nba_api")
            
            print(f"   Testing {endpoint_name} with params: {test_params}")
            
            # Make API call
            instance = endpoint_class(**test_params)
            dataframes = instance.get_data_frames()
            
            # Analyze results
            total_rows = sum(len(df) for df in dataframes if not df.empty)
            total_columns = sum(len(df.columns) for df in dataframes if not df.empty)
            
            result = {
                'endpoint': endpoint_name,
                'status': 'SUCCESS',
                'num_dataframes': len(dataframes),
                'total_rows': total_rows,
                'total_columns': total_columns,
                'test_params': test_params,
                'dataframe_shapes': [df.shape for df in dataframes],
                'timestamp': datetime.now().isoformat()
            }
            
            # Save sample data for inspection
            if dataframes and not dataframes[0].empty:
                sample_file = f"{self.test_output_dir}/{endpoint_name.lower()}_sample.csv"
                dataframes[0].head().to_csv(sample_file, index=False)
                result['sample_file'] = sample_file
            
            print(f"     ✅ SUCCESS: {len(dataframes)} dataframes, {total_rows:,} rows")
            self.successful_tests.append(result)
            
        except Exception as e:
            result = {
                'endpoint': endpoint_name,
                'status': 'FAILED',
                'error': str(e),
                'test_params': test_params,
                'timestamp': datetime.now().isoformat()
            }
            print(f"     ❌ FAILED: {str(e)}")
            self.failed_tests.append(result)
        
        self.test_results.append(result)
        return result
    
    def test_category(self, category, sample_size=3):
        """Test all endpoints in a specific category"""
        print(f"\n🔄 Testing {category.upper()} endpoints...")
        
        if category not in ALL_ENDPOINTS:
            print(f"❌ Category {category} not found in configuration")
            return
        
        endpoints = ALL_ENDPOINTS[category]
        test_samples = self.get_test_samples(category, sample_size)
        
        print(f"   Found {len(endpoints)} endpoints to test")
        print(f"   Using {len(test_samples)} test samples: {test_samples[:3]}...")
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config['endpoint']
            
            # Prepare test parameters based on endpoint requirements
            for i, sample in enumerate(test_samples[:sample_size]):  # Limit samples for testing
                try:
                    test_params = {}
                    
                    # Map parameters based on category
                    if category == 'game_based':
                        test_params['game_id'] = sample
                    elif category == 'player_based':
                        test_params['player_id'] = str(sample)
                    elif category == 'team_based':
                        test_params['team_id'] = str(sample)
                    elif category == 'league_based':
                        # Special handling for league endpoints
                        if 'PlayerGameLogs' in endpoint_name:
                            test_params = {
                                'season_nullable': sample,
                                'date_from_nullable': '01/01/2024',
                                'date_to_nullable': '01/31/2024'  # Test with January 2024
                            }
                    
                    # Test the endpoint
                    self.test_endpoint(endpoint_config, test_params)
                    
                    # Rate limiting
                    time.sleep(0.6)
                    
                    # Only test first sample for each endpoint to save time
                    break
                    
                except Exception as e:
                    print(f"     ❌ Parameter setup failed: {str(e)}")
                    continue
    
    def test_all_categories(self):
        """Test all endpoint categories"""
        print("🚀 Starting Systematic Endpoint Testing...")
        print("=" * 60)
        
        categories = ['game_based', 'player_based', 'team_based', 'league_based']
        
        for category in categories:
            self.test_category(category, sample_size=2)  # Small samples for testing
        
        self.generate_test_report()
    
    def test_high_priority_only(self):
        """Test only high priority endpoints across all categories"""
        print("🎯 Testing HIGH PRIORITY endpoints only...")
        print("=" * 60)
        
        for category, endpoints in ALL_ENDPOINTS.items():
            high_priority = [ep for ep in endpoints if ep.get('priority') == 'high']
            
            if high_priority:
                print(f"\n🔄 Testing HIGH PRIORITY {category.upper()} endpoints...")
                test_samples = self.get_test_samples(category, sample_size=2)
                
                for endpoint_config in high_priority:
                    sample = test_samples[0] if test_samples else None
                    if sample:
                        test_params = {}
                        
                        if category == 'game_based':
                            test_params['game_id'] = sample
                        elif category == 'player_based':
                            test_params['player_id'] = str(sample)
                        elif category == 'team_based':
                            test_params['team_id'] = str(sample)
                        elif category == 'league_based':
                            if 'PlayerGameLogs' in endpoint_config['endpoint']:
                                test_params = {
                                    'season_nullable': '2023-24',
                                    'date_from_nullable': '01/01/2024',
                                    'date_to_nullable': '01/07/2024'  # One week test
                                }
                        
                        self.test_endpoint(endpoint_config, test_params)
                        time.sleep(0.6)
        
        self.generate_test_report()
    
    def generate_test_report(self):
        """Generate comprehensive test report"""
        print(f"\n📋 SYSTEMATIC ENDPOINT TEST REPORT")
        print("=" * 60)
        
        total_tests = len(self.test_results)
        successful_tests = len(self.successful_tests)
        failed_tests = len(self.failed_tests)
        
        print(f"📊 TEST SUMMARY:")
        print(f"   Total endpoints tested: {total_tests}")
        print(f"   Successful: {successful_tests} ({successful_tests/total_tests*100:.1f}%)")
        print(f"   Failed: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")
        
        if successful_tests > 0:
            total_rows = sum(result.get('total_rows', 0) for result in self.successful_tests)
            print(f"   Total data rows that would be collected: {total_rows:,}")
        
        # Category breakdown
        print(f"\n📈 SUCCESS BY CATEGORY:")
        for category in ['game_based', 'player_based', 'team_based', 'league_based']:
            category_results = [r for r in self.test_results 
                              if any(ep['endpoint'] == r['endpoint'] 
                                   for ep in ALL_ENDPOINTS.get(category, []))]
            category_success = [r for r in category_results if r['status'] == 'SUCCESS']
            
            if category_results:
                success_rate = len(category_success) / len(category_results) * 100
                print(f"   {category}: {len(category_success)}/{len(category_results)} ({success_rate:.1f}%)")
        
        # Failed endpoints
        if failed_tests > 0:
            print(f"\n❌ FAILED ENDPOINTS:")
            for result in self.failed_tests:
                print(f"   {result['endpoint']}: {result['error']}")
        
        # Save detailed results
        results_file = f"{self.test_output_dir}/endpoint_test_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'summary': {
                    'total_tests': total_tests,
                    'successful_tests': successful_tests,
                    'failed_tests': failed_tests,
                    'timestamp': datetime.now().isoformat()
                },
                'successful_tests': self.successful_tests,
                'failed_tests': self.failed_tests
            }, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {results_file}")
        print(f"🔍 Sample data files saved to: {self.test_output_dir}/")


def main():
    """Main testing function"""
    tester = SystematicEndpointTester()
    
    # Choose testing mode
    print("Select testing mode:")
    print("1. Test HIGH PRIORITY endpoints only (recommended)")
    print("2. Test ALL endpoints")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        tester.test_high_priority_only()
    else:
        tester.test_all_categories()


if __name__ == "__main__":
    main()
